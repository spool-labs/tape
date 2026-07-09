//! Stream write implementation.

use futures::stream::{self, FuturesOrdered, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;

use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_api::state::Tape;
use tape_core::types::ContentType;
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::hash::hash;
use tape_crypto::Hash;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::tapedrive::Tapedrive;
use crate::metrics::{Operation, Phase};
use crate::track::write::{
    certify_submit_with_retry, certify_with_retry, collect_certification, inline_write_fits,
    should_retry_certification, submit_blob, submit_blob_with_logical_size, submit_certification,
    submit_raw_with_logical_size, upload_with_retry, wait_for_certified_track, UploadPlan,
    WrittenTrack, UNNAMED_TRACK, UNTYPED_TRACK,
};

use super::error::StreamError;
use super::manifest::{
    ChunkEntry, ChunkManifest, MAX_TRACK_SIZE, MAX_TRACKS_PER_TAPE, MANIFEST_VERSION,
};
use super::receipt::StreamReceipt;

/// Maximum track slots in a tape (2^TRACK_TREE_HEIGHT).
const MAX_TRACKS: TrackNumber = TrackNumber(MAX_TRACKS_PER_TAPE);

/// Slice uploads kept in flight per chunk; two saturate a typical uplink.
/// Higher raises peak memory, since each holds its encoded slices.
const STORE_CONCURRENCY: usize = 2;

/// Stored chunks whose signatures are collected ahead of the serial certify
/// submits. Collections hold no slice data, so lookahead is cheap.
const COLLECT_LOOKAHEAD: usize = 2;

// Chunks are internal fragments addressed by track number, never by name.
struct PendingChunk {
    pub entry: ChunkEntry,
    pub written: WrittenTrack,
}

// A registered chunk whose encoded slices still need to be stored.
struct RegisteredChunk {
    entry: ChunkEntry,
    written: WrittenTrack,
    plan: UploadPlan,
}

/// Validate stream-level input before any track writes begin.
fn validate_stream_size(size: StorageUnits) -> Result<(), StreamError> {
    if size.is_zero() {
        return Err(StreamError::InvalidInput(
            "empty streams are not supported".into(),
        ));
    }

    Ok(())
}

/// Compute the number of chunks required for a stream.
fn chunk_count_for_size(size: StorageUnits) -> Result<TrackNumber, StreamError> {
    let chunk_count = size.to_bytes().div_ceil(MAX_TRACK_SIZE as u64);
    Ok(TrackNumber(chunk_count))
}

/// Return the byte offset for a chunk index.
fn chunk_offset(chunk_index: usize) -> Result<StorageUnits, StreamError> {
    let chunk_index = u64::try_from(chunk_index)
        .map_err(|_| StreamError::InvalidInput("stream has too many chunks".into()))?;
    let offset = chunk_index
        .checked_mul(MAX_TRACK_SIZE as u64)
        .ok_or_else(|| StreamError::InvalidInput("stream size overflow".into()))?;
    Ok(StorageUnits::from_bytes(offset))
}

/// Return the stored byte size for a chunk at the given index.
fn chunk_size(
    chunk_index: usize,
    chunk_count: TrackNumber,
    total_size: StorageUnits,
) -> Result<StorageUnits, StreamError> {
    if chunk_index + 1 == chunk_count.as_usize() {
        let offset = chunk_offset(chunk_index)?;
        total_size
            .checked_sub(offset)
            .ok_or_else(|| StreamError::InvalidInput("stream size underflow".into()))
    } else {
        Ok(StorageUnits::from_bytes(MAX_TRACK_SIZE as u64))
    }
}

/// Build deterministic manifest entries before or after upload.
fn build_entries(
    start_track_number: TrackNumber,
    chunk_count: TrackNumber,
    total_size: StorageUnits,
) -> Result<Vec<ChunkEntry>, StreamError> {
    let mut entries = Vec::with_capacity(chunk_count.as_usize());

    for chunk_index in 0..chunk_count.as_usize() {
        let track_number = start_track_number
            .checked_add(TrackNumber(chunk_index as u64))
            .ok_or_else(|| StreamError::InvalidInput("chunk track number overflow".into()))?;

        entries.push(ChunkEntry {
            track_number,
            offset: chunk_offset(chunk_index)?,
            size: chunk_size(chunk_index, chunk_count, total_size)?,
        });
    }

    Ok(entries)
}

/// Build a chunk manifest from ordered entries.
fn build_manifest(
    key: Hash,
    total_size: StorageUnits,
    entries: Vec<ChunkEntry>,
) -> Result<ChunkManifest, StreamError> {
    let chunk_count = TrackNumber(
        u64::try_from(entries.len())
            .map_err(|_| StreamError::InvalidInput("stream has too many chunks".into()))?,
    );

    Ok(ChunkManifest {
        version: MANIFEST_VERSION,
        total_size,
        chunk_count,
        chunk_size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
        key,
        chunks: entries,
    })
}

/// Verify the tape has enough capacity and track slots for the stream.
fn preflight(
    tape: &Tape,
    total_required_bytes: StorageUnits,
    tracks_needed: TrackNumber,
) -> Result<(), TapedriveError> {
    let available_capacity = tape.capacity.saturating_sub(tape.used);
    if total_required_bytes > available_capacity {
        return Err(TapedriveError::InsufficientCapacity {
            need: total_required_bytes,
            available: available_capacity,
        });
    }

    let used_tracks = tape.tracks.next_number();
    let available_tracks = MAX_TRACKS.saturating_sub(used_tracks);

    if tracks_needed > available_tracks {
        let chunk_count = tracks_needed
            .checked_prev()
            .ok_or_else(|| stream_error(StreamError::InvalidInput("stream needs no data tracks".into())))?;

        return Err(stream_error(StreamError::InsufficientTrackSlots {
            available: available_tracks,
            needed: tracks_needed,
            chunks: chunk_count,
        }));
    }

    Ok(())
}

/// Write in-memory bytes as a multi-track stream.
pub async fn write_bytes<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
) -> Result<StreamReceipt, TapedriveError> {
    let size = StorageUnits::from_bytes(data.len() as u64);
    let chunk_count = prepare_write(client, tape_key, name, size).await?;
    let chunk_sources = stream::iter(data.chunks(MAX_TRACK_SIZE).map(Ok::<_, TapedriveError>));
    let pending_chunks =
        pipeline_chunks(client, tape_key, size, chunk_count, chunk_sources).await?;

    finalize_write(client, tape_key, name, content_type, size, pending_chunks).await
}

/// Write bytes from an async reader as a multi-track stream.
pub async fn write_stream<Blockchain: Rpc, Cluster: Api, Reader: AsyncRead + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    size: StorageUnits,
    mut reader: Reader,
) -> Result<StreamReceipt, TapedriveError> {
    let chunk_count = prepare_write(client, tape_key, name, size).await?;
    let chunk_sources = stream::unfold(
        (&mut reader, 0usize),
        move |(reader, chunk_index)| async move {
            if chunk_index >= chunk_count.as_usize() {
                return None;
            }
            let result = read_chunk(reader, chunk_index, chunk_count, size).await;
            Some((result, (reader, chunk_index + 1)))
        },
    );
    let pending_chunks =
        pipeline_chunks(client, tape_key, size, chunk_count, chunk_sources).await?;

    verify_stream_drained(&mut reader).await?;
    finalize_write(client, tape_key, name, content_type, size, pending_chunks).await
}

/// Validate the write upfront and return the chunk count.
async fn prepare_write<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    size: StorageUnits,
) -> Result<TrackNumber, TapedriveError> {
    let timer = client
        .timer(Operation::WriteStream, Phase::Preflight)
        .bytes(size.to_bytes());

    let result = async {
        validate_stream_size(size).map_err(stream_error)?;
        let chunk_count = chunk_count_for_size(size).map_err(stream_error)?;
        let tracks_needed = chunk_count.checked_next().ok_or_else(|| {
            stream_error(StreamError::InvalidInput("stream has too many chunks".into()))
        })?;

        let entries = build_entries(TrackNumber(0), chunk_count, size).map_err(stream_error)?;
        let manifest = build_manifest(hash(name), size, entries).map_err(stream_error)?;
        let manifest_bytes = manifest.to_bytes().map_err(stream_error)?;
        let total_size = size
            .checked_add(StorageUnits::from_bytes(manifest_bytes.len() as u64))
            .ok_or_else(|| stream_error(StreamError::InvalidInput("stream size overflow".into())))?;

        let tape = client.get_tape(&tape_key.address()).await?;
        preflight(&tape, total_size, tracks_needed)?;
        Ok(chunk_count)
    }
    .await;
    timer.finish_result(&result);
    result
}

/// Run chunk writes as a three-stage pipeline: register one at a time, keep a
/// bounded number of slice uploads in flight, and certify in track order behind
/// them. A stage error cancels the pipeline; incomplete tracks go to recovery.
async fn pipeline_chunks<Blockchain, Cluster, Bytes, Chunks>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    size: StorageUnits,
    chunk_count: TrackNumber,
    chunk_sources: Chunks,
) -> Result<Vec<PendingChunk>, TapedriveError>
where
    Blockchain: Rpc,
    Cluster: Api,
    Bytes: AsRef<[u8]>,
    Chunks: Stream<Item = Result<Bytes, TapedriveError>>,
{
    let (registered_sender, mut registered_receiver) = mpsc::channel(1);
    let (stored_sender, mut stored_receiver) = mpsc::channel(chunk_count.as_usize().max(1));

    let register_stage = async move {
        let mut chunk_sources = std::pin::pin!(chunk_sources);
        let mut chunk_index = 0usize;
        while let Some(chunk_data) = chunk_sources.next().await {
            let registered =
                register_chunk(client, tape_key, chunk_index, chunk_count, size, chunk_data?)
                    .await?;
            chunk_index += 1;
            if registered_sender.send(registered).await.is_err() {
                break;
            }
        }
        Ok::<_, TapedriveError>(())
    };

    let store_stage = async move {
        let mut in_flight = FuturesOrdered::new();
        let mut is_registering = true;
        while is_registering || !in_flight.is_empty() {
            tokio::select! {
                // Safe: a chunk only leaves the channel once this branch completes.
                registered = registered_receiver.recv(),
                    if is_registering && in_flight.len() < STORE_CONCURRENCY =>
                {
                    match registered {
                        Some(registered) => in_flight.push_back(store_chunk(client, registered)),
                        None => is_registering = false,
                    }
                }
                // Safe: a cancelled poll drops no upload; each leaves only once it completes.
                stored = in_flight.next(), if !in_flight.is_empty() => {
                    let Some(stored) = stored else { continue };
                    if stored_sender.send(stored?).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok::<_, TapedriveError>(())
    };

    // Signatures sign the track's leaf hash, so collection for the next chunk
    // can run while the previous chunk's certify transaction confirms.
    let (collected_sender, mut collected_receiver) = mpsc::channel(COLLECT_LOOKAHEAD);
    let collect_stage = async move {
        while let Some(pending) = stored_receiver.recv().await {
            let collected =
                collect_certification(client, &pending.written, Operation::WriteStream).await?;
            if collected_sender.send((pending, collected)).await.is_err() {
                break;
            }
        }
        Ok::<_, TapedriveError>(())
    };

    // Each certify mutates the tape's track tree, so a proof is only valid
    // against the root left by the previous certify; the proof is re-fetched
    // per chunk. Do not parallelize the submits.
    let certify_stage = async move {
        let mut pending_chunks = Vec::with_capacity(chunk_count.as_usize());
        while let Some((pending, collected)) = collected_receiver.recv().await {
            let submitted = submit_certification(
                client,
                tape_key,
                &pending.written,
                &collected,
                Operation::WriteStream,
            )
            .await;
            match submitted {
                Ok(()) => {}
                // Pre-collected signatures can go stale (epoch change) while
                // they wait in the lookahead; re-collect and submit fresh.
                Err(err) if should_retry_certification(&err) => {
                    certify_submit_with_retry(
                        client,
                        tape_key,
                        &pending.written,
                        Operation::WriteStream,
                    )
                    .await?;
                }
                Err(err) => return Err(err),
            }
            pending_chunks.push(pending);
        }

        // Every certify is on-chain; confirm peer visibility for all chunks at
        // once instead of once per chunk inside the serial loop.
        let visible = client
            .timer(Operation::WriteStream, Phase::CertifyVisible)
            .chunks(pending_chunks.len() as u64);
        let tape_address = tape_key.address();
        let result = futures::future::try_join_all(pending_chunks.iter().map(|pending| {
            wait_for_certified_track(client, &tape_address, pending.written.track.track_number)
        }))
        .await;
        visible.finish_result(&result);
        result?;

        Ok::<_, TapedriveError>(pending_chunks)
    };

    let ((), (), (), pending_chunks) =
        tokio::try_join!(register_stage, store_stage, collect_stage, certify_stage)?;
    Ok(pending_chunks)
}

/// Read one chunk's bytes from the source reader.
async fn read_chunk<Reader: AsyncRead + Unpin>(
    reader: &mut Reader,
    chunk_index: usize,
    chunk_count: TrackNumber,
    total_size: StorageUnits,
) -> Result<Vec<u8>, TapedriveError> {
    let expected_chunk_size =
        chunk_size(chunk_index, chunk_count, total_size).map_err(stream_error)?;
    let mut chunk_data = vec![0u8; expected_chunk_size.as_usize()];
    read_chunk_exact(reader, &mut chunk_data).await?;
    Ok(chunk_data)
}

/// Read exactly one chunk from the source reader.
async fn read_chunk_exact<Reader: AsyncRead + Unpin>(
    reader: &mut Reader,
    chunk_data: &mut [u8],
) -> Result<(), TapedriveError> {
    match reader.read_exact(chunk_data).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Err(stream_error(
            StreamError::InvalidInput("stream ended before declared size".into()),
        )),
        Err(error) => Err(TapedriveError::Io(error)),
    }
}

/// Ensure the source reader does not contain extra bytes beyond the declared size.
async fn verify_stream_drained<Reader: AsyncRead + Unpin>(
    reader: &mut Reader,
) -> Result<(), TapedriveError> {
    let mut extra = [0u8; 1];
    if reader.read(&mut extra).await? != 0 {
        return Err(stream_error(StreamError::InvalidInput(
            "stream exceeded declared size".into(),
        )));
    }

    Ok(())
}

/// Register one chunk track on-chain and return it with its upload plan.
async fn register_chunk<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    chunk_index: usize,
    chunk_count: TrackNumber,
    size: StorageUnits,
    chunk_data: impl AsRef<[u8]>,
) -> Result<RegisteredChunk, TapedriveError> {
    let (written, plan) = submit_blob(
        client,
        tape_key,
        UNNAMED_TRACK,
        UNTYPED_TRACK,
        chunk_data.as_ref(),
        Operation::WriteStream,
    )
    .await?;

    Ok(RegisteredChunk {
        entry: ChunkEntry {
            track_number: written.track.track_number,
            offset: chunk_offset(chunk_index).map_err(stream_error)?,
            size: chunk_size(chunk_index, chunk_count, size).map_err(stream_error)?,
        },
        written,
        plan,
    })
}

/// Upload a registered chunk's slices, dropping them once stored.
async fn store_chunk<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    registered: RegisteredChunk,
) -> Result<PendingChunk, TapedriveError> {
    upload_with_retry(
        client,
        &registered.written,
        &registered.plan,
        Operation::WriteStream,
    )
    .await?;

    Ok(PendingChunk {
        entry: registered.entry,
        written: registered.written,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManifestWriteMode {
    Inline,
    Coded,
}

fn manifest_write_mode(name: &[u8], manifest_bytes: &[u8]) -> ManifestWriteMode {
    if inline_write_fits(name, manifest_bytes.len()) {
        ManifestWriteMode::Inline
    } else {
        ManifestWriteMode::Coded
    }
}

/// Write the manifest inline when the transaction stays small; otherwise write it as a blob.
async fn write_manifest<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    logical_size: StorageUnits,
    manifest_bytes: &[u8],
) -> Result<WrittenTrack, TapedriveError> {
    if manifest_write_mode(name, manifest_bytes) == ManifestWriteMode::Inline {
        return submit_raw_with_logical_size(
            client,
            tape_key,
            name,
            content_type,
            logical_size,
            manifest_bytes,
            Operation::WriteStream,
        )
        .await;
    }

    let (written, plan) = submit_blob_with_logical_size(
        client,
        tape_key,
        name,
        content_type,
        logical_size,
        manifest_bytes,
        Operation::WriteStream,
    )
    .await?;
    upload_with_retry(client, &written, &plan, Operation::WriteStream).await?;
    let track = certify_with_retry(client, tape_key, &written, Operation::WriteStream).await?;
    Ok(WrittenTrack {
        address: written.address,
        track,
    })
}

/// Finalize the stream after every chunk is stored and certified.
async fn finalize_write<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    name: &[u8],
    content_type: ContentType,
    size: StorageUnits,
    pending_chunks: Vec<PendingChunk>,
) -> Result<StreamReceipt, TapedriveError> {
    let entries = pending_chunks
        .into_iter()
        .map(|pending_chunk| pending_chunk.entry)
        .collect();
    let manifest = build_manifest(hash(name), size, entries).map_err(stream_error)?;
    let manifest_bytes = manifest.to_bytes().map_err(stream_error)?;

    let manifest_track = write_manifest(client, tape_key, name, content_type, size, &manifest_bytes).await?;
    let manifest_address = track_pda(manifest_track.track.tape, manifest_track.track.track_number).0;

    Ok(StreamReceipt {
        tape: manifest_track.track.tape,
        manifest: manifest_address,
        manifest_track_number: manifest_track.track.track_number,
        manifest_value_hash: manifest_track.track.value_hash,
    })
}

fn stream_error(error: StreamError) -> TapedriveError {
    TapedriveError::Stream(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::state::Tape;
    use tape_core::types::{EpochNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::address::Address;

    use super::*;

    fn make_tape(capacity_bytes: u64, used_bytes: u64, next_track_number: u64) -> Tape {
        let mut tape = Tape::zeroed();
        tape.id = TapeNumber(1);
        tape.authority = Address::new_unique();
        tape.capacity = StorageUnits::from_bytes(capacity_bytes);
        tape.used = StorageUnits::from_bytes(used_bytes);
        tape.active_epoch = EpochNumber(1);
        tape.expiry_epoch = EpochNumber(2);
        tape.tracks.next_number = TrackNumber(next_track_number);
        tape
    }

    fn sample_manifest_bytes(chunk_count: u64) -> Vec<u8> {
        let key = Hash::from([0x11; 32]);
        let total_size = StorageUnits::from_bytes(MAX_TRACK_SIZE as u64 * chunk_count);
        let entries = build_entries(TrackNumber(0), TrackNumber(chunk_count), total_size)
            .expect("build entries");
        build_manifest(key, total_size, entries)
            .expect("build manifest")
            .to_bytes()
            .expect("serialize manifest")
    }

    #[test]
    fn small_manifest_uses_inline_write() {
        let manifest_bytes = sample_manifest_bytes(1);

        assert_eq!(
            manifest_write_mode(b"roms/small.bin", &manifest_bytes),
            ManifestWriteMode::Inline
        );
    }

    #[test]
    fn large_manifest_uses_coded_write() {
        let manifest_bytes = sample_manifest_bytes(64);

        assert_eq!(
            manifest_write_mode(b"roms/large.bin", &manifest_bytes),
            ManifestWriteMode::Coded
        );
    }

    // capacity checks include the serialized manifest bytes.
    #[test]
    fn manifest_size() {
        let key = Hash::from([0x11; 32]);
        let total_size = StorageUnits::from_bytes(MAX_TRACK_SIZE as u64);
        let entries = build_entries(TrackNumber(0), TrackNumber(1), total_size).expect("build entries");
        let manifest = build_manifest(key, total_size, entries).expect("build manifest");
        let manifest_bytes = manifest.to_bytes().expect("serialize manifest");
        let total_required = total_size + StorageUnits::from_bytes(manifest_bytes.len() as u64);
        let tape = make_tape(total_required.to_bytes() - 1, 0, 0);

        let error = preflight(&tape, total_required, TrackNumber(2)).expect_err("preflight should fail");

        match error {
            TapedriveError::InsufficientCapacity { need, available } => {
                assert_eq!(need, total_required);
                assert_eq!(available.to_bytes(), total_required.to_bytes() - 1);
            }
            other => panic!("expected insufficient capacity, got {other}"),
        }
    }

    // empty streams are rejected before chunk planning.
    #[test]
    fn empty_stream() {
        let error = validate_stream_size(StorageUnits::zero()).expect_err("empty stream should fail");

        match error {
            StreamError::InvalidInput(message) => {
                assert_eq!(message, "empty streams are not supported");
            }
            other => panic!("expected empty stream error, got {other}"),
        }
    }

    // manifest entries reject track number overflow.
    #[test]
    fn overflow() {
        let error = build_entries(
            TrackNumber(u64::MAX),
            TrackNumber(2),
            StorageUnits::from_bytes(MAX_TRACK_SIZE as u64 * 2),
        )
            .expect_err("entries should fail");

        match error {
            StreamError::InvalidInput(message) => {
                assert_eq!(message, "chunk track number overflow");
            }
            other => panic!("expected chunk track number overflow, got {other}"),
        }
    }
}
