//! Stream write implementation.

use std::time::Duration;

use futures::stream::{self, FuturesOrdered, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;

use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_api::state::Tape;
use tape_core::prelude::CompressedTrack;
use tape_core::track::mirror::ArchiveMirror;
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
    certified_track, certify_submit_with_retry, certify_with_retry, collect_certification,
    encode_blob, inline_write_fits, register_blob_processed, resolve_sent_blob,
    should_retry_certification, submit_blob_with_logical_size, submit_certification_with_proof,
    submit_raw_with_logical_size, upload_with_retry, wait_for_certified_track, CertifySend,
    UploadPlan, WrittenTrack, UNNAMED_TRACK, UNTYPED_TRACK,
};
use crate::transfer::certify::CollectedSignatures;

use super::error::StreamError;
use super::manifest::{
    ChunkEntry, ChunkManifest, MAX_TRACK_SIZE, MAX_TRACKS_PER_TAPE, MANIFEST_VERSION,
};
use super::receipt::StreamReceipt;

/// Maximum track slots in a tape (2^TRACK_TREE_HEIGHT).
const MAX_TRACKS: TrackNumber = TrackNumber(MAX_TRACKS_PER_TAPE);

/// Slice uploads kept in flight per chunk; each holds its encoded slices
/// (~2x the chunk), so this bounds peak memory together with ENCODE_AHEAD.
const STORE_CONCURRENCY: usize = 3;

/// Encoded chunks buffered ahead of the serial register submits, so encoding
/// (seconds of CPU per chunk) overlaps register confirmations.
const ENCODE_AHEAD: usize = 2;

/// Track-written event fetches kept in flight behind the register submits.
/// Events need confirmed level, so this hides that wait while the registers
/// themselves only pay a processed-level wait each.
const RESOLVE_CONCURRENCY: usize = 3;

/// Attempts for the end-of-stream root comparison; the confirmed root can
/// briefly trail the last processed certify.
const ROOT_CHECK_ATTEMPTS: usize = 10;

/// Delay between root comparison attempts.
const ROOT_CHECK_INTERVAL: Duration = Duration::from_millis(400);

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

/// Run chunk writes as a pipeline: register chunks one at a time at processed
/// level, resolve their track numbers concurrently in track order, keep up to
/// STORE_CONCURRENCY slice uploads in flight, and certify stored chunks
/// strictly in track order behind the uploads. A local mirror of the tape's
/// track tree supplies certify proofs without per-chunk refetches. Stage
/// errors cancel the whole pipeline; incomplete tracks are left for the
/// recovery worker.
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
    // The mirror seeds from the pre-stream track tree; the resolve stage
    // appends every registered track and the certify stage proves against
    // and updates the same tree, so both share it behind a mutex.
    let tape = client.get_tape(&tape_key.address()).await?;
    let mirror = Mutex::new(ArchiveMirror::new(&tape.tracks));
    let mirror = &mirror;

    let (encoded_sender, mut encoded_receiver) = mpsc::channel(ENCODE_AHEAD);
    let (sent_sender, mut sent_receiver) = mpsc::channel(1);
    let (registered_sender, mut registered_receiver) = mpsc::channel(1);
    let (stored_sender, mut stored_receiver) = mpsc::channel(chunk_count.as_usize().max(1));

    // Encoding is CPU-bound; running it ahead keeps register confirmations
    // from serializing with it.
    let encode_stage = async move {
        let mut chunk_sources = std::pin::pin!(chunk_sources);
        let mut chunk_index = 0usize;
        while let Some(chunk_data) = chunk_sources.next().await {
            let plan = encode_blob(
                client,
                chunk_data?.as_ref().to_vec(),
                Operation::WriteStream,
            )
            .await?;
            if encoded_sender.send((chunk_index, plan)).await.is_err() {
                break;
            }
            chunk_index += 1;
        }
        Ok::<_, TapedriveError>(())
    };

    // Registers submit at processed level: the processed wait is enough to
    // keep track numbers assigned in stream order, and the confirmed-level
    // event wait moves into the resolve stage.
    let register_stage = async move {
        while let Some((chunk_index, plan)) = encoded_receiver.recv().await {
            let logical_size = plan.storage_units;
            let sent = register_blob_processed(
                client,
                tape_key,
                UNNAMED_TRACK,
                UNTYPED_TRACK,
                logical_size,
                plan,
                Operation::WriteStream,
            )
            .await?;
            if sent_sender.send((chunk_index, sent)).await.is_err() {
                break;
            }
        }
        Ok::<_, TapedriveError>(())
    };

    // Resolving fetches each register's track-written event at confirmed
    // level; FuturesOrdered overlaps those waits but emits chunks in track
    // order, which the mirror appends require.
    let resolve_stage = async move {
        let mut in_flight = FuturesOrdered::new();
        let mut is_registering = true;
        while is_registering || !in_flight.is_empty() {
            tokio::select! {
                // Safe: recv is cancellation-safe and a chunk only leaves the
                // channel when this branch completes.
                sent = sent_receiver.recv(),
                    if is_registering && in_flight.len() < RESOLVE_CONCURRENCY =>
                {
                    match sent {
                        Some((chunk_index, sent)) => in_flight.push_back(async move {
                            resolve_sent_blob(client, sent)
                                .await
                                .map(|resolved| (chunk_index, resolved))
                        }),
                        None => is_registering = false,
                    }
                }
                // Safe: FuturesOrdered::next only removes a future once it
                // completes; a cancelled poll leaves every resolve in place.
                resolved = in_flight.next(), if !in_flight.is_empty() => {
                    let Some(resolved) = resolved else { continue };
                    let (chunk_index, (written, plan)) = resolved?;
                    append_to_mirror(mirror, &written).await?;
                    let registered = RegisteredChunk {
                        entry: ChunkEntry {
                            track_number: written.track.track_number,
                            offset: chunk_offset(chunk_index).map_err(stream_error)?,
                            size: chunk_size(chunk_index, chunk_count, size)
                                .map_err(stream_error)?,
                        },
                        written,
                        plan,
                    };
                    if registered_sender.send(registered).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok::<_, TapedriveError>(())
    };

    let store_stage = async move {
        let mut in_flight = FuturesOrdered::new();
        let mut is_registering = true;
        while is_registering || !in_flight.is_empty() {
            tokio::select! {
                // Safe: recv is cancellation-safe and a chunk only leaves the
                // channel when this branch completes.
                registered = registered_receiver.recv(),
                    if is_registering && in_flight.len() < STORE_CONCURRENCY =>
                {
                    match registered {
                        Some(registered) => in_flight.push_back(store_chunk(client, registered)),
                        None => is_registering = false,
                    }
                }
                // Safe: FuturesOrdered::next only removes a future once it
                // completes; a cancelled poll leaves every upload in place.
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
    // against the root left by the previous certify; proofs come from the
    // shared mirror. Do not parallelize the submits.
    let certify_stage = async move {
        let mut pending_chunks = Vec::with_capacity(chunk_count.as_usize());
        while let Some((pending, collected)) = collected_receiver.recv().await {
            certify_chunk(client, tape_key, mirror, &pending.written, &collected).await?;
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

        // Peers already report every track certified, so the confirmed root
        // has had time to catch up with the processed certifies; a lasting
        // mismatch means an external writer touched the tape mid-stream.
        verify_mirror_root(client, tape_key, mirror).await?;

        Ok::<_, TapedriveError>(pending_chunks)
    };

    let ((), (), (), (), (), pending_chunks) = tokio::try_join!(
        encode_stage,
        register_stage,
        resolve_stage,
        store_stage,
        collect_stage,
        certify_stage
    )?;
    Ok(pending_chunks)
}

/// Mirror a resolved register. A mirror reseeded from chain state mid-stream
/// already holds recently confirmed tracks in its base, so appends for those
/// are skipped rather than failed.
async fn append_to_mirror(
    mirror: &Mutex<ArchiveMirror>,
    written: &WrittenTrack,
) -> Result<(), TapedriveError> {
    let mut mirror = mirror.lock().await;
    if written.track.track_number < mirror.next_number() {
        return Ok(());
    }

    mirror.append(&written.track).map_err(|_| {
        stream_error(StreamError::Integrity(format!(
            "track {} arrived out of mirror order; an external writer touched the tape",
            written.track.track_number
        )))
    })
}

/// Certify one stored chunk. The fast path proves the track against the
/// local mirror and submits at processed level; any retryable failure falls
/// back to the confirmed path with re-collected signatures and a fresh peer
/// proof, then brings the mirror back into lockstep.
async fn certify_chunk<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    mirror: &Mutex<ArchiveMirror>,
    written: &WrittenTrack,
    collected: &CollectedSignatures,
) -> Result<(), TapedriveError> {
    let track_number = written.track.track_number;
    let certified = certified_track(&written.track);

    // Tracks a reseeded mirror can no longer prove go straight to the
    // fallback.
    let proof = mirror.lock().await.proof_for(track_number);
    if let Ok(proof) = proof {
        let submitted = submit_certification_with_proof(
            client,
            tape_key,
            proof,
            collected,
            CertifySend::Processed,
            Operation::WriteStream,
        )
        .await;
        match submitted {
            Ok(()) => return apply_certified_to_mirror(client, tape_key, mirror, &certified).await,
            // Pre-collected signatures can go stale (epoch change) and the
            // mirror can briefly trail a register the chain already applied;
            // the fallback re-collects and re-proves from live state.
            Err(err) if should_retry_certification(&err) => {}
            Err(err) => return Err(err),
        }
    }

    certify_submit_with_retry(client, tape_key, written, Operation::WriteStream).await?;
    apply_certified_to_mirror(client, tape_key, mirror, &certified).await
}

/// Replay a landed certify on the mirror; the chain wrote this exact leaf.
/// If the mirror cannot apply it the mirror has diverged, so reseed it from
/// chain state and let later certifies fall back until it covers them again.
async fn apply_certified_to_mirror<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    mirror: &Mutex<ArchiveMirror>,
    certified: &CompressedTrack,
) -> Result<(), TapedriveError> {
    let mut mirror = mirror.lock().await;
    if mirror.apply_certified(certified.track_number, certified).is_ok() {
        return Ok(());
    }

    // Hold the lock across the fetch so no append lands between the fetch
    // and the reseed.
    let tape = client.get_tape(&tape_key.address()).await?;
    *mirror = ArchiveMirror::new(&tape.tracks);
    Ok(())
}

/// Compare the mirrored root with the on-chain root. The confirmed root can
/// briefly trail the last processed certify, so retry the comparison before
/// declaring divergence.
async fn verify_mirror_root<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &impl TapeOperator,
    mirror: &Mutex<ArchiveMirror>,
) -> Result<(), TapedriveError> {
    let expected = mirror.lock().await.root();

    let mut observed = Hash::default();
    for attempt in 0..ROOT_CHECK_ATTEMPTS {
        if attempt > 0 {
            sleep(ROOT_CHECK_INTERVAL).await;
        }
        let tape = client.get_tape(&tape_key.address()).await?;
        observed = tape.tracks.tree.root();
        if observed == expected {
            return Ok(());
        }
    }

    Err(stream_error(StreamError::Integrity(format!(
        "tape track tree diverged from the stream mirror: mirror root {expected}, chain root {observed}"
    ))))
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
