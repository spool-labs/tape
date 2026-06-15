//! Stream write implementation.
//!
//! Byte streams are split into chunk tracks, each chunk is registered and
//! uploaded, chunk certifications are serialized per tape, and the manifest
//! track is written last.

use futures::stream::{self, FuturesUnordered, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};

use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_api::state::Tape;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::types::ContentType;
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::hash::hash;
use tape_crypto::Hash;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;
use crate::metrics::{Operation, Phase};
use crate::track::write::{
    certify_with_retry, submit_blob, upload_with_retry, WrittenTrack, UNNAMED_TRACK, UNTYPED_TRACK,
};

use super::error::StreamError;
use super::manifest::{ChunkEntry, ChunkManifest, CHUNK_SIZE, MANIFEST_VERSION};
use super::receipt::StreamReceipt;

/// Maximum concurrent chunk registrations/uploads.
const CHUNK_CONCURRENCY: usize = 1;

/// Maximum track slots in a tape (2^TRACK_TREE_HEIGHT).
const MAX_TRACKS: TrackNumber = TrackNumber(1 << TRACK_TREE_HEIGHT);

// Chunks are internal fragments addressed by track number, never by name. The
// object's name and content type live on the manifest track.
struct PendingChunk {
    pub entry: ChunkEntry,
    pub written: WrittenTrack,
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
    let chunk_count = size.to_bytes().div_ceil(CHUNK_SIZE as u64);
    Ok(TrackNumber(chunk_count))
}

/// Return the byte offset for a chunk index.
fn chunk_offset(chunk_index: usize) -> Result<StorageUnits, StreamError> {
    let chunk_index = u64::try_from(chunk_index)
        .map_err(|_| StreamError::InvalidInput("stream has too many chunks".into()))?;
    let offset = chunk_index
        .checked_mul(CHUNK_SIZE as u64)
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
        Ok(StorageUnits::from_bytes(CHUNK_SIZE as u64))
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
        chunk_size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
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
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    data: &[u8],
) -> Result<StreamReceipt, TapedriveError> {
    let size = StorageUnits::from_bytes(data.len() as u64);
    let chunk_count = prepare_write(client, tape_key, name, size).await?;
    let pending_chunks = register_and_upload_bytes_chunks(
        client,
        tape_key,
        data,
        size,
        chunk_count,
    )
    .await?;

    finalize_write(client, tape_key, name, content_type, size, pending_chunks).await
}

/// Write bytes from an async reader as a multi-track stream.
pub async fn write_stream<Blockchain: Rpc, Cluster: Api, Reader: AsyncRead + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    size: StorageUnits,
    mut reader: Reader,
) -> Result<StreamReceipt, TapedriveError> {
    let chunk_count = prepare_write(client, tape_key, name, size).await?;
    let pending_chunks = register_and_upload_stream_chunks(
        client,
        tape_key,
        size,
        chunk_count,
        &mut reader,
    )
    .await?;

    verify_stream_drained(&mut reader).await?;
    finalize_write(client, tape_key, name, content_type, size, pending_chunks).await
}

/// Validate the write upfront and return the chunk count.
async fn prepare_write<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
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

/// Register and upload all in-memory chunks concurrently.
async fn register_and_upload_bytes_chunks<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    data: &[u8],
    size: StorageUnits,
    chunk_count: TrackNumber,
) -> Result<Vec<PendingChunk>, TapedriveError> {
    let chunk_futures: Vec<_> = data
        .chunks(CHUNK_SIZE)
        .enumerate()
        .map(|(chunk_index, chunk_data)| {
            process_chunk(
                client,
                tape_key,
                chunk_index,
                chunk_count,
                size,
                chunk_data,
            )
        })
        .collect();

    let results: Vec<Result<_, TapedriveError>> = stream::iter(chunk_futures)
        .buffer_unordered(CHUNK_CONCURRENCY)
        .collect()
        .await;

    collect_pending_chunks(results, chunk_count.as_usize())
}

/// Register and upload streamed chunks concurrently with bounded memory.
async fn register_and_upload_stream_chunks<Blockchain: Rpc, Cluster: Api, Reader: AsyncRead + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    size: StorageUnits,
    chunk_count: TrackNumber,
    reader: &mut Reader,
) -> Result<Vec<PendingChunk>, TapedriveError> {
    let mut next_chunk_index = 0usize;
    let mut in_flight = FuturesUnordered::new();
    let mut results = Vec::with_capacity(chunk_count.as_usize());

    while next_chunk_index < chunk_count.as_usize() || !in_flight.is_empty() {
        while next_chunk_index < chunk_count.as_usize() && in_flight.len() < CHUNK_CONCURRENCY {
            let expected_chunk_size = chunk_size(next_chunk_index, chunk_count, size)
                .map_err(stream_error)?;
            let mut chunk_data = vec![0u8; expected_chunk_size.as_usize()];
            read_chunk_exact(reader, &mut chunk_data).await?;

            in_flight.push(process_chunk(
                client,
                tape_key,
                next_chunk_index,
                chunk_count,
                size,
                chunk_data,
            ));
            next_chunk_index += 1;
        }

        if let Some(result) = in_flight.next().await {
            results.push(result);
        }
    }

    collect_pending_chunks(results, chunk_count.as_usize())
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

/// Register one chunk track and upload its slices.
async fn process_chunk<Blockchain: Rpc, Cluster: Api, Bytes: AsRef<[u8]>>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    chunk_index: usize,
    chunk_count: TrackNumber,
    size: StorageUnits,
    chunk_data: Bytes,
) -> Result<(usize, PendingChunk), TapedriveError> {
    let (written, plan) = submit_blob(
        client,
        tape_key,
        UNNAMED_TRACK,
        UNTYPED_TRACK,
        chunk_data.as_ref(),
        Operation::WriteStream,
    )
    .await?;

    upload_with_retry(client, &written, &plan, Operation::WriteStream).await?;

    Ok((
        chunk_index,
        PendingChunk {
            entry: ChunkEntry {
                track_number: written.track.track_number,
                offset: chunk_offset(chunk_index).map_err(stream_error)?,
                size: chunk_size(chunk_index, chunk_count, size).map_err(stream_error)?,
            },
            written,
        },
    ))
}

/// Reassemble ordered chunk results after concurrent registration/upload.
fn collect_pending_chunks(
    results: Vec<Result<(usize, PendingChunk), TapedriveError>>,
    chunk_count: usize,
) -> Result<Vec<PendingChunk>, TapedriveError> {
    let mut pending_chunks = Vec::with_capacity(chunk_count);
    pending_chunks.resize_with(chunk_count, || None);

    for result in results {
        let (chunk_index, pending_chunk) = result?;
        pending_chunks[chunk_index] = Some(pending_chunk);
    }

    let mut ordered_chunks = Vec::with_capacity(chunk_count);
    for (chunk_index, pending_chunk) in pending_chunks.into_iter().enumerate() {
        ordered_chunks.push(pending_chunk.ok_or_else(|| {
            stream_error(StreamError::Chunk(format!(
                "chunk {chunk_index} missing after upload",
            )))
        })?);
    }

    Ok(ordered_chunks)
}

/// Certify chunk tracks one at a time so each proof is fetched against the latest tape tree.
async fn certify_chunks<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    pending_chunks: &[PendingChunk],
) -> Result<(), TapedriveError> {
    let mut chunk_order: Vec<&PendingChunk> = pending_chunks.iter().collect();
    chunk_order.sort_by_key(|pending_chunk| pending_chunk.written.track.track_number.0);

    for pending_chunk in chunk_order {
        certify_with_retry(
            client,
            tape_key,
            &pending_chunk.written,
            Operation::WriteStream,
        )
        .await?;
    }

    Ok(())
}

/// Write the manifest as a blob track, upload it, then certify it last.
async fn write_manifest<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    manifest_bytes: &[u8],
) -> Result<WrittenTrack, TapedriveError> {
    let (written, plan) = submit_blob(
        client,
        tape_key,
        name,
        content_type,
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

/// Finalize the stream after chunk registration/upload completes.
async fn finalize_write<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    name: &[u8],
    content_type: ContentType,
    size: StorageUnits,
    pending_chunks: Vec<PendingChunk>,
) -> Result<StreamReceipt, TapedriveError> {
    certify_chunks(client, tape_key, &pending_chunks).await?;

    let entries = pending_chunks
        .into_iter()
        .map(|pending_chunk| pending_chunk.entry)
        .collect();
    let manifest = build_manifest(hash(name), size, entries).map_err(stream_error)?;
    let manifest_bytes = manifest.to_bytes().map_err(stream_error)?;

    let manifest_track = write_manifest(client, tape_key, name, content_type, &manifest_bytes).await?;
    let manifest_address = track_pda(manifest_track.track.tape, manifest_track.track.track_number).0;

    Ok(StreamReceipt {
        tape: manifest_track.track.tape,
        manifest: manifest_address,
        manifest_track_number: manifest_track.track.track_number,
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

    // capacity checks include the serialized manifest bytes.
    #[test]
    fn manifest_size() {
        let key = Hash::from([0x11; 32]);
        let total_size = StorageUnits::from_bytes(CHUNK_SIZE as u64);
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
            StorageUnits::from_bytes(CHUNK_SIZE as u64 * 2),
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
