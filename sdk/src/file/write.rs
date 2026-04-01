//! File write implementation.
//!
//! Splits data into chunks, writes each as a track, then writes a manifest
//! track last. Always writes a manifest, even for single-chunk files.

use std::time::Instant;

use futures::stream::{self, StreamExt};

use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_api::state::Tape;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::Hash;
use tape_crypto::hash::hashv;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;
use crate::track::write::{UploadPlan, WrittenTrack};

use super::error::FileError;
use super::manifest::{CHUNK_SIZE, MANIFEST_VERSION, ChunkEntry, ChunkManifest};
use super::receipt::FileReceipt;

/// Maximum concurrent chunk registrations/uploads.
const CHUNK_CONCURRENCY: usize = 4;

/// Maximum track slots in a tape (2^TRACK_TREE_HEIGHT).
const MAX_TRACKS: u64 = 1 << TRACK_TREE_HEIGHT;

struct PendingChunk {
    pub entry: ChunkEntry,
    pub written: WrittenTrack,
    pub plan: UploadPlan,
}

/// Derive a deterministic key for a chunk from the file key and chunk index.
fn chunk_key(file_key: Hash, index: usize) -> Hash {
    hashv(&[file_key.as_ref(), &(index as u64).to_le_bytes()])
}

/// Validate file-level input before any track writes begin
fn validate_data(data: &[u8]) -> Result<(), FileError> {
    if data.is_empty() {
        return Err(FileError::InvalidInput("empty files are not supported"));
    }

    Ok(())
}

/// Return the stored byte size for a chunk at the given index
fn chunk_size(chunk_index: usize, chunk_count: usize, total_len: usize) -> u64 {
    if chunk_index == chunk_count - 1 {
        total_len as u64 - (chunk_index as u64 * CHUNK_SIZE as u64)
    } else {
        CHUNK_SIZE as u64
    }
}

/// Build deterministic manifest entries before or after upload
fn build_entries(
    start_track_number: TrackNumber,
    chunk_count: usize,
    total_len: usize,
) -> Result<Vec<ChunkEntry>, FileError> {
    let mut entries = Vec::with_capacity(chunk_count);

    for chunk_index in 0..chunk_count {
        let track_number = start_track_number
            .checked_add(TrackNumber(chunk_index as u64))
            .ok_or(FileError::InvalidInput("chunk track number overflow"))?;

        entries.push(ChunkEntry {
            track_number,
            offset: chunk_index as u64 * CHUNK_SIZE as u64,
            size: chunk_size(chunk_index, chunk_count, total_len),
        });
    }

    Ok(entries)
}

/// Build a chunk manifest from ordered entries
fn build_manifest(
    key: Hash,
    total_len: usize,
    entries: Vec<ChunkEntry>,
) -> ChunkManifest {
    ChunkManifest {
        version: MANIFEST_VERSION,
        total_size: total_len as u64,
        chunk_count: entries.len() as u64,
        chunk_size: CHUNK_SIZE as u64,
        key,
        chunks: entries,
    }
}

/// Verify the tape has enough capacity and track slots for the file.
fn preflight(
    tape: &Tape,
    total_required_bytes: u64,
    tracks_needed: u64,
) -> Result<(), TapedriveError> {
    let available_capacity = tape.capacity.to_bytes().saturating_sub(tape.used.to_bytes());
    if total_required_bytes > available_capacity {
        return Err(TapedriveError::InsufficientCapacity {
            need: StorageUnits::from_bytes(total_required_bytes),
            available: StorageUnits::from_bytes(available_capacity),
        });
    }

    let used_tracks = tape.tracks.next_number().as_u64();
    let available_tracks = MAX_TRACKS.saturating_sub(used_tracks);
    if tracks_needed > available_tracks {
        let chunk_count = tracks_needed - 1;
        return Err(file_error(FileError::InsufficientTrackSlots {
            available: available_tracks,
            needed: tracks_needed,
            chunks: chunk_count,
        }));
    }

    Ok(())
}

/// Write a file as chunk tracks followed by a manifest track
pub async fn write_file<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    data: &[u8],
) -> Result<FileReceipt, TapedriveError> {
    validate_data(data).map_err(file_error)?;

    let data_chunks: Vec<&[u8]> = data.chunks(CHUNK_SIZE).collect();
    let chunk_count = data_chunks.len();
    let tracks_needed = chunk_count as u64 + 1;

    let tape = client.get_tape(&tape_key.address()).await?;
    let entries = build_entries(tape.tracks.next_number(), chunk_count, data.len())
        .map_err(file_error)?;
    let manifest = build_manifest(key, data.len(), entries);
    let manifest_bytes = manifest
        .to_bytes()
        .map_err(file_error)?;
    let total_size = (data.len() as u64)
        .checked_add(manifest_bytes.len() as u64)
        .ok_or_else(|| file_error(FileError::InvalidInput("file size overflow")))?;

    preflight(&tape, total_size, tracks_needed)?;

    let pending_chunks = stage_chunks(client, tape_key, key, &data_chunks, data.len()).await?;
    upload_chunks(client, &pending_chunks).await?;
    certify_chunks(client, tape_key, &pending_chunks).await?;

    let entries = pending_chunks
        .into_iter()
        .map(|pending_chunk| pending_chunk.entry)
        .collect();
    let manifest = build_manifest(key, data.len(), entries);
    let manifest_bytes = manifest
        .to_bytes()
        .map_err(file_error)?;

    let manifest_track = write_manifest(client, tape_key, key, &manifest_bytes).await?;
    let manifest_address = track_pda(manifest_track.track.tape, manifest_track.track.track_number).0;

    Ok(FileReceipt {
        tape: manifest_track.track.tape,
        manifest: manifest_address,
        manifest_track_number: manifest_track.track.track_number,
    })
}

/// Register all chunk tracks and collect upload plans in chunk order.
async fn stage_chunks<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    data_chunks: &[&[u8]],
    total_len: usize,
) -> Result<Vec<PendingChunk>, TapedriveError> {
    let chunk_count = data_chunks.len();

    let chunk_futures: Vec<_> = data_chunks
        .iter()
        .enumerate()
        .map(|(chunk_index, chunk_data)| {
            let chunk_hash = chunk_key(key, chunk_index);
            async move {
                let (written, plan) = client.write_blob(tape_key, chunk_hash, chunk_data).await?;
                Ok::<_, TapedriveError>((
                    chunk_index,
                    PendingChunk {
                        entry: ChunkEntry {
                            track_number: written.track.track_number,
                            offset: chunk_index as u64 * CHUNK_SIZE as u64,
                            size: chunk_size(chunk_index, chunk_count, total_len),
                        },
                        written,
                        plan,
                    },
                ))
            }
        })
        .collect();

    let results: Vec<Result<_, TapedriveError>> = stream::iter(chunk_futures)
        .buffer_unordered(CHUNK_CONCURRENCY)
        .collect()
        .await;

    let mut pending_chunks = Vec::with_capacity(chunk_count);
    pending_chunks.resize_with(chunk_count, || None);

    for result in results {
        let (chunk_index, pending_chunk) = result?;
        pending_chunks[chunk_index] = Some(pending_chunk);
    }

    let mut ordered_chunks = Vec::with_capacity(chunk_count);
    for (chunk_index, pending_chunk) in pending_chunks.into_iter().enumerate() {
        ordered_chunks.push(pending_chunk.ok_or_else(|| {
            file_error(FileError::Chunk(format!(
                "chunk {chunk_index} missing after registration",
            )))
        })?);
    }

    Ok(ordered_chunks)
}

/// Upload staged chunk tracks concurrently after registration succeeds.
async fn upload_chunks<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    pending_chunks: &[PendingChunk],
) -> Result<(), TapedriveError> {
    let upload_futures: Vec<_> = pending_chunks
        .iter()
        .map(|pending_chunk| async move {
            let upload_phase_start = Instant::now();
            client.upload(&pending_chunk.written, &pending_chunk.plan).await?;
            let upload_phase_elapsed = upload_phase_start.elapsed();

            eprintln!(
                "slice upload phase: track_number={} size_bytes={} elapsed={:.2?}",
                pending_chunk.written.track.track_number.0,
                pending_chunk.entry.size,
                upload_phase_elapsed,
            );

            Ok::<_, TapedriveError>(())
        })
        .collect();

    let results: Vec<Result<_, TapedriveError>> = stream::iter(upload_futures)
        .buffer_unordered(CHUNK_CONCURRENCY)
        .collect()
        .await;

    for result in results {
        result?;
    }

    Ok(())
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
        client.certify(tape_key, &pending_chunk.written).await?;
    }

    Ok(())
}

/// Write the manifest as a blob track, upload it, then certify it last.
async fn write_manifest<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    manifest_bytes: &[u8],
) -> Result<WrittenTrack, TapedriveError> {
    let (written, plan) = client.write_blob(tape_key, key, manifest_bytes).await?;
    let upload_phase_start = Instant::now();
    client.upload(&written, &plan).await?;
    let upload_phase_elapsed = upload_phase_start.elapsed();

    eprintln!(
        "slice upload phase: track_number={} size_bytes={} elapsed={:.2?}",
        written.track.track_number.0,
        manifest_bytes.len(),
        upload_phase_elapsed,
    );

    client.certify(tape_key, &written).await?;
    Ok(written)
}

fn file_error(error: FileError) -> TapedriveError {
    TapedriveError::File(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use solana_sdk::pubkey::Pubkey;
    use tape_api::state::Tape;
    use tape_core::types::{EpochNumber, StorageUnits, TapeNumber, TrackNumber};

    use super::*;

    fn make_tape(capacity_bytes: u64, used_bytes: u64, next_track_number: u64) -> Tape {
        let mut tape = Tape::zeroed();
        tape.id = TapeNumber(1);
        tape.authority = Pubkey::new_unique();
        tape.capacity = StorageUnits::from_bytes(capacity_bytes);
        tape.used = StorageUnits::from_bytes(used_bytes);
        tape.active_epoch = EpochNumber(1);
        tape.expiry_epoch = EpochNumber(2);
        tape.tracks.next_number = TrackNumber(next_track_number);
        tape
    }

    // capacity checks include the serialized manifest bytes
    #[test]
    fn manifest_size() {
        let key = Hash::from([0x11; 32]);
        let total_len = CHUNK_SIZE;
        let entries = build_entries(TrackNumber(0), 1, total_len).expect("build entries");
        let manifest = build_manifest(key, total_len, entries);
        let manifest_bytes = manifest.to_bytes().expect("serialize manifest");
        let total_size = total_len as u64 + manifest_bytes.len() as u64;
        let tape = make_tape(total_size - 1, 0, 0);

        let error = preflight(&tape, total_size, 2).expect_err("preflight should fail");

        match error {
            TapedriveError::InsufficientCapacity { need, available } => {
                assert_eq!(need.to_bytes(), total_size);
                assert_eq!(available.to_bytes(), total_size - 1);
            }
            other => panic!("expected insufficient capacity, got {other}"),
        }
    }

    // empty files are rejected before chunk planning
    #[test]
    fn empty_file() {
        let error = validate_data(&[]).expect_err("empty file should fail");

        match error {
            FileError::InvalidInput(message) => {
                assert_eq!(message, "empty files are not supported");
            }
            other => panic!("expected empty file error, got {other}"),
        }
    }

    // manifest entries reject track number overflow
    #[test]
    fn overflow() {
        let error = build_entries(TrackNumber(u64::MAX), 2, CHUNK_SIZE * 2)
            .expect_err("entries should fail");

        match error {
            FileError::InvalidInput(message) => {
                assert_eq!(message, "chunk track number overflow");
            }
            other => panic!("expected chunk track number overflow, got {other}"),
        }
    }
}
