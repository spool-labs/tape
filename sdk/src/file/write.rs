//! File write implementation.
//!
//! Splits data into chunks, writes each as a track, then writes a manifest
//! track last. Always writes a manifest, even for single-chunk files.

use std::env;

use futures::stream::{self, StreamExt};
use tokio::sync::Semaphore;

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

use super::error::FileError;
use super::manifest::{CHUNK_SIZE, MANIFEST_VERSION, ChunkEntry, ChunkManifest};
use super::receipt::FileReceipt;

/// Maximum concurrent chunk uploads.
const CHUNK_CONCURRENCY: usize = 4;
const CHUNK_CONCURRENCY_ENV: &str = "TAPE_FILE_CHUNK_CONCURRENCY";

/// Maximum track slots in a tape (2^TRACK_TREE_HEIGHT).
const MAX_TRACKS: u64 = 1 << TRACK_TREE_HEIGHT;

/// Derive a deterministic key for a chunk from the file key and chunk index.
fn chunk_key(file_key: Hash, index: usize) -> Hash {
    hashv(&[file_key.as_ref(), &(index as u64).to_le_bytes()])
}

fn chunk_concurrency() -> usize {
    env::var(CHUNK_CONCURRENCY_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(CHUNK_CONCURRENCY)
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

    let entries = upload_chunks(client, tape_key, key, &data_chunks, data.len()).await?;
    let manifest = build_manifest(key, data.len(), entries);
    let manifest_bytes = manifest
        .to_bytes()
        .map_err(file_error)?;

    let manifest_track = client.write_track(tape_key, key, &manifest_bytes).await?;
    let manifest_address = track_pda(manifest_track.tape, manifest_track.track_number).0;

    Ok(FileReceipt {
        tape: manifest_track.tape,
        manifest: manifest_address,
        manifest_track_number: manifest_track.track_number,
    })
}

/// Upload all chunk tracks and return manifest entries in chunk order
async fn upload_chunks<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape_key: &TapeKey,
    key: Hash,
    data_chunks: &[&[u8]],
    total_len: usize,
) -> Result<Vec<ChunkEntry>, TapedriveError> {
    let chunk_count = data_chunks.len();
    let concurrency = chunk_concurrency();
    let semaphore = Semaphore::new(concurrency);

    let chunk_futures: Vec<_> = data_chunks
        .iter()
        .enumerate()
        .map(|(i, chunk_data)| {
            let sem = &semaphore;
            let chunk_hash = chunk_key(key, i);
            async move {
                let _permit = sem.acquire().await.map_err(|_| {
                    file_error(FileError::Chunk("chunk upload semaphore closed".into()))
                })?;
                let track = client.write_track(tape_key, chunk_hash, chunk_data).await?;
                Ok::<_, TapedriveError>((i, track))
            }
        })
        .collect();

    let results: Vec<Result<_, TapedriveError>> = stream::iter(chunk_futures)
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut entries = vec![None; chunk_count];
    for result in results {
        let (i, track) = result?;
        entries[i] = Some(ChunkEntry {
            track_number: track.track_number,
            offset: i as u64 * CHUNK_SIZE as u64,
            size: chunk_size(i, chunk_count, total_len),
        });
    }

    // Every slot must be filled — upload_chunks is only called after preflight.
    let mut result = Vec::with_capacity(chunk_count);
    for (i, slot) in entries.into_iter().enumerate() {
        result.push(slot.ok_or_else(|| {
            file_error(FileError::Chunk(format!("chunk {i} missing after upload")))
        })?);
    }
    Ok(result)
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
        let error = build_entries(TrackNumber(u64::MAX), 1, CHUNK_SIZE)
            .expect_err("entries should fail");

        match error {
            FileError::InvalidInput(message) => {
                assert_eq!(message, "chunk track number overflow");
            }
            other => panic!("expected chunk track number overflow, got {other}"),
        }
    }
}
