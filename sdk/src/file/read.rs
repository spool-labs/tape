//! File read implementation.
//!
//! Reads a manifest track, then fetches and reassembles all chunks.

use futures::stream::{self, StreamExt};
use tokio::sync::Semaphore;

use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_api::program::tapedrive::track_pda;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

use super::error::FileError;
use super::manifest::ChunkManifest;

/// Maximum concurrent chunk downloads.
const CHUNK_CONCURRENCY: usize = 8;

/// Read a manifest track, fetch all chunks, and reassemble the file
pub async fn read_file<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Pubkey,
) -> Result<Vec<u8>, TapedriveError> {
    let manifest_bytes = client.read(manifest_address).await?;
    let manifest = ChunkManifest::from_bytes(&manifest_bytes)
        .map_err(|e| TapedriveError::File(format!("invalid manifest: {e}")))?;

    let manifest_track = client.get_track(manifest_address).await?;
    let tape_pda = manifest_track.tape;

    let chunk_count = manifest.chunks.len();
    let semaphore = Semaphore::new(CHUNK_CONCURRENCY);

    let chunk_futures: Vec<_> = manifest
        .chunks
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let sem = &semaphore;
            let track_address = track_pda(tape_pda, entry.track_number).0;
            let expected_size = entry.size;
            async move {
                let _permit = sem.acquire().await.map_err(|_| {
                    file_error(FileError::Chunk("chunk download semaphore closed".into()))
                })?;
                let data = client.read(&track_address).await?;
                if data.len() as u64 != expected_size {
                    return Err(file_error(FileError::Chunk(format!(
                        "chunk {i} size mismatch: expected {expected_size}, got {}",
                        data.len()
                    ))));
                }
                Ok::<_, TapedriveError>((i, data))
            }
        })
        .collect();

    let results: Vec<Result<_, TapedriveError>> = stream::iter(chunk_futures)
        .buffer_unordered(CHUNK_CONCURRENCY)
        .collect()
        .await;

    let mut ordered = vec![None; chunk_count];
    for result in results {
        let (i, data) = result?;
        ordered[i] = Some(data);
    }

    let mut file = Vec::with_capacity(manifest.total_size as usize);
    for (i, slot) in ordered.into_iter().enumerate() {
        let chunk = slot.ok_or_else(|| {
            file_error(FileError::Chunk(format!("chunk {i} missing after download")))
        })?;
        file.extend_from_slice(&chunk);
    }

    if file.len() as u64 != manifest.total_size {
        return Err(file_error(FileError::Integrity(format!(
            "reassembled size mismatch: expected {}, got {}",
            manifest.total_size,
            file.len()
        ))));
    }

    Ok(file)
}

fn file_error(error: FileError) -> TapedriveError {
    TapedriveError::File(error.to_string())
}
