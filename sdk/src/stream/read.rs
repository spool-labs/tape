//! Stream read implementation.
//!
//! Reads a manifest track, fetches chunk tracks concurrently with bounded
//! memory, and reassembles the original byte stream.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_api::program::tapedrive::track_pda;
use tape_core::track::types::CompressedTrack;
use tape_core::types::StorageUnits;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

use super::error::StreamError;
use super::manifest::ChunkManifest;

/// Maximum concurrent chunk downloads.
const CHUNK_CONCURRENCY: usize = 8;

/// Read a manifest track and return the full stream contents in memory.
pub async fn read_bytes<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Pubkey,
) -> Result<Vec<u8>, TapedriveError> {
    let (manifest, manifest_track) = read_manifest(client, manifest_address).await?;
    let mut buffer = MemoryWriter::new(manifest.total_size)?;
    read_manifest_into(client, &manifest, &manifest_track, &mut buffer).await?;
    Ok(buffer.into_inner())
}

/// Read a manifest track and write the reconstructed stream into an async sink.
pub async fn read_into<Blockchain: Rpc, Cluster: Api, Writer: AsyncWrite + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Pubkey,
    mut writer: Writer,
) -> Result<(), TapedriveError> {
    let (manifest, manifest_track) = read_manifest(client, manifest_address).await?;
    read_manifest_into(client, &manifest, &manifest_track, &mut writer).await
}

/// Load and validate the manifest plus its track metadata.
async fn read_manifest<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Pubkey,
) -> Result<(ChunkManifest, CompressedTrack), TapedriveError> {
    let manifest_bytes = client.read(manifest_address).await?;
    let manifest = ChunkManifest::from_bytes(&manifest_bytes)
        .map_err(|error| stream_error(StreamError::Manifest(format!("invalid manifest: {error}"))))?;
    let manifest_track = client.get_track(manifest_address).await?;
    Ok((manifest, manifest_track))
}

/// Stream manifest chunks into an async writer with bounded buffering.
async fn read_manifest_into<Blockchain: Rpc, Cluster: Api, Writer: AsyncWrite + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest: &ChunkManifest,
    manifest_track: &CompressedTrack,
    writer: &mut Writer,
) -> Result<(), TapedriveError> {
    let tape_address = manifest_track.tape;
    let mut in_flight = FuturesUnordered::new();
    let mut ready_chunks = BTreeMap::<usize, Vec<u8>>::new();
    let mut next_chunk_to_schedule = 0usize;
    let mut next_chunk_to_write = 0usize;
    let mut total_written = StorageUnits::zero();

    while next_chunk_to_schedule < manifest.chunks.len() || !in_flight.is_empty() {
        while next_chunk_to_schedule < manifest.chunks.len() && in_flight.len() < CHUNK_CONCURRENCY {
            let entry = manifest.chunks[next_chunk_to_schedule].clone();
            let track_address = track_pda(tape_address, entry.track_number).0;
            let chunk_index = next_chunk_to_schedule;

            in_flight.push(async move {
                let data = client.read(&track_address).await?;
                let data_size = StorageUnits::from_bytes(data.len() as u64);
                if data_size != entry.size {
                    return Err(stream_error(StreamError::Chunk(format!(
                        "chunk {chunk_index} size mismatch: expected {}, got {}",
                        entry.size,
                        data.len()
                    ))));
                }

                Ok::<_, TapedriveError>((chunk_index, data))
            });

            next_chunk_to_schedule += 1;
        }

        if let Some(result) = in_flight.next().await {
            let (chunk_index, data) = result?;
            ready_chunks.insert(chunk_index, data);
        }

        while let Some(chunk_data) = ready_chunks.remove(&next_chunk_to_write) {
            writer.write_all(&chunk_data).await?;
            total_written = total_written
                .checked_add(StorageUnits::from_bytes(chunk_data.len() as u64))
                .ok_or_else(|| stream_error(StreamError::Integrity("stream size overflow".into())))?;
            next_chunk_to_write += 1;
        }
    }

    writer.flush().await?;

    if total_written != manifest.total_size {
        return Err(stream_error(StreamError::Integrity(format!(
            "reassembled size mismatch: expected {}, got {total_written}",
            manifest.total_size
        ))));
    }

    Ok(())
}

fn stream_error(error: StreamError) -> TapedriveError {
    TapedriveError::Stream(error.to_string())
}

struct MemoryWriter {
    data: Vec<u8>,
}

impl MemoryWriter {
    fn new(total_size: StorageUnits) -> Result<Self, TapedriveError> {
        let capacity = usize::try_from(total_size.to_bytes()).map_err(|_| {
            stream_error(StreamError::InvalidInput(
                "stream too large to fit in memory".into(),
            ))
        })?;

        Ok(Self {
            data: Vec::with_capacity(capacity),
        })
    }

    fn into_inner(self) -> Vec<u8> {
        self.data
    }
}

impl AsyncWrite for MemoryWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _ctx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.data.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}
