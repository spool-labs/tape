//! Stream read implementation.
//!
//! Reads a manifest track, fetches its segment tracks with a short read-ahead,
//! and reassembles the original byte stream.

use std::collections::HashMap;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};

use futures::stream::{self, Stream, StreamExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::join;
use tracing::debug;

use tape_rpc::Rpc;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::metrics::{Metrics, Operation, Phase, Timer};
use crate::tapedrive::Tapedrive;

use super::error::StreamError;
use super::manifest::{ChunkEntry, ChunkManifest};

/// Read a manifest track and return the full stream contents in memory.
pub async fn read_bytes<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Address,
) -> Result<Vec<u8>, TapedriveError> {
    let (manifest, manifest_track) = read_manifest(client, manifest_address).await?;
    read_manifest_bytes(client, &manifest, &manifest_track).await
}

/// Read a manifest track and write the reconstructed stream into an async sink.
pub async fn read_into<Blockchain: Rpc, Cluster: Api, Writer: AsyncWrite + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Address,
    mut writer: Writer,
) -> Result<(), TapedriveError> {
    let (manifest, manifest_track) = read_manifest(client, manifest_address).await?;
    read_manifest_into(client, &manifest, &manifest_track, &mut writer).await
}

/// Load and validate the manifest plus its track metadata.
pub(crate) async fn read_manifest<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest_address: &Address,
) -> Result<(ChunkManifest, CompressedTrack), TapedriveError> {
    let metadata = client.timer(Operation::ReadStream, Phase::TrackMetadata);
    let manifest_track = client.get_track(manifest_address).await;
    metadata.finish_result(&manifest_track);
    let manifest_track = manifest_track?;
    let manifest_bytes = client
        .read_known(manifest_address, manifest_track, Operation::ReadStream)
        .await?;
    let manifest = ChunkManifest::from_bytes(&manifest_bytes)
        .map_err(|error| stream_error(StreamError::Manifest(format!("invalid manifest: {error}"))))?;
    Ok((manifest, manifest_track))
}

/// Reassemble a stream in memory from a manifest the caller already parsed
pub(crate) async fn read_manifest_bytes<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest: &ChunkManifest,
    manifest_track: &CompressedTrack,
) -> Result<Vec<u8>, TapedriveError> {
    let mut buffer = MemoryWriter::new(manifest.total_size)?;
    read_manifest_into(client, manifest, manifest_track, &mut buffer).await?;
    Ok(buffer.into_inner())
}

/// A stream read loads this many segments while it writes the previous one
pub const STREAM_READ_AHEAD: usize = 2;

/// One track listing covers this many manifest entries
const SEGMENT_PAGE: usize = 256;

/// Stream manifest segments into an async writer in order, reading ahead while each one is written.
pub(crate) async fn read_manifest_into<Blockchain: Rpc, Cluster: Api, Writer: AsyncWrite + Unpin>(
    client: &Tapedrive<Blockchain, Cluster>,
    manifest: &ChunkManifest,
    manifest_track: &CompressedTrack,
    writer: &mut Writer,
) -> Result<(), TapedriveError> {
    let tape = manifest_track.tape;
    let mut total_written = StorageUnits::zero();

    for (page, entries) in manifest.chunks.chunks(SEGMENT_PAGE).enumerate() {
        let tracks = segment_tracks(client, tape, entries).await?;
        let first = page * SEGMENT_PAGE;
        let reads = entries
            .iter()
            .zip(tracks)
            .enumerate()
            .map(|(offset, (entry, track))| read_segment(client, first + offset, entry, track));
        let reads = pin!(stream::iter(reads).buffered(STREAM_READ_AHEAD));
        let written = write_in_order(reads, writer, client.metrics.as_ref()).await?;
        total_written = total_written
            .checked_add(written)
            .ok_or_else(|| stream_error(StreamError::Integrity("stream size overflow".into())))?;
    }

    let write_sink = client.timer(Operation::ReadStream, Phase::WriteSink);
    let result = writer.flush().await;
    write_sink.finish_result(&result);
    result?;

    if total_written != manifest.total_size {
        return Err(stream_error(StreamError::Integrity(format!(
            "reassembled size mismatch: expected {}, got {total_written}",
            manifest.total_size
        ))));
    }

    Ok(())
}

/// Look up a run of segment tracks with one listing page, then one by one for any the page missed
async fn segment_tracks<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: Address,
    entries: &[ChunkEntry],
) -> Result<Vec<CompressedTrack>, TapedriveError> {
    let (Some(first), Some(last)) = (
        entries.iter().map(|entry| entry.track_number).min(),
        entries.iter().map(|entry| entry.track_number).max(),
    ) else {
        return Ok(Vec::new());
    };

    let mut listed = HashMap::with_capacity(entries.len());
    let span = last.0.saturating_sub(first.0).saturating_add(1);
    if span <= (SEGMENT_PAGE * STREAM_READ_AHEAD * 2) as u64 {
        let cursor = first.0.checked_sub(1).map(TrackNumber);
        match client.list_tracks_by_tape(&tape, cursor, span as u32).await {
            Ok((tracks, _)) => {
                for track in tracks {
                    if track.tape == tape {
                        listed.insert(track.track_number, track);
                    }
                }
            }
            Err(error) => debug!(%tape, %error, "segment listing failed, looking tracks up one by one"),
        }
    }

    let mut tracks = Vec::with_capacity(entries.len());
    for entry in entries {
        let track = match listed.get(&entry.track_number) {
            Some(track) => *track,
            None => client.get_track_by_number(&tape, entry.track_number).await?,
        };
        tracks.push(track);
    }
    Ok(tracks)
}

async fn read_segment<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    index: usize,
    entry: &ChunkEntry,
    track: CompressedTrack,
) -> Result<Vec<u8>, TapedriveError> {
    let address = track.address();
    if address != entry.track {
        return Err(stream_error(StreamError::Chunk(format!(
            "segment {index} address mismatch: manifest lists {}, track {} is {address}",
            entry.track, entry.track_number
        ))));
    }
    let data = client.read_known(&address, track, Operation::ReadStream).await?;
    if StorageUnits::from_bytes(data.len() as u64) != entry.size {
        return Err(stream_error(StreamError::Chunk(format!(
            "segment {index} size mismatch: expected {}, got {}",
            entry.size,
            data.len()
        ))));
    }
    Ok(data)
}

/// Write each segment to the sink in order while the reads behind it keep loading
async fn write_in_order<Reads, Writer>(
    mut reads: Reads,
    writer: &mut Writer,
    metrics: &dyn Metrics,
) -> Result<StorageUnits, TapedriveError>
where
    Reads: Stream<Item = Result<Vec<u8>, TapedriveError>> + Unpin,
    Writer: AsyncWrite + Unpin,
{
    let mut written = StorageUnits::zero();
    let mut next = reads.next().await;
    while let Some(segment) = next {
        let segment = segment?;
        let write_sink = Timer::start(metrics, Operation::ReadStream, Phase::WriteSink)
            .bytes(segment.len() as u64);
        let (result, following) = join!(writer.write_all(&segment), reads.next());
        write_sink.finish_result(&result);
        result?;

        written = written
            .checked_add(StorageUnits::from_bytes(segment.len() as u64))
            .ok_or_else(|| stream_error(StreamError::Integrity("stream size overflow".into())))?;
        next = following;
    }
    Ok(written)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::metrics::Noop;

    use super::*;

    const SEGMENTS: u8 = 4;

    // segments reach the sink in order while later ones load during each write
    #[tokio::test]
    async fn read_ahead() {
        let loading = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let reads = (0..SEGMENTS).map(|index| {
            let (loading, peak) = (loading.clone(), peak.clone());
            async move {
                let now = loading.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let delay = if index == 0 { 30 } else { 5 };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                loading.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, TapedriveError>(vec![index; 2])
            }
        });
        let reads = pin!(stream::iter(reads).buffered(STREAM_READ_AHEAD));
        let mut sink = Vec::new();

        let written = write_in_order(reads, &mut sink, &Noop).await.expect("write");

        assert_eq!(sink, vec![0, 0, 1, 1, 2, 2, 3, 3]);
        assert_eq!(written, StorageUnits::from_bytes(8));
        assert!(peak.load(Ordering::SeqCst) >= 2);
    }
}
