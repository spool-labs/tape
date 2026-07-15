use axum::body::{Body, Bytes};
use axum::response::{IntoResponse, Response};
use futures::Stream;
use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::track::types::CompressedTrack;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_sdk::stream::manifest::ChunkManifest;

use super::decode::decode_track_bytes;
use super::response::{ByteRange, ObjectResponseMetadata, ranged_object_headers};
use crate::http::error::RouteError;
use crate::http::handlers::track::track_with_pending;
use crate::http::state::AppState;

/// One chunk a read will serve: its manifest index, the bytes the whole chunk
/// decodes to, and the byte window of it to send. A full read serves every
/// chunk whole; a ranged read skips the head of the first chunk and trims the
/// tail of the last.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlannedChunk {
    pub index: usize,
    pub decoded_size: u64,
    pub skip: u64,
    pub take: u64,
}

/// Map a byte window onto the manifest's chunk extents: every chunk the
/// half-open window touches, in order. Chunks are contiguous and ordered, so
/// the result is a consecutive run with at most the first and last entries
/// trimmed. Pure manifest arithmetic; no store access.
pub fn chunk_range_plan(
    manifest: &ChunkManifest,
    range: ByteRange,
) -> Vec<PlannedChunk> {
    let mut plan = Vec::new();
    for (index, entry) in manifest.chunks.iter().enumerate() {
        let (offset, size) = (entry.offset.to_bytes(), entry.size.to_bytes());
        if offset >= range.end {
            break;
        }
        if offset + size <= range.start {
            continue;
        }
        plan.push(PlannedChunk {
            index,
            decoded_size: size,
            skip: range.start.saturating_sub(offset),
            take: (offset + size).min(range.end) - offset.max(range.start),
        });
    }
    plan
}

/// One chunk of the response body: the resolved track to decode plus its
/// planned byte window.
#[derive(Clone, Copy)]
struct StreamChunk {
    index: usize,
    track_addr: Address,
    track: CompressedTrack,
    expected_size: u64,
    skip: u64,
    take: u64,
}

/// Resolve the planned chunks' track records, and only those: a ranged read
/// never touches the store for chunks outside its window.
fn resolve_planned_chunks<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    manifest: &ChunkManifest,
    plan: &[PlannedChunk],
) -> Result<Vec<StreamChunk>, RouteError> {
    let mut chunks = Vec::with_capacity(plan.len());
    for planned in plan {
        let entry = &manifest.chunks[planned.index];
        let chunk_addr = track_pda(tape, entry.track_number).0.into();
        let chunk = track_with_pending(state, chunk_addr)?.ok_or(RouteError::NotFound)?;
        if !chunk.is_certified() {
            return Err(RouteError::BadGateway(format!(
                "manifest chunk {} is not certified",
                planned.index
            )));
        }

        chunks.push(StreamChunk {
            index: planned.index,
            track_addr: chunk_addr,
            track: chunk,
            expected_size: planned.decoded_size,
            skip: planned.skip,
            take: planned.take,
        });
    }

    Ok(chunks)
}

pub fn object_stream_response<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    tape: Address,
    manifest: &ChunkManifest,
    plan: &[PlannedChunk],
    metadata: ObjectResponseMetadata,
    etag: Hash,
    total_size: u64,
    range: Option<ByteRange>,
) -> Result<Response, RouteError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let chunks = resolve_planned_chunks(&state, tape, manifest, plan)?;
    let (status, headers) = ranged_object_headers(range, total_size, &metadata, etag)?;
    let body = Body::from_stream(manifest_chunk_stream(state, chunks));
    Ok((status, headers, body).into_response())
}

fn manifest_chunk_stream<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<StreamChunk>,
) -> impl Stream<Item = Result<Bytes, RouteError>> + Send + 'static
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    futures::stream::try_unfold(
        ObjectStreamState {
            state,
            chunks,
            next: 0,
        },
        |mut stream| async move {
            let Some(chunk) = stream.chunks.get(stream.next).copied() else {
                return Ok(None);
            };
            stream.next += 1;

            // The whole chunk decodes and verifies against the manifest size
            // before any slicing; a ranged read changes what is sent, never
            // what is checked.
            let decoded = decode_track_bytes(&stream.state, chunk.track_addr, chunk.track).await?;
            if decoded.bytes.len() as u64 != chunk.expected_size {
                return Err(RouteError::BadGateway(format!(
                    "manifest chunk {} size mismatch",
                    chunk.index
                )));
            }

            stream
                .state
                .context
                .metrics
                .add_downloaded(decoded.bytes.len() as u64);

            let bytes = Bytes::from(decoded.bytes)
                .slice(chunk.skip as usize..(chunk.skip + chunk.take) as usize);
            Ok(Some((bytes, stream)))
        },
    )
}

struct ObjectStreamState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<StreamChunk>,
    next: usize,
}

#[cfg(test)]
mod tests {
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_crypto::Hash;
    use tape_sdk::stream::manifest::{ChunkEntry, ChunkManifest, MANIFEST_VERSION};

    use super::ByteRange;
    use super::chunk_range_plan;

    // Four contiguous 100-byte chunks with a short 50-byte tail.
    fn manifest() -> ChunkManifest {
        ChunkManifest {
            version: MANIFEST_VERSION,
            total_size: StorageUnits::from_bytes(450),
            chunk_count: TrackNumber(5),
            chunk_size: StorageUnits::from_bytes(100),
            key: Hash::default(),
            chunks: [(0, 100), (100, 100), (200, 100), (300, 100), (400, 50)]
                .into_iter()
                .enumerate()
                .map(|(index, (offset, size))| ChunkEntry {
                    track_number: TrackNumber(index as u64),
                    offset: StorageUnits::from_bytes(offset),
                    size: StorageUnits::from_bytes(size),
                })
                .collect(),
        }
    }

    fn plan(start: u64, end: u64) -> Vec<(usize, u64, u64)> {
        chunk_range_plan(&manifest(), ByteRange { start, end })
            .into_iter()
            .map(|chunk| (chunk.index, chunk.skip, chunk.take))
            .collect()
    }

    #[test]
    fn range_inside_one_middle_chunk_decodes_only_it() {
        assert_eq!(plan(210, 260), vec![(2, 10, 50)]);
    }

    #[test]
    fn range_spanning_a_boundary_trims_head_and_tail() {
        assert_eq!(plan(150, 250), vec![(1, 50, 50), (2, 0, 50)]);
    }

    #[test]
    fn range_on_exact_chunk_boundaries_serves_whole_chunks() {
        assert_eq!(plan(100, 300), vec![(1, 0, 100), (2, 0, 100)]);
    }

    #[test]
    fn suffix_range_covers_the_short_last_chunk() {
        assert_eq!(plan(380, 450), vec![(3, 80, 20), (4, 0, 50)]);
    }

    #[test]
    fn full_range_serves_everything_untrimmed() {
        assert_eq!(
            plan(0, 450),
            vec![(0, 0, 100), (1, 0, 100), (2, 0, 100), (3, 0, 100), (4, 0, 50)]
        );
    }

    #[test]
    fn served_lengths_sum_to_the_range_length() {
        for (start, end) in [(0, 1), (99, 101), (150, 420), (399, 450), (0, 450)] {
            let total: u64 = plan(start, end).iter().map(|(_, _, take)| take).sum();
            assert_eq!(total, end - start, "range {start}..{end}");
        }
    }

    #[test]
    fn plan_carries_the_decoded_chunk_sizes_for_metering() {
        let charged = |start, end| -> u64 {
            chunk_range_plan(&manifest(), ByteRange { start, end })
                .iter()
                .map(|chunk| chunk.decoded_size)
                .sum()
        };

        // One byte inside a middle chunk charges that whole chunk.
        assert_eq!(charged(250, 251), 100);
        // A boundary-spanning range charges both touched chunks.
        assert_eq!(charged(150, 250), 200);
        // The whole object charges every chunk.
        assert_eq!(charged(0, 450), 450);
    }
}
