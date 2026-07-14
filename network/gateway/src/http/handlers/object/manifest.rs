use axum::body::{Body, Bytes};
use axum::http::StatusCode;
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
use super::response::{object_headers, ObjectResponseMetadata};
use crate::http::error::RouteError;
use crate::http::handlers::track::track_with_pending;
use crate::http::state::AppState;

#[derive(Clone, Copy)]
pub(in crate::http::handlers::object) struct ManifestChunk {
    index: usize,
    track_addr: Address,
    track: CompressedTrack,
    expected_size: u64,
}

pub(in crate::http::handlers::object) fn manifest_chunks<
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    manifest: &ChunkManifest,
) -> Result<Vec<ManifestChunk>, RouteError> {
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    for (chunk_index, entry) in manifest.chunks.iter().enumerate() {
        let chunk_addr = track_pda(tape, entry.track_number).0.into();
        let chunk = track_with_pending(state, chunk_addr)?.ok_or(RouteError::NotFound)?;
        if !chunk.is_certified() {
            return Err(RouteError::BadGateway(format!(
                "manifest chunk {chunk_index} is not certified"
            )));
        }

        chunks.push(ManifestChunk {
            index: chunk_index,
            track_addr: chunk_addr,
            track: chunk,
            expected_size: entry.size.to_bytes(),
        });
    }

    Ok(chunks)
}

pub(in crate::http::handlers::object) fn object_stream_response<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<ManifestChunk>,
    metadata: ObjectResponseMetadata,
    etag: Hash,
    content_length: u64,
) -> Result<Response, RouteError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let headers = object_headers(content_length, &metadata, etag)?;
    let body = Body::from_stream(manifest_chunk_stream(state, chunks));
    Ok((StatusCode::OK, headers, body).into_response())
}

fn manifest_chunk_stream<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<ManifestChunk>,
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

            Ok(Some((Bytes::from(decoded.bytes), stream)))
        },
    )
}

struct ObjectStreamState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    state: AppState<Db, Cluster, Blockchain>,
    chunks: Vec<ManifestChunk>,
    next: usize,
}
