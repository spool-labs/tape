use std::time::Duration;

use axum::Extension;
use axum::extract::{Path, State};
use axum::response::Response;
use rpc::Rpc;
use store::Store;
use tape_core::track::types::CompressedTrack;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_sdk::stream::manifest::ChunkManifest;

use super::decode::decode_track_bytes;
use super::manifest::{manifest_chunks, object_stream_response};
use super::response::{
    ObjectResponseMetadata, object_response, object_response_metadata, object_response_ranged,
};
use crate::http::error::RouteError;
use crate::http::handlers::track::{parse_address, track_with_pending};
use crate::http::state::AppState;
use crate::meter::{GatewayMeterDecision, MeterCaller, rate_limited_response};

pub(crate) const OBJECT_PATH: &str = "/object/{track_id}";
pub(crate) const TRACK_BYTES_PATH: &str = "/track/{track_id}";

pub(crate) async fn get_object<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path(track_id): Path<String>,
) -> Result<Response, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::BadRequest("track is not certified".into()));
    }

    let metadata = object_response_metadata(&state, track_addr)?;
    read_object_response(
        state,
        track_addr,
        track,
        metadata,
        &caller,
        None,
        rate_limited_response,
    )
    .await
}

/// Decode an already-resolved, certified track and build its read response,
/// auto-detecting the manifest/stream layout.
///
/// `range` is the raw `Range` header value (`bytes=...`), honored only for
/// single-track objects (already decoded in memory); multi-track streams ignore
/// it and serve the whole object.
pub(crate) async fn read_object_response<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    state: AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
    metadata: ObjectResponseMetadata,
    caller: &MeterCaller,
    range: Option<String>,
    rate_limited: impl Fn(Duration) -> Response,
) -> Result<Response, RouteError> {
    match state.meter.check_object_bytes(caller, track.size.to_bytes()) {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited(retry_after));
        }
    }

    let decoded = decode_track_bytes(&state, track_addr, track).await?;
    let Ok(manifest) = ChunkManifest::from_bytes(&decoded.bytes) else {
        state
            .context
            .metrics
            .add_downloaded(decoded.bytes.len() as u64);
        // Single-track object: bytes are in memory, so honor a Range slice here.
        return object_response_ranged(decoded.bytes, &metadata, decoded.etag, range.as_deref());
    };

    match state
        .meter
        .check_object_bytes(caller, manifest.total_size.to_bytes())
    {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited(retry_after));
        }
    }

    let chunks = manifest_chunks(&state, track.tape, &manifest)?;
    object_stream_response(
        state,
        chunks,
        metadata,
        decoded.etag,
        manifest.total_size.to_bytes(),
    )
}

pub(crate) async fn get_track_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path(track_id): Path<String>,
) -> Result<Response, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::BadRequest("track is not certified".into()));
    }

    match state
        .meter
        .check_object_bytes(&caller, track.size.to_bytes())
    {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited_response(retry_after));
        }
    }

    let metadata = object_response_metadata(&state, track_addr)?;
    let decoded = decode_track_bytes(&state, track_addr, track).await?;
    state
        .context
        .metrics
        .add_downloaded(decoded.bytes.len() as u64);

    object_response(decoded.bytes, &metadata, decoded.etag)
}
