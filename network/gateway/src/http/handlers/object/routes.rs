use std::time::Duration;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use rpc::Rpc;
use store::Store;
use tape_core::track::types::CompressedTrack;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_sdk::stream::manifest::ChunkManifest;

use super::decode::decode_track_bytes;
use super::manifest::{chunk_range_plan, object_stream_response};
use super::response::{
    ByteRange, ObjectResponseMetadata, object_response_metadata, object_response_ranged,
    range_header, resolve_range,
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
    headers: HeaderMap,
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
        range_header(&headers).map(str::to_string),
        rate_limited_response,
    )
    .await
}

/// Decode an already-resolved, certified track and build its read response,
/// auto-detecting the manifest/stream layout.
///
/// `range` is the raw `Range` header value (`bytes=...`). Single-track objects
/// slice the decoded bytes in memory; multi-track streams decode only the
/// chunks the range touches. Either way a satisfied range answers `206` and an
/// unsatisfiable one `416`.
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

    let total_size = manifest.total_size.to_bytes();
    let range = resolve_range(range.as_deref(), total_size)?;

    // One plan drives everything downstream: what a ranged read is charged,
    // which chunk tracks get resolved, and what the stream decodes. A full
    // read is simply the whole-object window.
    let window = range.unwrap_or(ByteRange {
        start: 0,
        end: total_size,
    });
    let plan = chunk_range_plan(&manifest, window);

    // A ranged read charges the bytes the gateway decodes for it (the chunks
    // the range touches), matching the single-track routes, which charge the
    // whole decoded track.
    let metered = plan.iter().map(|chunk| chunk.decoded_size).sum();
    match state.meter.check_object_bytes(caller, metered) {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited(retry_after));
        }
    }

    object_stream_response(
        state,
        track.tape,
        &manifest,
        &plan,
        metadata,
        decoded.etag,
        total_size,
        range,
    )
}

pub(crate) async fn get_track_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path(track_id): Path<String>,
    headers: HeaderMap,
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

    object_response_ranged(
        decoded.bytes,
        &metadata,
        decoded.etag,
        range_header(&headers).as_deref(),
    )
}
