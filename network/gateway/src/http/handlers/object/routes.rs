use std::time::Duration;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::body::Bytes;
use axum::response::{IntoResponse, Response};
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::track::types::CompressedTrack;
use tape_core::object::object_etag;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::with_share_token;
use tape_sdk::stream::manifest::ChunkManifest;

use super::decode::{coded_blob, decode_track_bytes};
use super::range::{decode_track_range, head_range, is_plain_object, is_rangeable};
use super::manifest::{chunk_range_plan, object_stream_response};
use super::response::{
    ByteRange, CachePolicy, ObjectResponseMetadata, object_response_metadata, object_response_ranged,
    range_header, ranged_object_headers, resolve_range,
};
use crate::http::error::RouteError;
use crate::http::handlers::resolve::{Readable, resolve_readable_with_pending};
use crate::http::handlers::store_error;
use crate::http::handlers::share::authorize_signed_read;
use crate::http::handlers::track::{
    ensure_readable, parse_address, requested_track, track_with_pending,
};
use crate::http::state::AppState;
use crate::meter::{GatewayMeterDecision, MeterCaller, rate_limited_response};

pub const OBJECT_PATH: &str = "/object/{track_id}";
pub const TRACK_BYTES_PATH: &str = "/track/{track_id}";
pub const BUCKET_OBJECT_PATH: &str = "/bucket/{tape_id}/{*key}";

pub async fn get_object<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path(track_id): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    ensure_readable(&state, track_addr, track.tape)?;
    if !track.is_certified() {
        return Err(RouteError::BadRequest("track is not certified".into()));
    }

    let token = authorize_signed_read(&state, &headers, &method, uri.path(), &track)?;

    let metadata = object_response_metadata(&state, track_addr, track.track_number)?;
    with_share_token(
        token,
        read_object_response(
            state,
            track_addr,
            track,
            metadata,
            StatusCode::OK,
            &caller,
            range_header(&headers).map(str::to_string),
            rate_limited_response,
        ),
    )
    .await
}

/// Serve an object by bucket and key in one request
pub async fn get_bucket_object<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path((tape_id, key)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    if state.blacklists.is_unlisted(tape) {
        return Err(RouteError::Gone);
    }
    let readable = resolve_readable_with_pending(
        state.context.store.as_ref(),
        state.staging.as_ref(),
        state.context.pending.as_ref(),
        tape,
        key.as_bytes(),
    )
    .map_err(store_error)?
    .ok_or(RouteError::NotFound)?;

    let resolved = match readable {
        Readable::Queued(object) => {
            let bytes = state
                .staging
                .bytes(tape, key.as_bytes())
                .map_err(store_error)?
                .ok_or(RouteError::NotFound)?;
            let metadata = ObjectResponseMetadata {
                content_type: object.content_type,
                filename: None,
                cache: CachePolicy::Immutable,
            };
            return object_response_ranged(bytes, &metadata, object.etag, range_header(&headers), StatusCode::OK);
        }
        Readable::Track(resolved) => resolved,
    };

    let track_addr = resolved.track_address;
    let track = requested_track(&state, track_addr, resolved.track_number)?
        .ok_or(RouteError::NotFound)?;
    ensure_readable(&state, track_addr, track.tape)?;
    if !track.is_certified() {
        return Err(RouteError::NotFound);
    }

    let token = authorize_signed_read(&state, &headers, &method, uri.path(), &track)?;
    let metadata = object_response_metadata(&state, track_addr, track.track_number)?;
    with_share_token(
        token,
        read_object_response(
            state,
            track_addr,
            track,
            metadata,
            StatusCode::OK,
            &caller,
            range_header(&headers).map(str::to_string),
            rate_limited_response,
        ),
    )
    .await
}

/// Decode an already-resolved, certified track and build its read response,
/// auto-detecting the manifest/stream layout.
///
/// `range` is the raw `Range` header value (`bytes=...`). Single-track objects
/// slice the decoded bytes in memory; multi-track streams decode only the
/// chunks the range touches. Either way a satisfied range answers `206` and an
/// unsatisfiable one `416`. `status` is the serve status; a non-OK body, like
/// a site's 404 page, is served whole and never range-sliced.
pub async fn read_object_response<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    state: AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: CompressedTrack,
    metadata: ObjectResponseMetadata,
    status: StatusCode,
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

    if status == StatusCode::OK && range.is_some() && track.is_coded() {
        let blob = coded_blob(&state, track_addr, &track)?;
        if is_rangeable(&blob) {
            let head = decode_track_range(&state, track_addr, &track, blob, head_range(&blob)).await?;
            if is_plain_object(&head, blob.size.to_bytes()) {
                let total = blob.size.to_bytes();
                if let Some(range) = resolve_range(range.as_deref(), total)? {
                    let bytes = match range.end <= head.len() as u64 {
                        true => head.slice(range.start as usize..range.end as usize),
                        false => decode_track_range(&state, track_addr, &track, blob, range).await?,
                    };
                    let etag = object_etag(&track, Some(&blob));
                    return ranged_response(&state, bytes, range, total, &metadata, etag);
                }
            }
        }
    }

    let decoded = decode_track_bytes(&state, track_addr, track).await?;
    let Ok(manifest) = ChunkManifest::from_bytes(&decoded.bytes) else {
        state
            .context
            .metrics
            .add_downloaded(decoded.bytes.len() as u64);
        // Single-track object: bytes are in memory, so honor a Range slice here.
        return object_response_ranged(
            decoded.bytes,
            &metadata,
            decoded.etag,
            range.as_deref(),
            status,
        );
    };

    let total_size = manifest.total_size.to_bytes();
    let range = if status == StatusCode::OK {
        resolve_range(range.as_deref(), total_size)?
    } else {
        None
    };

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
        status,
    )
}

pub async fn get_track_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path(track_id): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;
    ensure_readable(&state, track_addr, track.tape)?;
    if !track.is_certified() {
        return Err(RouteError::BadRequest("track is not certified".into()));
    }

    let token = authorize_signed_read(&state, &headers, &method, uri.path(), &track)?;

    match state
        .meter
        .check_object_bytes(&caller, track.size.to_bytes())
    {
        GatewayMeterDecision::Allowed => {}
        GatewayMeterDecision::RateLimited { retry_after } => {
            return Ok(rate_limited_response(retry_after));
        }
    }

    let metadata = object_response_metadata(&state, track_addr, track.track_number)?;
    if let Some(response) = with_share_token(
        token,
        ranged_track_bytes(&state, track_addr, &track, &metadata, range_header(&headers)),
    )
    .await?
    {
        return Ok(response);
    }
    let decoded = with_share_token(token, decode_track_bytes(&state, track_addr, track)).await?;
    state
        .context
        .metrics
        .add_downloaded(decoded.bytes.len() as u64);

    object_response_ranged(
        decoded.bytes,
        &metadata,
        decoded.etag,
        range_header(&headers),
        StatusCode::OK,
    )
}

/// Serve a range of a coded track from the stripes it covers, when that saves work
async fn ranged_track_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track: &CompressedTrack,
    metadata: &ObjectResponseMetadata,
    range: Option<&str>,
) -> Result<Option<Response>, RouteError> {
    if range.is_none() || !track.is_coded() {
        return Ok(None);
    }
    let blob = coded_blob(state, track_addr, track)?;
    if !is_rangeable(&blob) {
        return Ok(None);
    }
    let total = blob.size.to_bytes();
    let Some(range) = resolve_range(range, total)? else {
        return Ok(None);
    };

    let bytes = decode_track_range(state, track_addr, track, blob, range).await?;
    let etag = object_etag(track, Some(&blob));
    ranged_response(state, bytes, range, total, metadata, etag).map(Some)
}

fn ranged_response<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    bytes: Bytes,
    range: ByteRange,
    total: u64,
    metadata: &ObjectResponseMetadata,
    etag: Hash,
) -> Result<Response, RouteError> {
    state.context.metrics.add_downloaded(bytes.len() as u64);
    let (status, headers) = ranged_object_headers(Some(range), total, metadata, etag, StatusCode::OK)?;
    Ok((status, headers, bytes).into_response())
}
