//! S3 request handlers and the Axum router for the S3 listener
//!
//! Hosts the per-route handlers (ListBuckets, ListObjectsV2, GetObject,
//! HeadObject, PutObject, multipart upload, DeleteObject).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{ConnectInfo, Extension, Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::{decode, encode};
use tokio::join;
use tokio::task::JoinError;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::instruction::MAX_NAME_LEN;
use tape_api::program::tapedrive::tape_pda;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ContentType, StorageUnits};
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_sdk::error::TapedriveError;
use tape_store::ops::{CredentialOps, ObjectListOps, TapeOps};
use tape_store::types::CredentialScope;

use crate::http::handlers::object::{ObjectResponseMetadata, range_header, read_object_response};
use crate::http::handlers::track::track_with_pending;
use crate::http::state::AppState;
use crate::meter::{GatewayMeterDecision, MeterCaller};
use super::accounting;
use super::authz::{Auth, WriteOp, WritePermit, authorize_multipart_read, authorize_write};
use super::chunked::object_reader;
use super::clock::now_unix;
use super::error::S3Error;
use super::multipart::{self, CompletedPartRef};
use super::resolve::{ResolvedObject, parse_bucket, resolve_object};
use super::response::{
    delete_response, head_response, put_response, set_last_modified, upload_part_response,
};
use super::sigv4::{query_param, sigv4_auth, verify_signed_body, SigV4Verifier, SignedPayloadHash};
use super::write::S3WriteContext;
use super::xml::{
    BucketEntry, ListObjectsV1, ListObjectsV2, ObjectEntry, Owner, PartEntry,
    STORAGE_CLASS_STANDARD, UploadEntry, complete_multipart_upload_body,
    initiate_multipart_upload_body, list_all_my_buckets_body, list_multipart_uploads_body,
    list_objects_v1_body, list_objects_v2_body, list_parts_body, parse_complete_multipart_upload,
};

/// Build the S3-compatible Axum router over the shared AppState
///
/// Routes:
/// - `GET /` -> ListBuckets
/// - `GET /{bucket}` -> ListObjectsV2 (`?list-type=2`) or an S3 error
/// - `HEAD /{bucket}` -> HeadBucket
/// - `GET|HEAD /{bucket}/{key}` -> GetObject / HeadObject
/// - `PUT /{bucket}/{key}` -> PutObject (or UploadPart with `?uploadId=`)
/// - `POST /{bucket}/{key}` -> CreateMultipartUpload (`?uploads`) /
///   CompleteMultipartUpload (`?uploadId=`)
/// - `DELETE /{bucket}/{key}` -> DeleteObject (or AbortMultipartUpload with
///   `?uploadId=`)
///
/// The `verifier` SigV4 layer gates every route (anonymous GET/HEAD/LIST are
/// allowed; signed requests are verified; unsigned writes are rejected).
pub fn router<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    verifier: Arc<SigV4Verifier>,
) -> Router
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    Router::new()
        .route("/", get(list_buckets::<Db, Cluster, Blockchain>))
        .route(
            "/{bucket}",
            get(bucket_get::<Db, Cluster, Blockchain>)
                .head(head_bucket::<Db, Cluster, Blockchain>),
        )
        .route(
            "/{bucket}/{*key}",
            get(get_object::<Db, Cluster, Blockchain>)
                .head(head_object::<Db, Cluster, Blockchain>)
                .put(put_object::<Db, Cluster, Blockchain>)
                .post(post_object::<Db, Cluster, Blockchain>)
                .delete(delete_object::<Db, Cluster, Blockchain>),
        )
        .with_state(state)
        .layer(from_fn_with_state(verifier, sigv4_auth))
}

/// Build the `NotImplemented` error for an operation that is scaffolded but not
/// yet wired up.
fn not_implemented(operation: &str) -> S3Error {
    S3Error::NotImplemented(format!("S3 {operation} is not implemented yet"))
}

/// Build the `NotImplemented` error for a *write* operation, distinguishing
/// whether delegate signing is even possible.
fn write_not_implemented(writes_enabled: bool, operation: &str) -> S3Error {
    if writes_enabled {
        S3Error::NotImplemented(format!("S3 {operation} is not implemented yet"))
    } else {
        S3Error::NotImplemented(format!(
            "S3 {operation} is unavailable: no delegate key configured (gateway.s3.delegate_key)"
        ))
    }
}

/// Returns whether the raw query string contains `key`, optionally requiring it
/// to equal `value`.
fn has_query_param(query: Option<&str>, key: &str, value: Option<&str>) -> bool {
    let Some(query) = query else {
        return false;
    };
    query.split('&').any(|pair| {
        let mut parts = pair.splitn(2, '=');
        let found_key = parts.next().unwrap_or("");
        let found_value = parts.next();
        found_key == key
            && match value {
                Some(expected) => found_value == Some(expected),
                None => true,
            }
    })
}

/// `GET /` -> ListBuckets
///
/// Authenticated and scoped to the caller: a `CredentialScope::Buckets(...)`
/// credential lists exactly its allow-listed bucket addresses.
async fn list_buckets<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let access_key_id = match auth {
        Auth::Verified(principal) => principal.access_key_id,
        Auth::Anonymous => {
            return Err(S3Error::AccessDenied(
                "ListBuckets requires authentication".to_string(),
            ));
        }
    };

    // The caller's credential (its owner authority + scope) drives the listing. 
    let resolved = match state.context.store.get_credential(&access_key_id) {
        Ok(Some(credential)) if credential.is_usable(now_unix()) => {
            Some((credential.principal, credential.scope))
        }
        Ok(Some(_)) => {
            return Err(S3Error::AccessDenied("credential is not usable".to_string()));
        }
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(%error, "s3 ListBuckets: credential store unavailable");
            return Err(S3Error::Internal("credential store unavailable".to_string()));
        }
    };

    let owner = Owner {
        id: access_key_id.clone(),
        display_name: access_key_id,
    };

    let buckets: Vec<BucketEntry> = match resolved {
        Some((_, CredentialScope::Buckets(addresses))) => addresses
            .into_iter()
            .map(|address| BucketEntry {
                name: address.to_string(),
                creation_date: 0,
            })
            .collect(),
        Some((principal, CredentialScope::AnyOwned)) => {
            let (tape, _) = tape_pda(principal);
            let reserved = state
                .context
                .store
                .get_tape(tape)
                .map_err(|error| {
                    tracing::warn!(%error, "s3 ListBuckets: tape store unavailable");
                    S3Error::Internal("tape store unavailable".to_string())
                })?
                .is_some();
            if reserved {
                vec![BucketEntry {
                    name: tape.to_string(),
                    creation_date: 0,
                }]
            } else {
                Vec::new()
            }
        }
        None => Vec::new(),
    };

    Ok(xml_ok_response(list_all_my_buckets_body(&owner, &buckets)))
}

/// `GET /{bucket}` -> ListObjectsV2 (`?list-type=2`), ListMultipartUploads
/// (`?uploads`), a recognized subresource (`NotImplemented`), or ListObjects V1
async fn bucket_get<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
    Path(bucket): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let query = query.as_deref();
    if has_query_param(query, "list-type", Some("2")) {
        list_objects_v2(&state, bucket, query)
    } else if has_query_param(query, "uploads", None) {
        list_multipart_uploads(&state, &auth, bucket)
    } else if BUCKET_SUBRESOURCES
        .iter()
        .any(|subresource| has_query_param(query, subresource, None))
    {
        // ?acl, ?versioning, ?tagging, … are real subresources, not listings.
        Err(not_implemented("bucket subresource"))
    } else {
        // Plain `GET /{bucket}` (prefix/marker/delimiter/max-keys) -> ListObjects V1.
        list_objects_v1(&state, bucket, query)
    }
}

/// Bucket subresource query flags that are recognized but unimplemented; a
/// `GET /{bucket}?<flag>` for any of these is `NotImplemented` rather than a
/// (legacy V1) object listing.
const BUCKET_SUBRESOURCES: &[&str] = &[
    "acl",
    "location",
    "versioning",
    "versions",
    "tagging",
    "cors",
    "policy",
    "lifecycle",
    "logging",
    "notification",
    "replication",
    "encryption",
    "website",
    "accelerate",
    "requestPayment",
    "analytics",
    "inventory",
    "metrics",
    "object-lock",
    "publicAccessBlock",
    "ownershipControls",
    "intelligent-tiering",
];

/// `GET /{bucket}?uploads` -> ListMultipartUploads
///
/// Lists the bucket's in-flight multipart uploads (key, upload id, initiation
/// time) in a single untruncated page.
fn list_multipart_uploads<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket_label: String,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    require_write_ctx(state, "ListMultipartUploads")?;
    let bucket = parse_bucket(&bucket_label)?;
    authorize_multipart_read(state, auth, bucket)?;
    let store = state.context.store.as_ref();

    let mut uploads: Vec<UploadEntry> = Vec::new();
    for upload in multipart::list_uploads(store, bucket)? {
        uploads.push(UploadEntry {
            key: upload.key,
            upload_id: upload.upload_id,
            initiated: upload.initiated,
        });
    }

    Ok(xml_ok_response(list_multipart_uploads_body(&bucket_label, &uploads)))
}

/// S3 caps `max-keys` at 1000; requests above this are clamped
const MAX_KEYS_LIMIT: u32 = 1000;

/// Handle `GET /{bucket}?list-type=2` (ListObjectsV2)
///
/// Maps the S3 query (`prefix`, `delimiter`, `max-keys`, `start-after`, and the
/// opaque base64 `continuation-token`) onto the store's `list_objects` op and
/// renders the `ListBucketResult` XML, including `IsTruncated` and a
/// `NextContinuationToken` (base64 of the store's raw-name cursor) when the
/// listing is truncated.
fn list_objects_v2<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    bucket_label: String,
    query: Option<&str>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let bucket = parse_bucket(&bucket_label)?;

    let prefix = query_value(query, "prefix").unwrap_or_default();
    let delimiter = query_value(query, "delimiter").filter(|delimiter| !delimiter.is_empty());
    let start_after = query_value(query, "start-after").filter(|start| !start.is_empty());
    let continuation_token =
        query_value(query, "continuation-token").filter(|token| !token.is_empty());
    let max_keys = query_value(query, "max-keys")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(MAX_KEYS_LIMIT)
        .clamp(1, MAX_KEYS_LIMIT);

    // A continuation token (the opaque base64 of a prior page's raw-name cursor)
    // takes precedence over `start-after`, matching S3.
    let start: Option<Vec<u8>> = match &continuation_token {
        Some(token) => Some(decode_continuation_token(token)?),
        None => start_after.as_ref().map(|start| start.as_bytes().to_vec()),
    };

    let page = state
        .context
        .store
        .list_objects(
            bucket,
            prefix.as_bytes(),
            delimiter.as_deref().map(str::as_bytes),
            start.as_deref(),
            max_keys as usize,
        )
        .map_err(|error| S3Error::Internal(error.to_string()))?;

    let mut contents: Vec<ObjectEntry> = Vec::new();
    for (name, entry) in &page.objects {
        contents.push(ObjectEntry {
            key: String::from_utf8_lossy(name).into_owned(),
            last_modified: entry.block_time,
            etag: entry.etag.to_string(),
            size: entry.size.to_bytes(),
            storage_class: STORAGE_CLASS_STANDARD,
        });
    }
    let mut common_prefixes: Vec<String> = Vec::new();
    for common_prefix in &page.common_prefixes {
        common_prefixes.push(String::from_utf8_lossy(common_prefix).into_owned());
    }

    let key_count = (contents.len() + common_prefixes.len()) as u32;
    let next_continuation_token = page
        .next
        .as_deref()
        .map(encode_continuation_token)
        .filter(|_| page.is_truncated);

    let result = ListObjectsV2 {
        name: bucket_label,
        prefix,
        delimiter,
        max_keys,
        key_count,
        is_truncated: page.is_truncated,
        continuation_token,
        next_continuation_token,
        start_after,
        contents,
        common_prefixes,
    };

    Ok(xml_ok_response(list_objects_v2_body(&result)))
}

/// `GET /{bucket}` (no `list-type=2`) -> ListObjects **V1** (legacy)
///
/// The same object-list query as V2, but paginated by a raw `marker` cursor (the
/// key to resume after) and rendered in the V1 `<Marker>`/`<NextMarker>` shape.
fn list_objects_v1<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    bucket_label: String,
    query: Option<&str>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let bucket = parse_bucket(&bucket_label)?;

    let prefix = query_value(query, "prefix").unwrap_or_default();
    let delimiter = query_value(query, "delimiter").filter(|delimiter| !delimiter.is_empty());
    let marker = query_value(query, "marker").filter(|marker| !marker.is_empty());
    let max_keys = query_value(query, "max-keys")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(MAX_KEYS_LIMIT)
        .clamp(1, MAX_KEYS_LIMIT);

    // V1's `marker` is the raw key to resume after (no opaque token).
    let start: Option<Vec<u8>> = marker.as_ref().map(|marker| marker.as_bytes().to_vec());

    let page = state
        .context
        .store
        .list_objects(
            bucket,
            prefix.as_bytes(),
            delimiter.as_deref().map(str::as_bytes),
            start.as_deref(),
            max_keys as usize,
        )
        .map_err(|error| S3Error::Internal(error.to_string()))?;

    let mut contents: Vec<ObjectEntry> = Vec::new();
    for (name, entry) in &page.objects {
        contents.push(ObjectEntry {
            key: String::from_utf8_lossy(name).into_owned(),
            last_modified: entry.block_time,
            etag: entry.etag.to_string(),
            size: entry.size.to_bytes(),
            storage_class: STORAGE_CLASS_STANDARD,
        });
    }
    let mut common_prefixes: Vec<String> = Vec::new();
    for common_prefix in &page.common_prefixes {
        common_prefixes.push(String::from_utf8_lossy(common_prefix).into_owned());
    }

    // NextMarker (when truncated) is the resume cursor, reported as a plain key.
    let next_marker = page
        .next
        .as_deref()
        .filter(|_| page.is_truncated)
        .map(|next| String::from_utf8_lossy(next).into_owned());

    let result = ListObjectsV1 {
        name: bucket_label,
        prefix,
        marker: marker.unwrap_or_default(),
        next_marker,
        max_keys,
        delimiter,
        is_truncated: page.is_truncated,
        contents,
        common_prefixes,
    };

    Ok(xml_ok_response(list_objects_v1_body(&result)))
}

/// Look up a raw query parameter by exact name, returning its percent-decoded
/// value. Matches the first occurrence; a bare flag (`?key`) yields `""`.
fn query_value(query: Option<&str>, key: &str) -> Option<String> {
    query_param(query?, key)
}

/// Encode a store raw-name cursor as the opaque base64 `continuation-token` S3
/// clients echo back to resume a listing.
fn encode_continuation_token(cursor: &[u8]) -> String {
    encode(cursor)
}

/// Decode an S3 `continuation-token` back into the store raw-name cursor. A
/// token that is not valid base64 is rejected as a bad request.
fn decode_continuation_token(token: &str) -> Result<Vec<u8>, S3Error> {
    decode(token).map_err(|_| S3Error::InvalidRequest("invalid continuation-token".into()))
}

/// Build a `200 OK` S3 XML response from a rendered body
fn xml_ok_response(body: String) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}

/// `HEAD /{bucket}` -> HeadBucket
async fn head_bucket<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(bucket): Path<String>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    // A bucket is a tape; it exists iff its tape account resolves on-chain.
    // HeadBucket has no body, so any resolution failure is reported as 404.
    let tape = parse_bucket(&bucket)?;
    match state.context.rpc.get_tape_by_address(&tape).await {
        Ok(_) => Ok(StatusCode::OK.into_response()),
        Err(_) => Err(S3Error::NoSuchBucket),
    }
}

/// Resolve an S3 `(bucket, key)` to its listing entry and the backing,
/// certified track that the read path consumes.
///
/// Maps a bucket label that is not a tape address to S3Error::NoSuchBucket,
/// a key absent from the object-list index to S3Error::NoSuchKey, and a
/// listed key whose track is missing or not yet certified (so it cannot be
/// served) to S3Error::NoSuchKey as well — the object simply is not
/// retrievable. Shared by GET (which decodes the track) and HEAD (which only
/// reports the entry metadata) so both agree on what is readable.
fn resolve_readable<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    bucket_label: &str,
    key: &str,
) -> Result<(ResolvedObject, CompressedTrack), S3Error> {
    let bucket = parse_bucket(bucket_label)?;
    let resolved = resolve_object(state, bucket, key)?.ok_or(S3Error::NoSuchKey)?;
    let track = track_with_pending(state, resolved.track_address)
        .map_err(S3Error::from)?
        .ok_or(S3Error::NoSuchKey)?;
    if !track.is_certified() {
        return Err(S3Error::NoSuchKey);
    }
    Ok((resolved, track))
}

/// `GET /{bucket}/{key}` -> GetObject
///
/// Resolves the key through the object-list index, applies the shared meter
/// (request rate + byte budget, layered per caller IP and per access key;
/// `RateLimited` -> `SlowDown` XML), then hands the resolved track to the
/// existing decode/manifest read path. The response carries Content-Type,
/// Content-Length, a quoted ETag, Cache-Control, and `Last-Modified` (from the
/// index entry's `block_time`).
async fn get_object<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    // `GET /{bucket}/{key}?uploadId=..` is ListParts, not an object read.
    if has_query_param(query.as_deref(), "uploadId", None) {
        return list_parts(&state, &auth, bucket, key, query.as_deref());
    }
    let range = range_header(&headers).map(str::to_string);
    let caller = meter_caller(&state, &headers, remote, &auth);
    get_object_impl(state, caller, bucket, key, range).await
}

async fn get_object_impl<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    caller: MeterCaller,
    bucket: String,
    key: String,
    range: Option<String>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    check_request_rate(&state, &caller)?;

    let (resolved, track) = resolve_readable(&state, &bucket, &key)?;
    // The S3 content type comes from the object-list index; objects carry no
    // separate filename, so no Content-Disposition is set.
    let metadata = ObjectResponseMetadata {
        content_type: resolved.content_type,
        filename: None,
    };
    let block_time = resolved.block_time;

    // `Range` is honored for every object: single-track objects slice the
    // decoded bytes, multi-track streams decode only the chunks the range
    // touches; see docs/s3-gateway-status.md (Range).
    let mut response = read_object_response(
        state,
        resolved.track_address,
        track,
        metadata,
        &caller,
        range,
        |retry_after| S3Error::slow_down(retry_after).into_response(),
    )
    .await
    .map_err(S3Error::from)?;

    set_last_modified(response.headers_mut(), block_time);
    Ok(response)
}

/// `HEAD /{bucket}/{key}` -> HeadObject
///
/// Returns the same headers as GetObject (Content-Type, Content-Length, quoted
/// ETag, Cache-Control, Last-Modified) with no body, including the ranged
/// headers for a `Range` request. Metadata comes straight from the object-list
/// index entry, so HEAD never decodes the object body.
async fn head_object<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let caller = meter_caller(&state, &headers, remote, &auth);
    head_object_impl(&state, &caller, &bucket, &key, range_header(&headers))
}

fn head_object_impl<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    caller: &MeterCaller,
    bucket: &str,
    key: &str,
    range: Option<&str>,
) -> Result<Response, S3Error> {
    check_request_rate(state, caller)?;
    let (resolved, _track) = resolve_readable(state, bucket, key)?;
    head_response(&resolved, range)
}

/// The metering identity for an S3 read: the resolved caller IP, plus the
/// access key and its grade when the request was signed. Reading the grade
/// from the store on each request keeps reassignment live without a restart.
fn meter_caller<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    headers: &HeaderMap,
    remote: SocketAddr,
    auth: &Auth,
) -> MeterCaller {
    let access_key = auth.access_key().map(str::to_string);
    let grade = access_key.as_deref().and_then(|access_key| {
        CredentialOps::get_credential(state.context.store.as_ref(), access_key)
            .ok()
            .flatten()
            .and_then(|credential| credential.grade)
    });
    MeterCaller::resolve(
        remote.ip(),
        headers,
        &state.context.config.gateway.metering.trusted_proxies,
        access_key,
        grade,
    )
}

/// Apply the shared request-rate meter (per caller IP, then per access key),
/// mapping a `RateLimited` decision to an S3 `SlowDown` error (the meter's
/// `429` rendered as S3 XML).
fn check_request_rate<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    caller: &MeterCaller,
) -> Result<(), S3Error> {
    match state.meter.check_object_request(caller) {
        GatewayMeterDecision::Allowed => Ok(()),
        GatewayMeterDecision::RateLimited { retry_after } => Err(S3Error::slow_down(retry_after)),
    }
}

/// `PUT /{bucket}/{key}` -> PutObject, or UploadPart when `?uploadId=` is set
///
/// PutObject is signed by the configured delegate keypair. A signed-hash request is
/// buffered and integrity-checked, then written as one track or a multi-track
/// stream depending on size; an `UNSIGNED-PAYLOAD` / `aws-chunked` request is
/// streamed straight onto chunk tracks with bounded memory. UploadPart buffers
/// the part bytes under the upload id.
async fn put_object<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
    Extension(signed_payload): Extension<SignedPayloadHash>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if has_query_param(query.as_deref(), "uploadId", None) {
        // UploadPart buffers the part bytes under the upload id; the assembled
        // object is written at CompleteMultipartUpload.
        let max_buffered_bytes = state.context.config.gateway.s3.max_buffered_bytes;
        let part = buffer_object_body(body, max_buffered_bytes).await?;
        verify_signed_body(&signed_payload, &part)?;
        return upload_part(&state, &auth, bucket, key, query.as_deref(), part).await;
    }

    put_object_impl(state, &auth, &signed_payload, bucket, key, &headers, body).await
}

async fn put_object_impl<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    signed_payload: &SignedPayloadHash,
    bucket: String,
    key: String,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    // Writes require a configured delegate keypair; without one the gateway holds
    // no key any tape can authorize.
    let Some(write_ctx) = state.write_ctx.clone() else {
        return Err(write_not_implemented(false, "PutObject"));
    };

    let tape = parse_bucket(&bucket)?;

    // The object key is the on-chain track name. Enforce the program's name bounds
    // (1..=MAX_NAME_LEN bytes) up front for a precise client error.
    validate_object_key(&key)?;

    let content_type = content_type_from_headers(headers);
    let max_object_bytes = state.context.config.gateway.s3.max_object_bytes;
    let max_buffered_bytes = state.context.config.gateway.s3.max_buffered_bytes;

    // Streamed (bounded-memory) when a sentinel payload declares a size, else
    // buffered so the body can be hash-verified. Either way the write chokepoint
    // reserves before the write and commits/refunds after.
    let written_etag = match streamed_object_size(signed_payload, headers)? {
        Some(size) => {
            if size > max_object_bytes as u64 {
                return Err(S3Error::EntityTooLarge(format!(
                    "object size {size} exceeds the maximum of {max_object_bytes} bytes"
                )));
            }
            let permit = authorize_write(&state, auth, tape, &key, WriteOp::Put, size).await?;
            let (reader, producer) = object_reader(body, signed_payload.is_aws_chunked());
            let (write_result, producer_result) = join!(
                write_ctx.write_object_stream(
                    state.context.as_ref(),
                    tape,
                    key.as_bytes(),
                    content_type,
                    StorageUnits::from_bytes(size),
                    reader,
                ),
                producer,
            );
            settle_streamed(permit, &state, size, write_result, producer_result)?
        }
        None => {
            let data = buffer_object_body(body, max_buffered_bytes).await?;
            verify_signed_body(signed_payload, &data)?;
            let size = data.len() as u64;
            let permit = authorize_write(&state, auth, tape, &key, WriteOp::Put, size).await?;
            let result = write_ctx
                .write_object(state.context.as_ref(), tape, key.as_bytes(), content_type, &data)
                .await;
            settle_write(permit, &state, size, result)?
        }
    };

    // Prefer the canonical object-list ETag (matches GET/HEAD exactly); fall back
    // to the write's content hash until the local index catches up. Making PutObject
    // and GET agree without that dependency folds into the ingestor-computed ETag
    // plan — see docs/s3-gateway-status.md (ETag).
    let etag = resolve_object(&state, tape, &key)?
        .map(|resolved| resolved.etag)
        .unwrap_or(written_etag);

    put_response(etag)
}

/// Header carrying the decoded object size for an `aws-chunked` streaming upload.
const AMZ_DECODED_CONTENT_LENGTH: &str = "x-amz-decoded-content-length";

/// The declared object size for the streamed (bounded-memory) write path, or
/// `None` to buffer and hash-verify
///
/// `aws-chunked` uploads must stream (the framing has to be stripped) and carry
/// the size in `x-amz-decoded-content-length`; plain `UNSIGNED-PAYLOAD` uploads
/// stream when `Content-Length` is present and non-empty. A signed-hash request
/// always buffers so the body can be verified, so it returns `None`.
fn streamed_object_size(
    signed_payload: &SignedPayloadHash,
    headers: &HeaderMap,
) -> Result<Option<u64>, S3Error> {
    if signed_payload.is_aws_chunked() {
        let size = parse_byte_count(headers, AMZ_DECODED_CONTENT_LENGTH).ok_or_else(|| {
            S3Error::InvalidRequest(
                "aws-chunked upload is missing x-amz-decoded-content-length".into(),
            )
        })?;
        return Ok(Some(size));
    }
    if signed_payload.is_verifiable() {
        return Ok(None);
    }
    Ok(parse_byte_count(headers, header::CONTENT_LENGTH.as_str()).filter(|&size| size > 0))
}

/// Parse a `u64` byte count from a request header.
fn parse_byte_count(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

/// Buffer a request body into memory, bounded by `max_bytes`.
async fn buffer_object_body(body: Body, max_bytes: usize) -> Result<Bytes, S3Error> {
    to_bytes(body, max_bytes).await.map_err(|_| {
        S3Error::EntityTooLarge(format!("object body exceeds the maximum of {max_bytes} bytes"))
    })
}

/// Commit the permit on a successful buffered write; refund and map the error
/// otherwise.
fn settle<T, E, Db, Cluster, Blockchain>(
    permit: WritePermit,
    state: &AppState<Db, Cluster, Blockchain>,
    size: u64,
    result: Result<T, E>,
) -> Result<T, E>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match &result {
        Ok(_) => permit.commit(state, size),
        Err(_) => permit.refund(state),
    }
    result
}

fn settle_write<Db, Cluster, Blockchain>(
    permit: WritePermit,
    state: &AppState<Db, Cluster, Blockchain>,
    size: u64,
    result: Result<Hash, TapedriveError>,
) -> Result<Hash, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match result {
        Ok(etag) => {
            permit.commit(state, size);
            Ok(etag)
        }
        Err(error) => {
            permit.refund(state);
            Err(s3_write_error(error))
        }
    }
}

/// Reconcile a streamed write against both the write pipeline and the body
/// producer (de-framer / copier) task.
fn settle_streamed<Db, Cluster, Blockchain>(
    permit: WritePermit,
    state: &AppState<Db, Cluster, Blockchain>,
    size: u64,
    write_result: Result<Hash, TapedriveError>,
    producer_result: Result<io::Result<()>, JoinError>,
) -> Result<Hash, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    match write_result {
        Ok(etag) => {
            permit.commit(state, size);
            Ok(etag)
        }
        // A failed body producer is the root cause; otherwise fall back to the
        // pipeline error (computed lazily so its warn! only fires when used).
        Err(error) => {
            permit.refund(state);
            Err(body_producer_error(producer_result).unwrap_or_else(|| s3_write_error(error)))
        }
    }
}

/// Map a failed body-producer task (de-framer / copier) to its client-facing
/// error, or `None` when the producer finished cleanly.
fn body_producer_error(producer_result: Result<io::Result<()>, JoinError>) -> Option<S3Error> {
    match producer_result {
        Ok(Ok(())) => None,
        Ok(Err(body_error)) => {
            tracing::warn!(%body_error, "s3 streamed PutObject: request body error");
            Some(S3Error::InvalidRequest(
                "request body was incomplete or malformed".into(),
            ))
        }
        Err(join_error) => {
            tracing::error!(%join_error, "s3 streamed PutObject: body task failed");
            Some(S3Error::Internal("request body task failed".into()))
        }
    }
}

fn s3_write_error(error: TapedriveError) -> S3Error {
    if is_operator_auth_failure(&error) {
        tracing::warn!(%error, "s3 write denied: tape has not delegated to this gateway");
        return S3Error::AccessDenied(
            "the bucket tape has not delegated writes to this gateway".to_string(),
        );
    }
    match error {
        TapedriveError::InvalidArgument(message) => S3Error::InvalidRequest(message),
        TapedriveError::NotFound => S3Error::NoSuchKey,
        other @ (TapedriveError::MissingPayer
        | TapedriveError::Rpc(_)
        | TapedriveError::Upload(_)
        | TapedriveError::Download(_)
        | TapedriveError::Certification(_)
        | TapedriveError::Network(_)
        | TapedriveError::Peer(_)
        | TapedriveError::Encoding(_)
        | TapedriveError::CommitmentMismatch
        | TapedriveError::InsufficientCapacity { .. }
        | TapedriveError::Io(_)
        | TapedriveError::Stream(_)) => S3Error::Internal(other.to_string()),
    }
}

/// Whether a write-pipeline error is an on-chain operator-authorization
/// rejection.
fn is_operator_auth_failure(error: &TapedriveError) -> bool {
    match error {
        TapedriveError::Rpc(RpcError::Transaction { message, .. }) => {
            message.to_ascii_lowercase().contains("invalid account data")
        }
        TapedriveError::Rpc(_)
        | TapedriveError::MissingPayer
        | TapedriveError::Upload(_)
        | TapedriveError::Download(_)
        | TapedriveError::Certification(_)
        | TapedriveError::Network(_)
        | TapedriveError::Peer(_)
        | TapedriveError::Encoding(_)
        | TapedriveError::CommitmentMismatch
        | TapedriveError::NotFound
        | TapedriveError::InsufficientCapacity { .. }
        | TapedriveError::InvalidArgument(_)
        | TapedriveError::Io(_)
        | TapedriveError::Stream(_) => false,
    }
}

/// `POST /{bucket}/{key}` -> CreateMultipartUpload (`?uploads`) or
/// CompleteMultipartUpload (`?uploadId=`).
async fn post_object<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
    Extension(signed_payload): Extension<SignedPayloadHash>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    // The part-list XML (CompleteMultipartUpload) is itself signed; verify it
    // hashes to the signed x-amz-content-sha256 before parsing.
    verify_signed_body(&signed_payload, &body)?;

    if has_query_param(query.as_deref(), "uploads", None) {
        // CreateMultipartUpload mints an upload id; the request carries no body.
        return create_multipart_upload(&state, &auth, bucket, key, &headers).await;
    }
    if has_query_param(query.as_deref(), "uploadId", None) {
        // CompleteMultipartUpload assembles the buffered parts (XML body lists
        // them) and drives the write pipeline.
        return complete_multipart_upload(&state, &auth, bucket, key, query.as_deref(), body).await;
    }
    Err(not_implemented("object POST"))
}

/// `DELETE /{bucket}/{key}` -> DeleteObject, or AbortMultipartUpload when
/// `?uploadId=` is set.
async fn delete_object<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(auth): Extension<Auth>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(query): RawQuery,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if has_query_param(query.as_deref(), "uploadId", None) {
        // `DELETE /{bucket}/{key}?uploadId=..` is AbortMultipartUpload: discard
        // the buffered parts for the upload id.
        return abort_multipart_upload(&state, &auth, bucket, key, query.as_deref()).await;
    }
    delete_object_impl(&state, &auth, bucket, key).await
}

async fn delete_object_impl<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket: String,
    key: String,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    // Writes require a configured delegate keypair; without one the gateway
    // holds no key any tape can authorize.
    let Some(write_ctx) = state.write_ctx.as_ref() else {
        return Err(write_not_implemented(false, "DeleteObject"));
    };

    let tape = parse_bucket(&bucket)?;

    // Authorization chokepoint runs before the existence check so an
    // unauthorized caller cannot probe which keys exist via the response code.
    let permit = authorize_write(state, auth, tape, &key, WriteOp::Delete, 0).await?;

    // S3 DeleteObject is idempotent: a key absent from the object-list index is
    // already "deleted", so report success without touching the chain. Nothing
    // was spent, so release the reservation.
    let Some(resolved) = resolve_object(state, tape, &key)? else {
        permit.refund(state);
        return Ok(delete_response());
    };

    match write_ctx
        .delete_object(state.context.as_ref(), tape, resolved.track_address)
        .await
    {
        Ok(()) => {
            permit.commit(state, 0);
            Ok(delete_response())
        }
        // A track that raced to deletion (no longer resolvable on-chain) is
        // treated as an idempotent success, matching S3; nothing was spent, so
        // refund the reservation.
        Err(TapedriveError::NotFound) => {
            permit.refund(state);
            Ok(delete_response())
        }
        Err(error) => {
            permit.refund(state);
            Err(s3_write_error(error))
        }
    }
}

/// S3 caps `max-parts` (and a single ListParts page) at 1000
const MAX_PARTS_LIMIT: u32 = 1000;

/// `SlowDown` retry hint when a principal is at its concurrent-multipart budget.
const MULTIPART_BUDGET_RETRY_AFTER_SECS: u64 = 1;

/// Validate an S3 object key as an on-chain track name: 1..=`MAX_NAME_LEN`
/// bytes. Shared by PutObject and CreateMultipartUpload (both land a named
/// track), so they reject malformed keys identically.
fn validate_object_key(key: &str) -> Result<(), S3Error> {
    let length = key.len();
    if length == 0 || length > MAX_NAME_LEN {
        return Err(S3Error::InvalidRequest(format!(
            "object key must be between 1 and {MAX_NAME_LEN} bytes"
        )));
    }
    // Control characters are not representable in listing XML without
    // `encoding-type=url` (unsupported), and a single such key would make a
    // whole bucket listing unparseable, so reject them at write time.
    if key.bytes().any(|byte| byte < 0x20 || byte == 0x7F) {
        return Err(S3Error::InvalidRequest(
            "object key must not contain control characters".to_string(),
        ));
    }
    Ok(())
}

fn content_type_from_headers(headers: &HeaderMap) -> ContentType {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ContentType::from_str)
        .unwrap_or(ContentType::Unknown)
}

/// Borrow the delegate write context.
fn require_write_ctx<'state, Db, Cluster, Blockchain>(
    state: &'state AppState<Db, Cluster, Blockchain>,
    operation: &str,
) -> Result<&'state Arc<S3WriteContext>, S3Error>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    state
        .write_ctx
        .as_ref()
        .ok_or_else(|| write_not_implemented(false, operation))
}

/// Extract a non-empty `uploadId` query parameter.
fn require_upload_id(query: Option<&str>) -> Result<String, S3Error> {
    query_value(query, "uploadId")
        .filter(|upload_id| !upload_id.is_empty())
        .ok_or_else(|| S3Error::InvalidRequest("missing uploadId".into()))
}

/// `POST /{bucket}/{key}?uploads` -> CreateMultipartUpload
///
/// Mints an opaque upload id bound to `(bucket, key)` and records the object's
/// content type. No tracks are written until CompleteMultipartUpload.
async fn create_multipart_upload<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket_label: String,
    key: String,
    headers: &HeaderMap,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    require_write_ctx(state, "CreateMultipartUpload")?;
    let bucket = parse_bucket(&bucket_label)?;
    validate_object_key(&key)?;

    let content_type = content_type_from_headers(headers);
    let store = state.context.store.as_ref();

    // Authorization chokepoint.
    let permit = authorize_write(state, auth, bucket, &key, WriteOp::CreateMultipart, 0).await?;
    let principal = permit.owner();

    // Enforce the per-principal concurrent-upload budget.
    let limit = match accounting::concurrent_multipart_limit(state, &principal) {
        Ok(limit) => limit,
        Err(reason) => {
            permit.refund(state);
            return Err(S3Error::Internal(reason));
        }
    };
    let admitted = accounting::with_ledger_lock(state.accounting.as_ref(), || -> Result<Option<String>, S3Error> {
        if multipart::count_open_uploads(store, principal)? as u64 >= limit as u64 {
            return Ok(None);
        }
        multipart::create_upload(store, bucket, key.clone(), content_type, principal).map(Some)
    });
    let upload_id = match admitted {
        Ok(Some(upload_id)) => {
            permit.commit(state, 0);
            upload_id
        }
        Ok(None) => {
            permit.refund(state);
            return Err(S3Error::SlowDown {
                retry_after_seconds: MULTIPART_BUDGET_RETRY_AFTER_SECS,
            });
        }
        Err(error) => {
            permit.refund(state);
            return Err(error);
        }
    };

    Ok(xml_ok_response(initiate_multipart_upload_body(
        &bucket_label,
        &key,
        &upload_id,
    )))
}

/// `PUT /{bucket}/{key}?uploadId=..&partNumber=..` -> UploadPart
///
/// Buffers the part bytes under the upload id keyed by part number and returns
/// the part's ETag (hex of a placeholder content hash) for the client to echo
/// back at CompleteMultipartUpload.
async fn upload_part<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket_label: String,
    key: String,
    query: Option<&str>,
    body: Bytes,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    require_write_ctx(state, "UploadPart")?;
    let bucket = parse_bucket(&bucket_label)?;
    let upload_id = require_upload_id(query)?;
    let part_number = query_value(query, "partNumber")
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| S3Error::InvalidRequest("missing or invalid partNumber".into()))?;
    let store = state.context.store.as_ref();

    // Authorization chokepoint: gate persisting a part. The part is held in the
    // store until CompleteMultipartUpload (no on-chain cost here), so nothing is
    // reserved — the object cost is reserved at CompleteMultipartUpload. The
    // object ceiling bounds the bytes a single upload may stage.
    let size = body.len() as u64;
    let max_object_bytes = state.context.config.gateway.s3.max_object_bytes;
    let permit = authorize_write(state, auth, bucket, &key, WriteOp::UploadPart, size).await?;
    let etag = settle(
        permit,
        state,
        size,
        multipart::put_part(store, &upload_id, bucket, &key, part_number, body.to_vec(), max_object_bytes),
    )?;
    upload_part_response(etag)
}

/// `GET /{bucket}/{key}?uploadId=..` -> ListParts
///
/// Reports the upload's parts (number, size, ETag, last-modified) in ascending
/// part-number order, honoring `part-number-marker` and `max-parts` pagination.
fn list_parts<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket_label: String,
    key: String,
    query: Option<&str>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    require_write_ctx(state, "ListParts")?;
    let bucket = parse_bucket(&bucket_label)?;
    authorize_multipart_read(state, auth, bucket)?;
    let upload_id = require_upload_id(query)?;
    let store = state.context.store.as_ref();

    let part_number_marker = query_value(query, "part-number-marker")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let max_parts = query_value(query, "max-parts")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(MAX_PARTS_LIMIT)
        .clamp(1, MAX_PARTS_LIMIT);

    let listing = multipart::list_parts(store, &upload_id, bucket, &key)?;

    // Return parts strictly after the marker, up to `max_parts`; truncate (and
    // report the resume marker) when more remain.
    let mut parts: Vec<PartEntry> = Vec::new();
    let mut next_part_number_marker = 0;
    let mut is_truncated = false;
    for part in &listing.parts {
        if part.part_number <= part_number_marker {
            continue;
        }
        if parts.len() as u32 >= max_parts {
            is_truncated = true;
            break;
        }
        parts.push(PartEntry {
            part_number: part.part_number,
            last_modified: part.last_modified,
            etag: part.etag.clone(),
            size: part.size,
        });
        next_part_number_marker = part.part_number;
    }
    if !is_truncated {
        next_part_number_marker = 0;
    }

    Ok(xml_ok_response(list_parts_body(
        &bucket_label,
        &listing.key,
        &upload_id,
        part_number_marker,
        next_part_number_marker,
        max_parts,
        is_truncated,
        &parts,
    )))
}

/// `DELETE /{bucket}/{key}?uploadId=..` -> AbortMultipartUpload
///
/// Discards the buffered parts for the upload id and returns `204 No Content`.
/// Aborting another tenant's in-flight upload deletes durable state, so it
/// passes the write chokepoint and `multipart::abort` additionally requires the
/// caller to own the upload.
async fn abort_multipart_upload<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket_label: String,
    key: String,
    query: Option<&str>,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    require_write_ctx(state, "AbortMultipartUpload")?;
    let bucket = parse_bucket(&bucket_label)?;
    let upload_id = require_upload_id(query)?;
    let store = state.context.store.as_ref();

    let permit = authorize_write(state, auth, bucket, &key, WriteOp::Abort, 0).await?;
    match multipart::abort(store, &upload_id, bucket, &key, permit.owner()) {
        Ok(()) => {
            permit.commit(state, 0);
            Ok(delete_response())
        }
        Err(error) => {
            permit.refund(state);
            Err(error)
        }
    }
}

/// `POST /{bucket}/{key}?uploadId=..` -> CompleteMultipartUpload
///
/// Parses the part list from the request body, validates it against the
/// buffered parts, concatenates them in part-number order, and writes the
/// assembled object. An object that fits in a single 64 MiB chunk track is
/// materialized as one delegate-signed named track.
async fn complete_multipart_upload<Db, Cluster, Blockchain>(
    state: &AppState<Db, Cluster, Blockchain>,
    auth: &Auth,
    bucket_label: String,
    key: String,
    query: Option<&str>,
    body: Bytes,
) -> Result<Response, S3Error>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let write_ctx = require_write_ctx(state, "CompleteMultipartUpload")?;
    let bucket = parse_bucket(&bucket_label)?;
    let upload_id = require_upload_id(query)?;
    let store = state.context.store.as_ref();

    let body_text = std::str::from_utf8(&body).map_err(|_| {
        S3Error::InvalidRequest("CompleteMultipartUpload body is not valid UTF-8".into())
    })?;
    let parsed = parse_complete_multipart_upload(body_text).map_err(S3Error::InvalidRequest)?;
    let mut requested: Vec<CompletedPartRef> = Vec::new();
    for (part_number, etag) in parsed {
        requested.push(CompletedPartRef { part_number, etag });
    }

    // Validate the client's part list and concatenate the persisted parts. The
    // upload is retained until the write below lands, so a failed write leaves
    // it intact for the client to retry or abort.
    let max_object_bytes = state.context.config.gateway.s3.max_object_bytes;
    let assembled = multipart::assemble(store, &upload_id, bucket, &key, &requested, max_object_bytes)?;

    // Authorization chokepoint.
    let size = assembled.data.len() as u64;
    let permit = authorize_write(state, auth, bucket, &key, WriteOp::CompleteMultipart, size).await?;
    let result = write_ctx
        .write_object(
            state.context.as_ref(),
            bucket,
            assembled.key.as_bytes(),
            assembled.content_type,
            &assembled.data,
        )
        .await;
    // On failure `?` returns before the upload is dropped, so it stays intact for
    // the client to retry or abort.
    let written_etag = settle_write(permit, state, size, result)?;

    // The object is durable; drop the persisted upload state. A delete failure
    // only leaks reclaimable upload state, so log it rather than fail the write.
    if let Err(error) = multipart::remove(store, &upload_id) {
        tracing::warn!(?error, "s3 CompleteMultipartUpload: failed to drop upload state");
    }

    // Mirror PutObject's ETag resolution.
    let etag = resolve_object(state, bucket, &assembled.key)?
        .map(|resolved| resolved.etag)
        .unwrap_or(written_etag);

    // Location is the configured public endpoint URL, else a path-style resource.
    let location = match &state.context.config.gateway.s3.public_endpoint {
        Some(endpoint) => format!("{}/{bucket_label}/{key}", endpoint.trim_end_matches('/')),
        None => format!("/{bucket_label}/{key}"),
    };
    Ok(xml_ok_response(complete_multipart_upload_body(
        &location,
        &bucket_label,
        &assembled.key,
        &etag.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    // query lookup returns the percent-decoded value
    #[test]
    fn query_extract() {
        let query = Some("list-type=2&prefix=photos%2Fa&max-keys=10");
        assert_eq!(query_value(query, "prefix").as_deref(), Some("photos/a"));
        assert_eq!(query_value(query, "max-keys").as_deref(), Some("10"));
        assert_eq!(query_value(query, "missing"), None);
        assert_eq!(query_value(None, "prefix"), None);
    }

    // a bare flag yields an empty-string value
    #[test]
    fn bare_flag() {
        assert_eq!(query_value(Some("uploads"), "uploads").as_deref(), Some(""));
    }

    // a cursor survives encode then decode
    #[test]
    fn round_trip() {
        let cursor = b"photos/sub/file.jpg".to_vec();
        let token = encode_continuation_token(&cursor);
        assert_eq!(decode_continuation_token(&token).expect("test setup"), cursor);
    }

    // a non-base64 token is rejected as a bad request
    #[test]
    fn token_invalid() {
        assert!(matches!(
            decode_continuation_token("not base64!!!"),
            Err(S3Error::InvalidRequest(_))
        ));
    }

    // param matching honors exact value and bare flags
    #[test]
    fn parameter_match() {
        assert!(has_query_param(Some("list-type=2"), "list-type", Some("2")));
        assert!(!has_query_param(Some("list-type=1"), "list-type", Some("2")));
        assert!(has_query_param(Some("uploads"), "uploads", None));
        assert!(has_query_param(Some("uploadId=abc"), "uploadId", None));
        assert!(!has_query_param(None, "uploads", None));
    }

    // write-pipeline errors map onto the S3 surface
    #[test]
    fn error_mapping() {
        // A bad name/oversized inline payload is a client error.
        assert!(matches!(
            s3_write_error(TapedriveError::InvalidArgument("bad name".into())),
            S3Error::InvalidRequest(_)
        ));
        // A missing track (e.g. already deleted) maps to NoSuchKey.
        assert!(matches!(
            s3_write_error(TapedriveError::NotFound),
            S3Error::NoSuchKey
        ));
        // An on-chain operator-authorization rejection (the tape has not
        // delegated writes to this gateway) surfaces as a transaction error whose
        // message carries the program's `InvalidAccountData`; it maps to
        // AccessDenied (403), not InternalError.
        assert!(matches!(
            s3_write_error(TapedriveError::Rpc(RpcError::Transaction {
                err: None,
                message: "Error processing Instruction 0: invalid account data for instruction"
                    .into(),
            })),
            S3Error::AccessDenied(_)
        ));
        // A genuine transaction failure that is *not* the operator-auth signal
        // stays an InternalError — only the recognized signal is re-mapped.
        assert!(matches!(
            s3_write_error(TapedriveError::Rpc(RpcError::Transaction {
                err: None,
                message: "blockhash expired".into(),
            })),
            S3Error::Internal(_)
        ));
        // Everything else collapses to InternalError.
        assert!(matches!(
            s3_write_error(TapedriveError::CommitmentMismatch),
            S3Error::Internal(_)
        ));
    }
}
