use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::ObjectMetadataOps;

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

pub(in crate::http::handlers::object) fn object_content_type<
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<ContentType, RouteError> {
    let content_type = state
        .context
        .store
        .get_object_metadata(track_addr)
        .map_err(store_error)?
        .map(|metadata| metadata.content_type)
        .unwrap_or(ContentType::Unknown);
    Ok(content_type)
}

pub(in crate::http::handlers::object) fn object_response(
    bytes: Vec<u8>,
    content_type: ContentType,
    etag: Hash,
) -> Result<Response, RouteError> {
    let headers = object_headers(bytes.len() as u64, content_type, etag)?;
    Ok((StatusCode::OK, headers, bytes).into_response())
}

pub(in crate::http::handlers::object) fn object_headers(
    content_length: u64,
    content_type: ContentType,
    etag: Hash,
) -> Result<HeaderMap, RouteError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type.to_str()),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| RouteError::Internal(format!("content length header: {error}")))?,
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{etag}\""))
            .map_err(|error| RouteError::Internal(format!("etag header: {error}")))?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );

    Ok(headers)
}
