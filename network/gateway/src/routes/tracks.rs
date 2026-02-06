use axum::{
    body::Bytes,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// GET /v1/tracks/{address}
///
/// Full blob retrieval with optional Range header support.
pub async fn get_track(Path(_address): Path<String>) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

/// PUT /v1/tracks/{address}
///
/// Store a blob (gateway encodes + distributes to storage nodes).
pub async fn put_track(Path(_address): Path<String>, _body: Bytes) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

/// GET /v1/tracks/{address}/byte-range
///
/// Byte-range access into a stored blob.
pub async fn get_byte_range(Path(_address): Path<String>) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}
