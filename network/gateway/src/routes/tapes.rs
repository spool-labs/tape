use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// GET /v1/tapes/{address}
///
/// Tape info (capacity, epoch range, track count).
pub async fn get_tape(Path(_address): Path<String>) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

/// GET /v1/tapes/{address}/by-track-number/{number}
///
/// Retrieve a blob by its track number within a tape.
pub async fn get_track_by_number(Path((_address, _number)): Path<(String, u64)>) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}

/// GET /v1/tapes/{address}/tracks
///
/// List tracks in a tape.
pub async fn list_tracks(Path(_address): Path<String>) -> Response {
    StatusCode::NOT_IMPLEMENTED.into_response()
}
