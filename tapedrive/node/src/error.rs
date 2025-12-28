//! API error types.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Errors that can occur during API operations.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("invalid slice index")]
    InvalidSliceIndex,

    #[error("not responsible for this spool")]
    NotResponsible,

    #[error("slice not found")]
    NotFound,

    #[error("track not found")]
    TrackNotFound,

    #[error("invalid request body")]
    InvalidBody,

    #[error("unauthorized")]
    Unauthorized,

    #[error("internal error: {0}")]
    Internal(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("storage error: {0}")]
    Storage(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::InvalidSliceIndex => StatusCode::BAD_REQUEST,
            ApiError::InvalidBody => StatusCode::BAD_REQUEST,
            ApiError::NotResponsible => StatusCode::MISDIRECTED_REQUEST, // 421
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::TrackNotFound => StatusCode::NOT_FOUND,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Serialization(_) => StatusCode::BAD_REQUEST,
            ApiError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, self.to_string()).into_response()
    }
}
