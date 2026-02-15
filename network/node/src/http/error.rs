//! API error type with `IntoResponse` conversion.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Errors returned by HTTP handlers.
pub enum ApiError {
    NotFound,
    BadRequest(String),
    NotResponsible,
    NotInCommittee,
    InvalidSignature,
    InternalError(String),
    PayloadTooLarge,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            Self::NotResponsible => {
                (StatusCode::FORBIDDEN, "not responsible").into_response()
            }
            Self::NotInCommittee => {
                (StatusCode::FORBIDDEN, "not in committee").into_response()
            }
            Self::InvalidSignature => {
                (StatusCode::UNAUTHORIZED, "invalid signature").into_response()
            }
            Self::InternalError(msg) => {
                tracing::error!("internal error: {msg}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
            Self::PayloadTooLarge => {
                (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response()
            }
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self::InternalError(e.to_string())
    }
}
