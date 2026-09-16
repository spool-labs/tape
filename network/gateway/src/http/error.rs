use std::fmt::{self, Display};

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::cache::GatewayCacheError;

#[derive(Debug)]
pub enum RouteError {
    NotFound,
    BadRequest(String),
    /// A syntactically valid Range that cannot be satisfied; carries the
    /// object size for the `Content-Range: bytes */{size}` answer.
    RangeNotSatisfiable(u64),
    /// Content this gateway no longer serves
    Gone,
    BadGateway(String),
    Internal(String),
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message).into_response(),
            Self::RangeNotSatisfiable(total) => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{total}"))],
            )
                .into_response(),
            Self::Gone => (StatusCode::GONE, "content removed").into_response(),
            Self::BadGateway(message) => {
                tracing::warn!("gateway upstream error: {message}");
                (StatusCode::BAD_GATEWAY, "bad gateway").into_response()
            }
            Self::Internal(message) => {
                tracing::error!("gateway internal error: {message}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}

impl Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::BadRequest(message) => write!(f, "bad request: {message}"),
            Self::RangeNotSatisfiable(total) => {
                write!(f, "range not satisfiable for object of {total} bytes")
            }
            Self::Gone => f.write_str("content removed"),
            Self::BadGateway(message) => write!(f, "bad gateway: {message}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for RouteError {}

impl From<GatewayCacheError> for RouteError {
    fn from(error: GatewayCacheError) -> Self {
        match error {
            GatewayCacheError::Removed => Self::Gone,
            GatewayCacheError::Store(_) | GatewayCacheError::Codec(_) | GatewayCacheError::State(_) => {
                Self::Internal(error.to_string())
            }
        }
    }
}
