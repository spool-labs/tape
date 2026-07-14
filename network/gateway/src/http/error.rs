use std::fmt::{self, Display};

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::cache::GatewayCacheError;

#[derive(Debug)]
pub(crate) enum RouteError {
    NotFound,
    BadRequest(String),
    BadGateway(String),
    Internal(String),
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message).into_response(),
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
            Self::BadGateway(message) => write!(f, "bad gateway: {message}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for RouteError {}

impl From<GatewayCacheError> for RouteError {
    fn from(error: GatewayCacheError) -> Self {
        Self::Internal(error.to_string())
    }
}
