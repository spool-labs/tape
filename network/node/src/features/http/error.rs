use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
pub enum RouteError {
    NotFound,
    BadRequest(String),
    NotResponsible,
    NotInCommittee,
    InvalidSignature,
    Internal(String),
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message).into_response(),
            Self::NotResponsible => (StatusCode::FORBIDDEN, "not responsible").into_response(),
            Self::NotInCommittee => (StatusCode::FORBIDDEN, "not in committee").into_response(),
            Self::InvalidSignature => {
                (StatusCode::UNAUTHORIZED, "invalid signature").into_response()
            }
            Self::Internal(message) => {
                tracing::error!("http internal error: {message}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}
