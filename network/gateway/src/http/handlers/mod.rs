use std::fmt::Display;

use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use tape_protocol::api::BINARY_CONTENT;

use super::error::RouteError;

pub(crate) mod health;
pub(crate) mod object;
pub(crate) mod track;

pub(crate) fn binary_response<T: wincode::SchemaWrite<Src = T>>(
    value: &T,
) -> Result<impl IntoResponse, RouteError> {
    let body = wincode::serialize(value)
        .map_err(|error| RouteError::Internal(format!("serialize response: {error}")))?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub(crate) fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
