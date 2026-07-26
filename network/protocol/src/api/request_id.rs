//! Per-request correlation id, shared between listeners and the peer client.
//!
//! A serving listener scopes the id around each request; the peer HTTP client
//! stamps it on outbound calls when one is in scope. One id then ties a
//! request's log lines together across the gateway and the storage nodes it
//! fans out to.

use std::future::Future;

tokio::task_local! {
    static REQUEST_ID: String;
}

/// Header carrying the correlation id end to end.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Longest inbound id honored before a fresh one is minted instead.
const MAX_REQUEST_ID_LEN: usize = 64;

/// Run a future with the request id in scope.
pub async fn with_request_id<F: Future>(id: String, future: F) -> F::Output {
    REQUEST_ID.scope(id, future).await
}

/// The id in scope, when the caller runs inside a request.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// The request span listeners nest their logs under. One definition, so the
/// span name and field names both sides grep by cannot drift apart. An absent
/// id leaves the field unrecorded rather than logging an empty value.
pub fn request_span(
    method: impl std::fmt::Display,
    uri: impl std::fmt::Display,
    request_id: Option<&str>,
) -> tracing::Span {
    match request_id {
        Some(id) => tracing::info_span!(
            "request",
            method = %method,
            uri = %uri,
            request_id = %id,
        ),
        None => tracing::info_span!(
            "request",
            method = %method,
            uri = %uri,
            request_id = tracing::field::Empty,
        ),
    }
}

/// An inbound id is honored only when short and header-safe (alphanumeric plus
/// - _ .), so a proxy-assigned id survives while junk cannot pollute logs.
pub fn sanitize_request_id(value: &str) -> Option<&str> {
    let clean = !value.is_empty()
        && value.len() <= MAX_REQUEST_ID_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    clean.then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    // the id round-trips inside its scope and is absent outside one
    #[tokio::test]
    async fn scope_round_trip() {
        assert_eq!(current_request_id(), None);
        let seen = with_request_id("abc-123".into(), async { current_request_id() }).await;
        assert_eq!(seen.as_deref(), Some("abc-123"));
        assert_eq!(current_request_id(), None);
    }

    // proxy-supplied ids are honored only when short and header-safe
    #[test]
    fn sanitizing() {
        assert_eq!(sanitize_request_id("abc-DEF_1.2"), Some("abc-DEF_1.2"));
        assert_eq!(sanitize_request_id(""), None);
        assert_eq!(sanitize_request_id("has space"), None);
        assert_eq!(sanitize_request_id("bad\theader"), None);
        assert_eq!(sanitize_request_id(&"x".repeat(MAX_REQUEST_ID_LEN + 1)), None);
    }
}
