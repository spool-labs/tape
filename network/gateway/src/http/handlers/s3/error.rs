//! S3-compatible error surface. Every variant maps to an S3 code string, 
//! an HTTP status, and renders as a real S3 XML error body.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::xml;
use crate::http::error::RouteError;

/// The `x-amz-request-id` header echoed alongside the `<RequestId>` body field
const AMZ_REQUEST_ID: &str = "x-amz-request-id";

/// 64-bit odd "golden ratio" multiplier used to bit-mix the monotonic counter.
const REQUEST_ID_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// An S3-compatible error.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Variants are part of the planned S3 surface.
pub enum S3Error {
    /// The specified bucket does not exist. HTTP 404
    NoSuchBucket,
    /// The specified key does not exist. HTTP 404
    NoSuchKey,
    /// The specified multipart upload id does not exist (unknown, already
    /// completed, or aborted). HTTP 404
    NoSuchUpload,
    /// Anonymous or under-privileged access is denied. HTTP 403
    AccessDenied(String),
    /// A signed request's signature did not verify. HTTP 403
    SignatureDoesNotMatch,
    /// The received body did not hash to the signed `x-amz-content-sha256`. HTTP 400
    ContentSha256Mismatch,
    /// The object exceeds the configured maximum object size. HTTP 400
    EntityTooLarge(String),
    /// A multipart part below the 5 MiB minimum (last part exempt). HTTP 400
    EntityTooSmall(String),
    /// The request was malformed. HTTP 400
    InvalidRequest(String),
    /// A Range request that cannot be satisfied; carries the object size. HTTP 416
    InvalidRange(u64),
    /// The caller is being rate limited; carries Retry-After seconds. HTTP 503
    SlowDown { retry_after_seconds: u64 },
    /// The operation is recognized but not implemented yet. HTTP 501
    NotImplemented(String),
    /// An unexpected internal error. HTTP 500
    Internal(String),
}

impl S3Error {
    /// Build a S3Error::SlowDown from a meter retry-after Duration.
    pub fn slow_down(retry_after: Duration) -> Self {
        Self::SlowDown {
            retry_after_seconds: retry_after.as_secs().max(1),
        }
    }

    /// The S3 error `<Code>` string
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::AccessDenied(_) => "AccessDenied",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::ContentSha256Mismatch => "XAmzContentSHA256Mismatch",
            Self::EntityTooLarge(_) => "EntityTooLarge",
            Self::EntityTooSmall(_) => "EntityTooSmall",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::InvalidRange(_) => "InvalidRange",
            Self::SlowDown { .. } => "SlowDown",
            Self::NotImplemented(_) => "NotImplemented",
            Self::Internal(_) => "InternalError",
        }
    }

    /// The HTTP status code for this error
    pub fn status(&self) -> StatusCode {
        match self {
            Self::NoSuchBucket | Self::NoSuchKey | Self::NoSuchUpload => StatusCode::NOT_FOUND,
            Self::AccessDenied(_) | Self::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            Self::ContentSha256Mismatch
            | Self::EntityTooLarge(_)
            | Self::EntityTooSmall(_)
            | Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::InvalidRange(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::SlowDown { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// A human-readable `<Message>` for this error
    fn message(&self) -> String {
        match self {
            Self::NoSuchBucket => "The specified bucket does not exist.".to_string(),
            Self::NoSuchKey => "The specified key does not exist.".to_string(),
            Self::NoSuchUpload => {
                "The specified multipart upload does not exist. The upload id may be invalid, \
                 or the upload may have been aborted or completed."
                    .to_string()
            }
            Self::SignatureDoesNotMatch => {
                "The request signature we calculated does not match the signature you provided."
                    .to_string()
            }
            Self::SlowDown { .. } => "Please reduce your request rate.".to_string(),
            Self::InvalidRange(total) => {
                format!("The requested range is not satisfiable (object size {total}).")
            }
            Self::ContentSha256Mismatch => {
                "The provided 'x-amz-content-sha256' header does not match what was computed."
                    .to_string()
            }
            // Internal errors return a fixed message; the backend detail is logged
            // server-side (see `internal_detail`) and never sent to the client.
            Self::Internal(_) => "We encountered an internal error. Please try again.".to_string(),
            Self::AccessDenied(detail)
            | Self::EntityTooLarge(detail)
            | Self::EntityTooSmall(detail)
            | Self::InvalidRequest(detail)
            | Self::NotImplemented(detail) => detail.clone(),
        }
    }

    /// The backend detail for an internal error, for server-side logging only.
    /// Never rendered into the client-facing `<Message>`.
    fn internal_detail(&self) -> Option<&str> {
        match self {
            Self::Internal(detail) => Some(detail),
            Self::NoSuchBucket
            | Self::NoSuchKey
            | Self::NoSuchUpload
            | Self::AccessDenied(_)
            | Self::SignatureDoesNotMatch
            | Self::ContentSha256Mismatch
            | Self::EntityTooLarge(_)
            | Self::EntityTooSmall(_)
            | Self::InvalidRequest(_)
            | Self::SlowDown { .. }
            | Self::InvalidRange(_)
            | Self::NotImplemented(_) => None,
        }
    }
}

/// Map the shared read/decode path's RouteError onto the S3 surface.
impl From<RouteError> for S3Error {
    fn from(error: RouteError) -> Self {
        match error {
            RouteError::NotFound => Self::NoSuchKey,
            RouteError::RangeNotSatisfiable(total) => Self::InvalidRange(total),
            RouteError::BadRequest(message) => Self::InvalidRequest(message),
            RouteError::BadGateway(message) | RouteError::Internal(message) => {
                Self::Internal(message)
            }
        }
    }
}

/// Generate an S3-style request id (uppercase hex). Unique-ish without a random
/// dependency: a process-wide monotonic counter mixed with the wall clock
fn next_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:016X}", nanoseconds ^ sequence.wrapping_mul(REQUEST_ID_MIX))
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let status = self.status();
        let code = self.code();
        let message = self.message();
        let retry_after = match &self {
            Self::SlowDown { retry_after_seconds } => Some(*retry_after_seconds),
            Self::NoSuchBucket
            | Self::NoSuchKey
            | Self::NoSuchUpload
            | Self::AccessDenied(_)
            | Self::SignatureDoesNotMatch
            | Self::ContentSha256Mismatch
            | Self::EntityTooLarge(_)
            | Self::EntityTooSmall(_)
            | Self::InvalidRequest(_)
            | Self::InvalidRange(_)
            | Self::NotImplemented(_)
            | Self::Internal(_) => None,
        };

        if status.is_server_error() {
            // Log the backend detail (never sent to the client) so 5xx causes stay
            // diagnosable; fall back to the generic message for detail-less 5xx.
            let detail = self.internal_detail().unwrap_or(message.as_str());
            tracing::error!(%code, "s3 gateway error: {detail}");
        }

        let request_id = next_request_id();
        // <Resource> is left empty: S3Error carries no request path, and threading
        // it through every construction site (or rewriting the body in middleware)
        // isn't worth it for an informational field.
        let body = xml::error_body(code, &message, "", &request_id);
        let mut response =
            (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response();

        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert(AMZ_REQUEST_ID, value);
        }
        if let Some(seconds) = retry_after {
            if let Ok(value) = HeaderValue::from_str(&seconds.max(1).to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        // A 416 carries the object size in Content-Range, matching the native
        // listener's answer.
        if let Self::InvalidRange(total) = &self {
            if let Ok(value) = HeaderValue::from_str(&format!("bytes */{total}")) {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RouteError maps onto the matching S3 error variant
    #[test]
    fn route_mapping() {
        assert!(matches!(S3Error::from(RouteError::NotFound), S3Error::NoSuchKey));
        assert!(matches!(
            S3Error::from(RouteError::BadRequest("x".into())),
            S3Error::InvalidRequest(_)
        ));
        assert!(matches!(
            S3Error::from(RouteError::BadGateway("x".into())),
            S3Error::Internal(_)
        ));
        assert!(matches!(
            S3Error::from(RouteError::Internal("x".into())),
            S3Error::Internal(_)
        ));
    }

    // a sub-second retry-after clamps up to one second
    #[test]
    fn retry_clamp() {
        let error = S3Error::slow_down(Duration::from_millis(10));

        let S3Error::SlowDown { retry_after_seconds } = error else {
            unreachable!("slow_down must produce a SlowDown variant");
        };

        assert_eq!(retry_after_seconds, 1);
    }

    // each variant reports the S3-correct HTTP status
    #[test]
    fn status_codes() {
        assert_eq!(S3Error::NoSuchKey.status(), StatusCode::NOT_FOUND);
        assert_eq!(S3Error::NoSuchBucket.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            S3Error::SignatureDoesNotMatch.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            S3Error::SlowDown { retry_after_seconds: 1 }.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            S3Error::InvalidRange(1024).status(),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(S3Error::InvalidRange(1024).code(), "InvalidRange");
        assert_eq!(
            S3Error::NotImplemented("x".into()).status(),
            StatusCode::NOT_IMPLEMENTED
        );
    }
}
