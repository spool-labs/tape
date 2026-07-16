use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::ObjectMetadataOps;

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

#[derive(Clone, Debug)]
pub(crate) struct ObjectResponseMetadata {
    pub content_type: ContentType,
    pub filename: Option<Vec<u8>>,
}

pub(crate) fn object_response_metadata<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<ObjectResponseMetadata, RouteError> {
    let metadata = state
        .context
        .store
        .get_object_metadata(track_addr)
        .map_err(store_error)?;

    let content_type = metadata
        .as_ref()
        .map(|metadata| metadata.content_type)
        .unwrap_or(ContentType::Unknown);
    let filename = metadata
        .and_then(|metadata| (!metadata.name.is_empty()).then_some(metadata.name));

    Ok(ObjectResponseMetadata {
        content_type,
        filename,
    })
}

pub(in crate::http::handlers::object) fn object_response(
    bytes: Vec<u8>,
    metadata: &ObjectResponseMetadata,
    etag: Hash,
) -> Result<Response, RouteError> {
    let headers = object_headers(bytes.len() as u64, metadata, etag)?;
    Ok((StatusCode::OK, headers, bytes).into_response())
}

/// A resolved half-open byte range `[start, end)` for a `Range` request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::http::handlers::object) struct ByteRange {
    pub start: u64,
    pub end: u64,
}

/// Outcome of resolving a `Range` header against an object of `total` bytes.
pub(in crate::http::handlers::object) enum RangeOutcome {
    /// No usable range (absent, multi-range, or malformed) — serve the full object.
    Full,
    /// A single satisfiable range.
    Satisfiable(ByteRange),
    /// A syntactically valid but unsatisfiable range — respond `416`.
    Unsatisfiable,
}

/// Resolve a `Range` header value (`bytes=...`) against `total` object bytes. Only
/// a single byte range is honored; multi-range or malformed specs fall back to
/// `Full` (serve the whole object, which HTTP permits).
pub(in crate::http::handlers::object) fn parse_range(header: &str, total: u64) -> RangeOutcome {
    let Some(spec) = header.trim().strip_prefix("bytes=") else {
        return RangeOutcome::Full;
    };
    if spec.contains(',') {
        return RangeOutcome::Full;
    }
    let Some((raw_start, raw_end)) = spec.split_once('-') else {
        return RangeOutcome::Full;
    };
    let (raw_start, raw_end) = (raw_start.trim(), raw_end.trim());

    if total == 0 {
        return RangeOutcome::Unsatisfiable;
    }

    if raw_start.is_empty() {
        // Suffix range `bytes=-N`: the last N bytes.
        let Ok(suffix) = raw_end.parse::<u64>() else {
            return RangeOutcome::Full;
        };
        if suffix == 0 {
            return RangeOutcome::Unsatisfiable;
        }
        return RangeOutcome::Satisfiable(ByteRange {
            start: total.saturating_sub(suffix),
            end: total,
        });
    }

    let Ok(start) = raw_start.parse::<u64>() else {
        return RangeOutcome::Full;
    };
    if start >= total {
        return RangeOutcome::Unsatisfiable;
    }
    // HTTP's end is inclusive; clamp to the object and convert to half-open.
    let end = if raw_end.is_empty() {
        total
    } else {
        let Ok(end_inclusive) = raw_end.parse::<u64>() else {
            return RangeOutcome::Full;
        };
        end_inclusive.saturating_add(1).min(total)
    };
    if end <= start {
        return RangeOutcome::Unsatisfiable;
    }
    RangeOutcome::Satisfiable(ByteRange { start, end })
}

/// Build the read response for fully-decoded object `bytes`, honoring a single
/// `Range` (slice + `206 Partial Content`), serving the whole object (`200`), or
/// rejecting an unsatisfiable range (`416`). Single-track objects only — the
/// bytes are already in memory, so the slice is free.
pub(in crate::http::handlers::object) fn object_response_ranged(
    bytes: Vec<u8>,
    metadata: &ObjectResponseMetadata,
    etag: Hash,
    range_header: Option<&str>,
) -> Result<Response, RouteError> {
    let total = bytes.len() as u64;
    let outcome = range_header.map_or(RangeOutcome::Full, |header| parse_range(header, total));
    match outcome {
        RangeOutcome::Full => {
            let mut headers = object_headers(total, metadata, etag)?;
            headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            Ok((StatusCode::OK, headers, bytes).into_response())
        }
        RangeOutcome::Satisfiable(range) => {
            // Zero-copy slice: `Bytes::from` takes ownership and `slice` is a refcount.
            let length = range.end - range.start;
            let slice = Bytes::from(bytes).slice(range.start as usize..range.end as usize);
            let mut headers = object_headers(length, metadata, etag)?;
            headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {}-{}/{total}", range.start, range.end - 1))
                    .map_err(|error| RouteError::Internal(format!("content range header: {error}")))?,
            );
            Ok((StatusCode::PARTIAL_CONTENT, headers, slice).into_response())
        }
        RangeOutcome::Unsatisfiable => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{total}"))
                    .map_err(|error| RouteError::Internal(format!("content range header: {error}")))?,
            );
            Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response())
        }
    }
}

pub(crate) fn object_headers(
    content_length: u64,
    metadata: &ObjectResponseMetadata,
    etag: Hash,
) -> Result<HeaderMap, RouteError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(metadata.content_type.to_str()),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| RouteError::Internal(format!("content length header: {error}")))?,
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{etag}\""))
            .map_err(|error| RouteError::Internal(format!("etag header: {error}")))?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    if let Some(filename) = metadata.filename.as_deref() {
        headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&content_disposition(filename)).map_err(|error| {
                RouteError::Internal(format!("content disposition header: {error}"))
            })?,
        );
    }

    Ok(headers)
}

fn content_disposition(filename: &[u8]) -> String {
    format!("attachment; filename*=UTF-8''{}", encode_rfc5987(filename))
}

// RFC 5987 encoding for the Content-Disposition `filename*` parameter.
fn encode_rfc5987(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value {
        match byte {
            b'0'..=b'9'
            | b'A'..=b'Z'
            | b'a'..=b'z'
            | b'!'
            | b'#'
            | b'$'
            | b'&'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~' => out.push(byte as char),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use axum::http::header;
    use tape_core::types::ContentType;
    use tape_crypto::Hash;

    use super::{ObjectResponseMetadata, object_headers};

    #[test]
    fn object_headers_include_encoded_filename() {
        let metadata = ObjectResponseMetadata {
            content_type: ContentType::TextPlain,
            filename: Some(b"reports/june final.txt".to_vec()),
        };

        let headers = object_headers(42, &metadata, Hash::default()).unwrap();

        assert_eq!(
            headers
                .get(header::CONTENT_DISPOSITION)
                .and_then(|value| value.to_str().ok()),
            Some("attachment; filename*=UTF-8''reports%2Fjune%20final.txt")
        );
    }

    #[test]
    fn object_headers_skip_empty_metadata_filename() {
        let metadata = ObjectResponseMetadata {
            content_type: ContentType::Unknown,
            filename: None,
        };

        let headers = object_headers(42, &metadata, Hash::default()).unwrap();

        assert!(headers.get(header::CONTENT_DISPOSITION).is_none());
    }

    #[test]
    fn parse_range_variants() {
        use super::{ByteRange, RangeOutcome, parse_range};

        // Inclusive HTTP end becomes a half-open `[start, end)`.
        assert!(matches!(
            parse_range("bytes=0-99", 1000),
            RangeOutcome::Satisfiable(ByteRange { start: 0, end: 100 })
        ));
        // Open-ended runs to the object end.
        assert!(matches!(
            parse_range("bytes=500-", 1000),
            RangeOutcome::Satisfiable(ByteRange { start: 500, end: 1000 })
        ));
        // Suffix range is the last N bytes.
        assert!(matches!(
            parse_range("bytes=-200", 1000),
            RangeOutcome::Satisfiable(ByteRange { start: 800, end: 1000 })
        ));
        // The end clamps to the object size.
        assert!(matches!(
            parse_range("bytes=0-9999", 1000),
            RangeOutcome::Satisfiable(ByteRange { start: 0, end: 1000 })
        ));
        // start at/after the end is unsatisfiable -> 416.
        assert!(matches!(parse_range("bytes=1000-", 1000), RangeOutcome::Unsatisfiable));
        // Malformed, multi-range, and non-`bytes` units fall back to the full object.
        assert!(matches!(parse_range("bytes=abc", 1000), RangeOutcome::Full));
        assert!(matches!(parse_range("bytes=0-1,4-5", 1000), RangeOutcome::Full));
        assert!(matches!(parse_range("items=0-1", 1000), RangeOutcome::Full));
    }
}
