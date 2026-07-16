//! S3 GET/HEAD response construction.

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use tape_crypto::Hash;

use super::error::S3Error;
use super::resolve::ResolvedObject;
use super::xml::civil_from_unix;
use crate::http::handlers::object::{
    ObjectResponseMetadata, ranged_object_headers, resolve_range,
};

use super::clock::SECONDS_PER_DAY;

/// Weekday abbreviations, indexed `0 = Sunday`
const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
/// Month abbreviations, indexed `0 = January`
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Format a unix timestamp (seconds, UTC) as an RFC 1123 / IMF-fixdate HTTP
/// date, e.g. `Mon, 12 Oct 2009 17:50:30 GMT`, for the `Last-Modified` header.
fn http_date(unix_seconds: i64) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(unix_seconds);
    // 1970-01-01 (day 0) is a Thursday; the table is indexed with 0 = Sunday.
    let weekday = (unix_seconds.div_euclid(SECONDS_PER_DAY).rem_euclid(7) + 4).rem_euclid(7) as usize;

    format!(
        "{}, {day:02} {} {year:04} {hour:02}:{minute:02}:{second:02} GMT",
        WEEKDAYS[weekday],
        MONTHS[(month - 1) as usize],
    )
}

/// Insert the `Last-Modified` header from an object's `block_time`, when known.
pub fn set_last_modified(headers: &mut HeaderMap, block_time: Option<i64>) {
    if let Some(unix_seconds) = block_time {
        if let Ok(value) = HeaderValue::from_str(&http_date(unix_seconds)) {
            headers.insert(header::LAST_MODIFIED, value);
        }
    }
}

/// Build the `HEAD /{bucket}/{key}` response: object headers (Content-Type,
/// Content-Length, quoted ETag, Cache-Control) from the listing index entry plus
/// `Last-Modified`, with an empty body. A `Range` request answers with the
/// ranged Content-Length and `Content-Range`, exactly as the GET would.
pub fn head_response(resolved: &ResolvedObject, range: Option<&str>) -> Result<Response, S3Error> {
    // S3 content type comes from the listing index; no filename (no
    // Content-Disposition) is set for S3 objects.
    let metadata = ObjectResponseMetadata {
        content_type: resolved.content_type,
        filename: None,
    };

    let range = resolve_range(range, resolved.size).map_err(S3Error::from)?;
    let (status, mut headers) =
        ranged_object_headers(range, resolved.size, &metadata, resolved.etag)
            .map_err(S3Error::from)?;
    set_last_modified(&mut headers, resolved.block_time);
    Ok((status, headers).into_response())
}

/// Build an ETag-only `200 OK` response: a quoted `ETag` header and empty body,
/// matching S3's PutObject/UploadPart success.
fn etag_response(etag: &str) -> Result<Response, S3Error> {
    let value = HeaderValue::from_str(&format!("\"{etag}\""))
        .map_err(|error| S3Error::Internal(format!("etag header: {error}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(header::ETAG, value);
    Ok((StatusCode::OK, headers).into_response())
}

/// PutObject success: the object ETag (the commitment hash).
pub fn put_response(etag: Hash) -> Result<Response, S3Error> {
    etag_response(&etag.to_string())
}

/// UploadPart success: the part ETag (hex of its content hash).
pub fn upload_part_response(etag: Hash) -> Result<Response, S3Error> {
    etag_response(&hex::encode(etag))
}

/// Build the `DELETE /{bucket}/{key}` (DeleteObject) success response.
pub fn delete_response() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    // the unix epoch formats as the 1970 HTTP-date
    #[test]
    fn date_epoch() {
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
    }

    // a known timestamp formats with the correct weekday
    #[test]
    fn date_known() {
        // 2009-10-12T17:50:30Z was a Monday.
        assert_eq!(http_date(1_255_369_830), "Mon, 12 Oct 2009 17:50:30 GMT");
    }

    // a pre-1970 timestamp formats correctly
    #[test]
    fn date_negative() {
        // 1969-12-31T23:59:59Z was a Wednesday.
        assert_eq!(http_date(-1), "Wed, 31 Dec 1969 23:59:59 GMT");
    }
}
