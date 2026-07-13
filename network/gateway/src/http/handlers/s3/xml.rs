//! Minimal S3 XML body builders

/// XML 1.0 declaration prefixed to every S3 response body
pub const XML_DECL: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

/// S3 namespace used by listing responses
pub const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Default storage class reported for every object.
#[allow(dead_code)]
pub const STORAGE_CLASS_STANDARD: &str = "STANDARD";

use super::clock::{SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE};

/// Escape the five predefined XML entities in `value` into `out`
fn escape_into(out: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
}

/// Escape the five predefined XML entities in `value`
#[allow(dead_code)]
pub fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    escape_into(&mut out, value);
    out
}

/// Append `<tag>escaped(value)</tag>` to `out`
fn push_element(out: &mut String, tag: &str, value: &str) {
    out.push('<');
    out.push_str(tag);
    out.push('>');
    escape_into(out, value);
    out.push_str("</");
    out.push_str(tag);
    out.push('>');
}

/// Append `<tag>escaped(value)</tag>` only when `value` is `Some`
fn push_optional(out: &mut String, tag: &str, value: Option<&str>) {
    if let Some(value) = value {
        push_element(out, tag, value);
    }
}

/// Decompose a unix timestamp (seconds, UTC) into `(year, month, day, hour, minute,
/// second)` via Howard Hinnant's `civil_from_days`, so the gateway needs no calendar
/// dependency. Negative timestamps (pre-1970) are handled.
pub fn civil_from_unix(unix_seconds: i64) -> (i64, i64, i64, i64, i64, i64) {
    let days = unix_seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = unix_seconds.rem_euclid(SECONDS_PER_DAY);
    let hour = seconds_of_day / SECONDS_PER_HOUR;
    let minute = (seconds_of_day % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    let second = seconds_of_day % SECONDS_PER_MINUTE;

    // civil_from_days: shift the epoch to 0000-03-01 so leap days fall last.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year_shifted = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year_shifted + 1 } else { year_shifted };
    (year, month, day, hour, minute, second)
}

/// Format a unix timestamp (seconds, UTC) as an S3 ISO 8601 string, e.g.
/// `2009-10-12T17:50:30.000Z`. Negative timestamps (pre-1970) are handled.
pub fn iso8601(unix_seconds: i64) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(unix_seconds);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.000Z")
}

/// Build an S3 `<Error>` XML body
pub fn error_body(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str("<Error>");
    push_element(&mut out, "Code", code);
    push_element(&mut out, "Message", message);
    push_element(&mut out, "Resource", resource);
    push_element(&mut out, "RequestId", request_id);
    out.push_str("</Error>");
    out
}

/// The owner identity echoed in listing responses
#[allow(dead_code)]
pub struct Owner {
    /// Canonical owner id
    pub id: String,
    /// Human-readable owner name
    pub display_name: String,
}

/// One `<Bucket>` entry in a `ListAllMyBucketsResult` body
pub struct BucketEntry {
    /// Bucket name (a base58 tape address)
    pub name: String,
    /// Creation time in unix seconds
    pub creation_date: i64,
}

/// Build a `ListAllMyBucketsResult` (ListBuckets) response body
pub fn list_all_my_buckets_body(owner: &Owner, buckets: &[BucketEntry]) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str("<ListAllMyBucketsResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");

    out.push_str("<Owner>");
    push_element(&mut out, "ID", &owner.id);
    push_element(&mut out, "DisplayName", &owner.display_name);
    out.push_str("</Owner>");

    out.push_str("<Buckets>");
    for bucket in buckets {
        out.push_str("<Bucket>");
        push_element(&mut out, "Name", &bucket.name);
        push_element(&mut out, "CreationDate", &iso8601(bucket.creation_date));
        out.push_str("</Bucket>");
    }
    out.push_str("</Buckets>");

    out.push_str("</ListAllMyBucketsResult>");
    out
}

/// One `<Contents>` entry in a `ListBucketResult` (ListObjectsV2) body
#[allow(dead_code)]
pub struct ObjectEntry {
    /// Object key (the name from the object-list index, decoded as UTF-8)
    pub key: String,
    /// Last-modified time in unix seconds, when the backing block carried one
    pub last_modified: Option<i64>,
    /// ETag value; the builder adds the surrounding quotes S3 requires
    pub etag: String,
    /// Object size in bytes
    pub size: u64,
    /// S3 storage class (typically STORAGE_CLASS_STANDARD)
    pub storage_class: &'static str,
}

/// A `ListBucketResult` (ListObjectsV2) response.
#[allow(dead_code)]
pub struct ListObjectsV2 {
    /// Bucket name (the base58 tape address as requested)
    pub name: String,
    /// Echoed request prefix (empty string when none was supplied)
    pub prefix: String,
    /// Echoed request delimiter, when supplied
    pub delimiter: Option<String>,
    /// Echoed, clamped `max-keys` (S3 caps this at 1000)
    pub max_keys: u32,
    /// Number of keys returned: `contents.len() + common_prefixes.len()`
    pub key_count: u32,
    /// Whether more results remain beyond this page
    pub is_truncated: bool,
    /// Echoed request continuation token, when one was supplied
    pub continuation_token: Option<String>,
    /// Opaque base64 token to resume from, when `is_truncated`
    pub next_continuation_token: Option<String>,
    /// Echoed `start-after`, when supplied
    pub start_after: Option<String>,
    /// Matching objects in lexicographic key order
    pub contents: Vec<ObjectEntry>,
    /// Rolled-up folder prefixes (only when a delimiter is given)
    pub common_prefixes: Vec<String>,
}

/// Build a `ListBucketResult` (ListObjectsV2) response body
#[allow(dead_code)]
pub fn list_objects_v2_body(result: &ListObjectsV2) -> String {
    let mut out =
        String::with_capacity(512 + result.contents.len() * 256 + result.common_prefixes.len() * 64);
    out.push_str(XML_DECL);
    out.push_str("<ListBucketResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");

    push_element(&mut out, "Name", &result.name);
    // S3 always emits <Prefix>, even when empty.
    push_element(&mut out, "Prefix", &result.prefix);
    push_optional(&mut out, "ContinuationToken", result.continuation_token.as_deref());
    push_optional(&mut out, "StartAfter", result.start_after.as_deref());
    push_element(&mut out, "KeyCount", &result.key_count.to_string());
    push_element(&mut out, "MaxKeys", &result.max_keys.to_string());
    push_optional(&mut out, "Delimiter", result.delimiter.as_deref());
    push_element(&mut out, "IsTruncated", if result.is_truncated { "true" } else { "false" });
    push_optional(
        &mut out,
        "NextContinuationToken",
        result.next_continuation_token.as_deref(),
    );

    for entry in &result.contents {
        out.push_str("<Contents>");
        push_element(&mut out, "Key", &entry.key);
        // S3 always reports LastModified; fall back to the unix epoch when the
        // backing block had no wall-clock time recorded.
        push_element(
            &mut out,
            "LastModified",
            &iso8601(entry.last_modified.unwrap_or(0)),
        );
        // ETag is rendered quoted, e.g. <ETag>"abc..."</ETag>.
        out.push_str("<ETag>\"");
        escape_into(&mut out, &entry.etag);
        out.push_str("\"</ETag>");
        push_element(&mut out, "Size", &entry.size.to_string());
        push_element(&mut out, "StorageClass", entry.storage_class);
        out.push_str("</Contents>");
    }

    for prefix in &result.common_prefixes {
        out.push_str("<CommonPrefixes>");
        push_element(&mut out, "Prefix", prefix);
        out.push_str("</CommonPrefixes>");
    }

    out.push_str("</ListBucketResult>");
    out
}

/// A `ListBucketResult` response
pub struct ListObjectsV1 {
    /// Bucket name (the base58 tape address as requested)
    pub name: String,
    /// Echoed request prefix (empty string when none was supplied)
    pub prefix: String,
    /// Echoed request marker (empty string when none was supplied)
    pub marker: String,
    /// The key to resume after on the next request, when `is_truncated`
    pub next_marker: Option<String>,
    /// Echoed, clamped `max-keys` (S3 caps this at 1000)
    pub max_keys: u32,
    /// Echoed request delimiter, when supplied
    pub delimiter: Option<String>,
    /// Whether more results remain beyond this page
    pub is_truncated: bool,
    /// Matching objects in lexicographic key order
    pub contents: Vec<ObjectEntry>,
    /// Rolled-up folder prefixes (only when a delimiter is given)
    pub common_prefixes: Vec<String>,
}

/// Build a `ListBucketResult` response body
pub fn list_objects_v1_body(result: &ListObjectsV1) -> String {
    let mut out =
        String::with_capacity(512 + result.contents.len() * 256 + result.common_prefixes.len() * 64);
    out.push_str(XML_DECL);
    out.push_str("<ListBucketResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");

    push_element(&mut out, "Name", &result.name);
    // S3 always emits <Prefix> and <Marker>, even when empty.
    push_element(&mut out, "Prefix", &result.prefix);
    push_element(&mut out, "Marker", &result.marker);
    push_optional(&mut out, "NextMarker", result.next_marker.as_deref());
    push_element(&mut out, "MaxKeys", &result.max_keys.to_string());
    push_optional(&mut out, "Delimiter", result.delimiter.as_deref());
    push_element(&mut out, "IsTruncated", if result.is_truncated { "true" } else { "false" });

    for entry in &result.contents {
        out.push_str("<Contents>");
        push_element(&mut out, "Key", &entry.key);
        push_element(
            &mut out,
            "LastModified",
            &iso8601(entry.last_modified.unwrap_or(0)),
        );
        out.push_str("<ETag>\"");
        escape_into(&mut out, &entry.etag);
        out.push_str("\"</ETag>");
        push_element(&mut out, "Size", &entry.size.to_string());
        push_element(&mut out, "StorageClass", entry.storage_class);
        out.push_str("</Contents>");
    }

    for prefix in &result.common_prefixes {
        out.push_str("<CommonPrefixes>");
        push_element(&mut out, "Prefix", prefix);
        out.push_str("</CommonPrefixes>");
    }

    out.push_str("</ListBucketResult>");
    out
}

/// Build an `InitiateMultipartUploadResult` (CreateMultipartUpload) body.
pub fn initiate_multipart_upload_body(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str("<InitiateMultipartUploadResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");
    push_element(&mut out, "Bucket", bucket);
    push_element(&mut out, "Key", key);
    push_element(&mut out, "UploadId", upload_id);
    out.push_str("</InitiateMultipartUploadResult>");
    out
}

/// Build a `CompleteMultipartUploadResult` body.
pub fn complete_multipart_upload_body(
    location: &str,
    bucket: &str,
    key: &str,
    etag: &str,
) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(XML_DECL);
    out.push_str("<CompleteMultipartUploadResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");
    push_element(&mut out, "Location", location);
    push_element(&mut out, "Bucket", bucket);
    push_element(&mut out, "Key", key);
    out.push_str("<ETag>\"");
    escape_into(&mut out, etag);
    out.push_str("\"</ETag>");
    out.push_str("</CompleteMultipartUploadResult>");
    out
}

/// One `<Part>` entry in a `ListPartsResult` body
pub struct PartEntry {
    /// Part number (1..=10000)
    pub part_number: u32,
    /// Upload time in unix seconds
    pub last_modified: i64,
    /// Part ETag (the builder adds the surrounding quotes S3 requires)
    pub etag: String,
    /// Part size in bytes
    pub size: u64,
}

/// Build a `ListPartsResult` body.
pub fn list_parts_body(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number_marker: u32,
    next_part_number_marker: u32,
    max_parts: u32,
    is_truncated: bool,
    parts: &[PartEntry],
) -> String {
    let mut out = String::with_capacity(512 + parts.len() * 192);
    out.push_str(XML_DECL);
    out.push_str("<ListPartsResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");
    push_element(&mut out, "Bucket", bucket);
    push_element(&mut out, "Key", key);
    push_element(&mut out, "UploadId", upload_id);
    push_element(&mut out, "StorageClass", STORAGE_CLASS_STANDARD);
    push_element(&mut out, "PartNumberMarker", &part_number_marker.to_string());
    push_element(&mut out, "NextPartNumberMarker", &next_part_number_marker.to_string());
    push_element(&mut out, "MaxParts", &max_parts.to_string());
    push_element(&mut out, "IsTruncated", if is_truncated { "true" } else { "false" });

    for part in parts {
        out.push_str("<Part>");
        push_element(&mut out, "PartNumber", &part.part_number.to_string());
        push_element(&mut out, "LastModified", &iso8601(part.last_modified));
        out.push_str("<ETag>\"");
        escape_into(&mut out, &part.etag);
        out.push_str("\"</ETag>");
        push_element(&mut out, "Size", &part.size.to_string());
        out.push_str("</Part>");
    }

    out.push_str("</ListPartsResult>");
    out
}

/// One `<Upload>` entry in a `ListMultipartUploadsResult` body.
pub struct UploadEntry {
    /// Object key the upload targets
    pub key: String,
    /// Opaque upload id
    pub upload_id: String,
    /// Initiation time in unix seconds
    pub initiated: i64,
}

/// Build a `ListMultipartUploadsResult` body.
pub fn list_multipart_uploads_body(bucket: &str, uploads: &[UploadEntry]) -> String {
    let mut out = String::with_capacity(512 + uploads.len() * 192);
    out.push_str(XML_DECL);
    out.push_str("<ListMultipartUploadsResult xmlns=\"");
    out.push_str(S3_XMLNS);
    out.push_str("\">");
    push_element(&mut out, "Bucket", bucket);
    push_element(&mut out, "KeyMarker", "");
    push_element(&mut out, "UploadIdMarker", "");
    push_element(&mut out, "NextKeyMarker", "");
    push_element(&mut out, "NextUploadIdMarker", "");
    push_element(&mut out, "MaxUploads", "1000");
    push_element(&mut out, "IsTruncated", "false");

    for upload in uploads {
        out.push_str("<Upload>");
        push_element(&mut out, "Key", &upload.key);
        push_element(&mut out, "UploadId", &upload.upload_id);
        push_element(&mut out, "StorageClass", STORAGE_CLASS_STANDARD);
        push_element(&mut out, "Initiated", &iso8601(upload.initiated));
        out.push_str("</Upload>");
    }

    out.push_str("</ListMultipartUploadsResult>");
    out
}

/// Parse a `CompleteMultipartUpload` request body into ordered `(part_number,
/// etag)` pairs.
pub fn parse_complete_multipart_upload(body: &str) -> Result<Vec<(u32, String)>, String> {
    let mut parts = Vec::new();
    let mut rest = body;
    while let Some(open) = rest.find("<Part>") {
        let after = &rest[open + "<Part>".len()..];
        let close = after
            .find("</Part>")
            .ok_or_else(|| "unterminated <Part> element".to_string())?;
        let block = &after[..close];

        let part_number = extract_element(block, "PartNumber")
            .ok_or_else(|| "missing <PartNumber> in <Part>".to_string())?
            .trim()
            .parse::<u32>()
            .map_err(|_| "invalid <PartNumber> value".to_string())?;
        let etag = normalize_part_etag(
            &extract_element(block, "ETag").ok_or_else(|| "missing <ETag> in <Part>".to_string())?,
        );

        parts.push((part_number, etag));
        rest = &after[close + "</Part>".len()..];
    }

    if parts.is_empty() {
        return Err("CompleteMultipartUpload listed no <Part> elements".to_string());
    }
    Ok(parts)
}

/// Read the text content of the first `<tag>...</tag>` in `block`, unescaping the
/// predefined XML entities.
fn extract_element(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(unescape(&block[start..end]))
}

/// Normalize an ETag from a request body so it compares equal to the hex ETag
/// the gateway returned.
fn normalize_part_etag(raw: &str) -> String {
    let value = raw.trim();
    let value = value.strip_prefix("W/").unwrap_or(value).trim();
    let value = value
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(value);
    value.trim().to_ascii_lowercase()
}

/// Reverse escape_into
fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let tail = &rest[amp..];
        if let Some(semi) = tail.find(';') {
            let entity = &tail[..=semi];
            match entity {
                "&amp;" => out.push('&'),
                "&lt;" => out.push('<'),
                "&gt;" => out.push('>'),
                "&quot;" => out.push('"'),
                "&apos;" => out.push('\''),
                // Unknown entity: pass it through verbatim.
                other => out.push_str(other),
            }
            rest = &tail[semi + 1..];
        } else {
            // No terminating ';': nothing more to decode.
            out.push_str(tail);
            return out;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // the unix epoch renders as the 1970 ISO 8601 instant
    #[test]
    fn iso_epoch() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
    }

    // a known timestamp renders to its ISO 8601 string
    #[test]
    fn iso_known() {
        // 2009-10-12T17:50:30Z
        assert_eq!(iso8601(1_255_369_830), "2009-10-12T17:50:30.000Z");
    }

    // a pre-1970 timestamp renders correctly
    #[test]
    fn iso_negative() {
        assert_eq!(iso8601(-1), "1969-12-31T23:59:59.000Z");
    }

    // the error body carries code, message, resource, and request id
    #[test]
    fn error_fields() {
        let body = error_body("NoSuchKey", "missing", "/bucket/key", "REQ123");
        assert!(body.starts_with(XML_DECL));
        assert!(body.contains("<Code>NoSuchKey</Code>"));
        assert!(body.contains("<Message>missing</Message>"));
        assert!(body.contains("<Resource>/bucket/key</Resource>"));
        assert!(body.contains("<RequestId>REQ123</RequestId>"));
    }

    // the error body escapes XML metacharacters
    #[test]
    fn error_escaping() {
        let body = error_body("Code", "a & b < c", "/x?<y>", "id");
        assert!(body.contains("a &amp; b &lt; c"));
        assert!(body.contains("/x?&lt;y&gt;"));
    }

    // a listing renders contents and common prefixes
    #[test]
    fn list_render() {
        let result = ListObjectsV2 {
            name: "bucket".to_string(),
            prefix: "photos/".to_string(),
            delimiter: Some("/".to_string()),
            max_keys: 1000,
            key_count: 2,
            is_truncated: true,
            continuation_token: None,
            next_continuation_token: Some("dG9rZW4=".to_string()),
            start_after: None,
            contents: vec![ObjectEntry {
                key: "photos/a.jpg".to_string(),
                last_modified: Some(1_255_369_830),
                etag: "abc123".to_string(),
                size: 42,
                storage_class: STORAGE_CLASS_STANDARD,
            }],
            common_prefixes: vec!["photos/sub/".to_string()],
        };
        let body = list_objects_v2_body(&result);
        assert!(body.contains("<Name>bucket</Name>"));
        assert!(body.contains("<Prefix>photos/</Prefix>"));
        assert!(body.contains("<Delimiter>/</Delimiter>"));
        assert!(body.contains("<KeyCount>2</KeyCount>"));
        assert!(body.contains("<MaxKeys>1000</MaxKeys>"));
        assert!(body.contains("<IsTruncated>true</IsTruncated>"));
        assert!(body.contains("<NextContinuationToken>dG9rZW4=</NextContinuationToken>"));
        assert!(body.contains("<Key>photos/a.jpg</Key>"));
        assert!(body.contains("<ETag>\"abc123\"</ETag>"));
        assert!(body.contains("<Size>42</Size>"));
        assert!(body.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(body.contains("<LastModified>2009-10-12T17:50:30.000Z</LastModified>"));
        assert!(body.contains("<CommonPrefixes><Prefix>photos/sub/</Prefix></CommonPrefixes>"));
    }

    // a listing matches its exact wire bytes
    #[test]
    fn list_fixture() {
        // A small, fully-specified ListObjectsV2 fixture pinned to its exact wire
        // bytes: one object, no truncation, no delimiter/continuation/start-after.
        // This guards element ordering and the always-present empty <Prefix>, which
        // the substring-based test above does not.
        let result = ListObjectsV2 {
            name: "tape-bucket".to_string(),
            prefix: String::new(),
            delimiter: None,
            max_keys: 1000,
            key_count: 1,
            is_truncated: false,
            continuation_token: None,
            next_continuation_token: None,
            start_after: None,
            contents: vec![ObjectEntry {
                key: "hello.txt".to_string(),
                last_modified: Some(1_255_369_830),
                etag: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
                size: 11,
                storage_class: STORAGE_CLASS_STANDARD,
            }],
            common_prefixes: Vec::new(),
        };
        let expected = concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
            "<Name>tape-bucket</Name>",
            "<Prefix></Prefix>",
            "<KeyCount>1</KeyCount>",
            "<MaxKeys>1000</MaxKeys>",
            "<IsTruncated>false</IsTruncated>",
            "<Contents>",
            "<Key>hello.txt</Key>",
            "<LastModified>2009-10-12T17:50:30.000Z</LastModified>",
            "<ETag>\"d41d8cd98f00b204e9800998ecf8427e\"</ETag>",
            "<Size>11</Size>",
            "<StorageClass>STANDARD</StorageClass>",
            "</Contents>",
            "</ListBucketResult>",
        );
        assert_eq!(list_objects_v2_body(&result), expected);
    }

    // a V1 listing uses Marker/NextMarker and omits the V2-only fields
    #[test]
    fn list_v1_render() {
        let result = ListObjectsV1 {
            name: "bucket".to_string(),
            prefix: "photos/".to_string(),
            marker: "photos/a.jpg".to_string(),
            next_marker: Some("photos/c.jpg".to_string()),
            max_keys: 1000,
            delimiter: Some("/".to_string()),
            is_truncated: true,
            contents: vec![ObjectEntry {
                key: "photos/b.jpg".to_string(),
                last_modified: Some(1_255_369_830),
                etag: "abc123".to_string(),
                size: 42,
                storage_class: STORAGE_CLASS_STANDARD,
            }],
            common_prefixes: vec!["photos/sub/".to_string()],
        };
        let body = list_objects_v1_body(&result);
        assert!(body.contains("<Marker>photos/a.jpg</Marker>"));
        assert!(body.contains("<NextMarker>photos/c.jpg</NextMarker>"));
        assert!(body.contains("<IsTruncated>true</IsTruncated>"));
        assert!(body.contains("<Key>photos/b.jpg</Key>"));
        assert!(body.contains("<ETag>\"abc123\"</ETag>"));
        assert!(body.contains("<CommonPrefixes><Prefix>photos/sub/</Prefix></CommonPrefixes>"));
        // V1 must not carry V2-only fields.
        assert!(!body.contains("KeyCount"));
        assert!(!body.contains("ContinuationToken"));
    }

    // the initiate body carries bucket, key, and upload id
    #[test]
    fn initiate_render() {
        let body = initiate_multipart_upload_body("bucket", "obj/key", "UPLOAD123");
        assert!(body.starts_with(XML_DECL));
        assert!(body.contains("<Bucket>bucket</Bucket>"));
        assert!(body.contains("<Key>obj/key</Key>"));
        assert!(body.contains("<UploadId>UPLOAD123</UploadId>"));
    }

    // the complete body quotes the assembled ETag
    #[test]
    fn complete_render() {
        let body = complete_multipart_upload_body("/bucket/obj", "bucket", "obj", "abc123");
        assert!(body.contains("<Location>/bucket/obj</Location>"));
        assert!(body.contains("<ETag>\"abc123\"</ETag>"));
    }

    // the parts listing renders each buffered part
    #[test]
    fn parts_render() {
        let parts = vec![
            PartEntry {
                part_number: 1,
                last_modified: 0,
                etag: "aaa".to_string(),
                size: 5,
            },
            PartEntry {
                part_number: 2,
                last_modified: 0,
                etag: "bbb".to_string(),
                size: 7,
            },
        ];
        let body = list_parts_body("bucket", "obj", "UP", 0, 0, 1000, false, &parts);
        assert!(body.contains("<UploadId>UP</UploadId>"));
        assert!(body.contains("<MaxParts>1000</MaxParts>"));
        assert!(body.contains("<IsTruncated>false</IsTruncated>"));
        assert!(body.contains("<Part><PartNumber>1</PartNumber>"));
        assert!(body.contains("<ETag>\"aaa\"</ETag>"));
        assert!(body.contains("<Size>7</Size>"));
    }

    // a truncated parts page reports the resume marker
    #[test]
    fn parts_truncated() {
        let parts = vec![PartEntry {
            part_number: 3,
            last_modified: 0,
            etag: "aaa".to_string(),
            size: 5,
        }];
        let body = list_parts_body("bucket", "obj", "UP", 2, 3, 1, true, &parts);
        assert!(body.contains("<PartNumberMarker>2</PartNumberMarker>"));
        assert!(body.contains("<NextPartNumberMarker>3</NextPartNumberMarker>"));
        assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    }

    // a multipart-uploads listing renders each in-flight upload
    #[test]
    fn uploads_render() {
        let uploads = vec![UploadEntry {
            key: "obj".to_string(),
            upload_id: "UP".to_string(),
            initiated: 0,
        }];
        let body = list_multipart_uploads_body("bucket", &uploads);
        assert!(body.contains("<ListMultipartUploadsResult"));
        assert!(body.contains("<Upload><Key>obj</Key><UploadId>UP</UploadId>"));
        assert!(body.contains("<IsTruncated>false</IsTruncated>"));
    }

    // the complete-upload body parses into ordered parts
    #[test]
    fn parse_parts() {
        let body = "<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"ABC\"</ETag></Part>\
            <Part><PartNumber>2</PartNumber><ETag>W/\"def\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload(body).expect("test setup");
        assert_eq!(parts, vec![(1, "abc".to_string()), (2, "def".to_string())]);
    }

    // a body listing no parts is rejected
    #[test]
    fn parse_empty() {
        assert!(parse_complete_multipart_upload("<CompleteMultipartUpload/>").is_err());
    }

    // a non-numeric part number is rejected
    #[test]
    fn parse_invalid() {
        let body = "<Part><PartNumber>x</PartNumber><ETag>\"a\"</ETag></Part>";
        assert!(parse_complete_multipart_upload(body).is_err());
    }

    // unescape inverts escape
    #[test]
    fn round_trip() {
        let raw = "a & b < c > d \" e ' f";
        assert_eq!(unescape(&escape(raw)), raw);
    }

    // the buckets listing renders the owner and each bucket
    #[test]
    fn buckets_render() {
        let owner = Owner {
            id: "owner-id".to_string(),
            display_name: "owner".to_string(),
        };
        let buckets = vec![BucketEntry {
            name: "tapeaddr".to_string(),
            creation_date: 0,
        }];
        let body = list_all_my_buckets_body(&owner, &buckets);
        assert!(body.contains("<Owner><ID>owner-id</ID><DisplayName>owner</DisplayName></Owner>"));
        assert!(body.contains(
            "<Bucket><Name>tapeaddr</Name><CreationDate>1970-01-01T00:00:00.000Z</CreationDate></Bucket>"
        ));
    }
}
