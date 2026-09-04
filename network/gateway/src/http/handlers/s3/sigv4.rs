//! AWS Signature Version 4 verification for the S3 surface.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use tape_node::config::gateway::S3Config;

use super::authz::Auth;
use super::clock::now_unix;
use super::error::S3Error;

/// SigV4 algorithm identifier, used as the `Authorization` scheme, the
/// `X-Amz-Algorithm` value, and the first line of the string-to-sign.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// Terminating component of every SigV4 credential scope
const AWS4_REQUEST: &str = "aws4_request";
/// Request-timestamp header (`YYYYMMDDTHHMMSSZ`)
const AMZ_DATE: &str = "x-amz-date";
/// Header carrying the hex SHA-256 of the payload (or a sentinel)
const AMZ_CONTENT_SHA256: &str = "x-amz-content-sha256";
/// Sentinel payload hash used by presigned URLs
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
/// Prefix of the `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
const STREAMING_PAYLOAD_PREFIX: &str = "STREAMING-";
/// Hex SHA-256 of the empty string, the payload hash for bodyless requests
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// Presigned-URL signature query parameter (excluded from the canonical query)
const AMZ_SIGNATURE_PARAM: &str = "X-Amz-Signature";

/// Maximum clock skew tolerated between a signed request's timestamp and the
/// gateway's clock, in seconds.
const MAX_CLOCK_SKEW_SECS: i64 = 900;

/// Maximum lifetime AWS permits on a presigned URL: 7 days.
const MAX_PRESIGNED_EXPIRY_SECS: i64 = 604_800;

use super::clock::{SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE};

type HmacSha256 = Hmac<Sha256>;

/// A SigV4 credential the gateway accepts on signed requests
#[derive(Clone)]
pub struct SigV4Credential {
    /// The access key id clients embed in their credential scope
    pub access_key_id: String,
    /// The secret used to derive the signing key
    pub secret_access_key: String,
}

/// Verification state shared by the auth middleware.
pub struct SigV4Verifier {
    credential: Option<SigV4Credential>,
}

impl SigV4Verifier {
    /// Wrap a credential (or its absence) as shared verification state
    pub fn new(credential: Option<SigV4Credential>) -> Arc<Self> {
        Arc::new(Self { credential })
    }

    /// Verify a request and return the Auth.
    fn authenticate_with_payload(
        &self,
        request: &Request,
        now: i64,
    ) -> Result<(Auth, Option<String>), S3Error> {
        match PresentedSignature::extract(request)? {
            Some(presented) => {
                let auth = self.verify(request, &presented)?;
                check_request_freshness(&presented, now)?;
                Ok((auth, Some(presented.payload_hash)))
            }
            None => {
                // Unsigned: reads are public, writes require a signature.
                if is_read_method(request.method()) {
                    Ok((Auth::Anonymous, None))
                } else {
                    Err(S3Error::AccessDenied(
                        "anonymous access to this S3 write operation is not allowed".to_string(),
                    ))
                }
            }
        }
    }

    /// Reconstruct the canonical request, derive the signing key from the
    /// presented scope, and compare signatures in constant time.
    fn verify(
        &self,
        request: &Request,
        presented: &PresentedSignature,
    ) -> Result<Auth, S3Error> {
        let credential = self
            .credential
            .as_ref()
            .ok_or(S3Error::SignatureDoesNotMatch)?;
        if presented.access_key_id != credential.access_key_id {
            return Err(S3Error::SignatureDoesNotMatch);
        }

        // Scope is `YYYYMMDD/region/service/aws4_request`.
        let scope_parts: Vec<&str> = presented.scope.split('/').collect();
        if scope_parts.len() != 4 || scope_parts[3] != AWS4_REQUEST {
            return Err(S3Error::SignatureDoesNotMatch);
        }
        let (date_stamp, region, service) = (scope_parts[0], scope_parts[1], scope_parts[2]);

        // Signed headers must be lowercase and sorted in the canonical request;
        // a compliant client already sends them that way, but re-sort to be safe.
        let mut signed_headers = presented.signed_headers.clone();
        signed_headers.sort();
        let canonical_headers = canonical_headers(request, &signed_headers)?;
        let signed_headers_list = signed_headers.join(";");

        // S3 does not normalize URI paths: the raw, already-encoded request path
        // is the canonical URI. The query string is canonicalized (decoded then
        // re-encoded, sorted, with the signature param dropped when presigned).
        let canonical_request = format!(
            "{method}\n{uri}\n{query}\n{headers}\n{signed}\n{payload}",
            method = request.method().as_str(),
            uri = request.uri().path(),
            query = canonical_query_string(request.uri().query(), presented.is_presigned),
            headers = canonical_headers,
            signed = signed_headers_list,
            payload = presented.payload_hash,
        );

        let string_to_sign = format!(
            "{ALGORITHM}\n{date}\n{scope}\n{hash}",
            date = presented.date,
            scope = presented.scope,
            hash = sha256_hex(canonical_request.as_bytes()),
        );

        let signing_key =
            derive_signing_key(&credential.secret_access_key, date_stamp, region, service)?;
        let expected = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes())?);

        if constant_time_eq(expected.as_bytes(), presented.signature.as_bytes()) {
            Ok(Auth::verified(presented.access_key_id.clone()))
        } else {
            Err(S3Error::SignatureDoesNotMatch)
        }
    }
}

pub fn verifier_from_config(config: &S3Config) -> Arc<SigV4Verifier> {
    let credential = match (
        config.access_key_id.as_deref(),
        config.secret_access_key.as_deref(),
    ) {
        (Some(access_key_id), Some(secret_access_key))
            if !access_key_id.is_empty() && !secret_access_key.is_empty() =>
        {
            Some(SigV4Credential {
                access_key_id: access_key_id.to_string(),
                secret_access_key: secret_access_key.to_string(),
            })
        }
        (Some(_), Some(_)) => None,
        (Some(_), None) => None,
        (None, Some(_)) => None,
        (None, None) => None,
    };
    SigV4Verifier::new(credential)
}

/// Tower middleware (`from_fn_with_state` style) gating the S3 router.
pub async fn sigv4_auth(
    State(verifier): State<Arc<SigV4Verifier>>,
    mut request: Request,
    next: Next,
) -> Response {
    match verifier.authenticate_with_payload(&request, now_unix()) {
        Ok((auth, signed_payload)) => {
            request.extensions_mut().insert(auth);
            request
                .extensions_mut()
                .insert(SignedPayloadHash(signed_payload));
            next.run(request).await
        }
        Err(error) => error.into_response(),
    }
}

/// The SigV4-signed `x-amz-content-sha256` for a verified request, attached to
/// the request extensions by `sigv4_auth``.
#[derive(Clone)]
pub struct SignedPayloadHash(pub Option<String>);

impl SignedPayloadHash {
    /// Whether a concrete body hash was signed.
    pub fn is_verifiable(&self) -> bool {
        match self.0.as_deref() {
            Some(claimed) => {
                claimed != UNSIGNED_PAYLOAD && !claimed.starts_with(STREAMING_PAYLOAD_PREFIX)
            }
            None => false,
        }
    }

    /// Whether the body is `aws-chunked` framed (streaming SigV4), so it must be
    /// de-framed to recover the object bytes.
    pub fn is_aws_chunked(&self) -> bool {
        self.0
            .as_deref()
            .is_some_and(|claimed| claimed.starts_with(STREAMING_PAYLOAD_PREFIX))
    }
}

/// Confirm a write's received body hashes to the SigV4-signed
/// `x-amz-content-sha256`.
pub fn verify_signed_body(signed: &SignedPayloadHash, body: &[u8]) -> Result<(), S3Error> {
    if !signed.is_verifiable() {
        return Ok(());
    }

    // `is_verifiable` guarantees a concrete (non-sentinel) hash to compare against.
    if signed.0.as_deref() == Some(sha256_hex(body).as_str()) {
        Ok(())
    } else {
        Err(S3Error::ContentSha256Mismatch)
    }
}

/// The signature material parsed off a request, from either carrier
#[derive(Clone)]
struct PresentedSignature {
    /// Access key id from the credential scope
    access_key_id: String,
    /// Credential scope: `YYYYMMDD/region/service/aws4_request`
    scope: String,
    /// Request timestamp `YYYYMMDDTHHMMSSZ`
    date: String,
    /// Lowercased signed-header names
    signed_headers: Vec<String>,
    /// Hex-encoded signature the client presented
    signature: String,
    /// Payload hash used in the canonical request
    payload_hash: String,
    /// Whether this came from the presigned-query carrier
    is_presigned: bool,
    /// Presigned-URL lifetime in seconds (`X-Amz-Expires`)
    expires_secs: Option<u64>,
}

impl PresentedSignature {
    /// Extract SigV4 material, preferring the `Authorization` header and falling
    /// back to presigned-URL query parameters. Returns `Ok(None)` when the
    /// request carries no SigV4 material (an anonymous request).
    fn extract(request: &Request) -> Result<Option<Self>, S3Error> {
        if let Some(auth) = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        {
            if let Some(rest) = auth.strip_prefix(ALGORITHM) {
                return Ok(Some(parse_authorization_header(request, rest.trim_start())?));
            }
            // A non-SigV4 scheme (e.g. legacy SigV2) is treated as unsigned;
            // reads still pass, writes are denied. SigV2 is intentionally
            // unsupported (see docs/s3-gateway-status.md).
        }

        if let Some(query) = request.uri().query() {
            if query_param(query, AMZ_SIGNATURE_PARAM).is_some() {
                return Ok(Some(parse_presigned_query(request, query)?));
            }
        }

        Ok(None)
    }
}

/// Parse the comma-separated parameters following `AWS4-HMAC-SHA256` in an
/// `Authorization` header.
fn parse_authorization_header(
    request: &Request,
    params: &str,
) -> Result<PresentedSignature, S3Error> {
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for segment in params.split(',') {
        let Some((key, value)) = segment.trim().split_once('=') else {
            continue;
        };
        match key.trim() {
            "Credential" => credential = Some(value.trim().to_string()),
            "SignedHeaders" => signed_headers = Some(value.trim().to_string()),
            "Signature" => signature = Some(value.trim().to_string()),
            _ => {}
        }
    }

    let credential =
        credential.ok_or_else(|| invalid("missing Credential in Authorization header"))?;
    let signed_headers =
        signed_headers.ok_or_else(|| invalid("missing SignedHeaders in Authorization header"))?;
    let signature =
        signature.ok_or_else(|| invalid("missing Signature in Authorization header"))?;

    let (access_key_id, scope) = credential
        .split_once('/')
        .ok_or_else(|| invalid("malformed Credential scope"))?;

    let date = request
        .headers()
        .get(AMZ_DATE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| invalid("missing x-amz-date header"))?
        .to_string();

    // The canonical request uses the literal x-amz-content-sha256 value the
    // client signed (or the empty-payload hash for a bodyless request). For signed
    // writes the body is verified against this hash in the write handlers via
    // `verify_signed_body` (a buffered or streamed mismatch is rejected).
    let payload_hash = request
        .headers()
        .get(AMZ_CONTENT_SHA256)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| EMPTY_PAYLOAD_SHA256.to_string());

    Ok(PresentedSignature {
        access_key_id: access_key_id.to_string(),
        scope: scope.to_string(),
        date,
        signed_headers: split_signed_headers(&signed_headers),
        signature,
        payload_hash,
        is_presigned: false,
        expires_secs: None,
    })
}

/// Parse presigned-URL SigV4 query parameters
fn parse_presigned_query(request: &Request, query: &str) -> Result<PresentedSignature, S3Error> {
    let credential =
        query_param(query, "X-Amz-Credential").ok_or_else(|| invalid("missing X-Amz-Credential"))?;
    let signature = query_param(query, AMZ_SIGNATURE_PARAM)
        .ok_or_else(|| invalid("missing X-Amz-Signature"))?;
    let signed_headers = query_param(query, "X-Amz-SignedHeaders")
        .ok_or_else(|| invalid("missing X-Amz-SignedHeaders"))?;
    let date = query_param(query, "X-Amz-Date").ok_or_else(|| invalid("missing X-Amz-Date"))?;
    // `X-Amz-Expires` is the presigned-URL lifetime (seconds).
    let expires_secs = query_param(query, "X-Amz-Expires")
        .and_then(|raw| raw.parse::<u64>().ok());
    let _ = request;

    let (access_key_id, scope) = credential
        .split_once('/')
        .ok_or_else(|| invalid("malformed X-Amz-Credential scope"))?;

    Ok(PresentedSignature {
        access_key_id: access_key_id.to_string(),
        scope: scope.to_string(),
        date,
        signed_headers: split_signed_headers(&signed_headers),
        signature,
        payload_hash: UNSIGNED_PAYLOAD.to_string(),
        is_presigned: true,
        expires_secs,
    })
}

/// Split and lowercase a `;`-separated `SignedHeaders` list
fn split_signed_headers(list: &str) -> Vec<String> {
    list.split(';')
        .map(|header| header.trim().to_ascii_lowercase())
        .filter(|header| !header.is_empty())
        .collect()
}

/// Build the canonical headers block: `name:trimmed-value\n` for each signed
/// header, in the (already sorted) order of `signed_headers`.
fn canonical_headers(request: &Request, signed_headers: &[String]) -> Result<String, S3Error> {
    let mut out = String::new();
    for name in signed_headers {
        let value = header_value(request, name).ok_or(S3Error::SignatureDoesNotMatch)?;
        out.push_str(name);
        out.push(':');
        out.push_str(&trim_collapse(&value));
        out.push('\n');
    }
    Ok(out)
}

/// Resolve a signed header's value, special-casing `host` so it works whether
/// the client sent a `Host` header (HTTP/1.1) or only the URI authority.
fn header_value(request: &Request, name: &str) -> Option<String> {
    if let Some(value) = request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
    {
        return Some(value.to_string());
    }
    if name == "host" {
        return request.uri().authority().map(|authority| authority.to_string());
    }
    None
}

/// Build the canonical query string: each parameter decoded then AWS-URI-encoded,
/// sorted by encoded name (then value).
fn canonical_query_string(query: Option<&str>, is_presigned: bool) -> String {
    let Some(query) = query else {
        return String::new();
    };

    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = match part.split_once('=') {
            Some((key, value)) => (key, value),
            None => (part, ""),
        };
        let key = aws_uri_encode(&percent_decode(raw_key), true);
        if is_presigned && key == AMZ_SIGNATURE_PARAM {
            continue;
        }
        let value = aws_uri_encode(&percent_decode(raw_value), true);
        pairs.push((key, value));
    }

    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Look up a raw query parameter by exact (un-encoded) name, returning its
/// percent-decoded value.
pub fn query_param(query: &str, key: &str) -> Option<String> {
    for part in query.split('&') {
        let (found_key, found_value) = match part.split_once('=') {
            Some((found_key, found_value)) => (found_key, found_value),
            None => (part, ""),
        };
        if found_key == key {
            return Some(percent_decode(found_value));
        }
    }
    None
}

/// Whether an HTTP method is a public (anonymous-allowed) read
fn is_read_method(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD)
}

/// Derive the SigV4 signing key for the given scope
fn derive_signing_key(
    secret: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
) -> Result<[u8; 32], S3Error> {
    let signing_key_secret = format!("AWS4{secret}");
    let signing_key_date = hmac_sha256(signing_key_secret.as_bytes(), date_stamp.as_bytes())?;
    let signing_key_region = hmac_sha256(&signing_key_date, region.as_bytes())?;
    let signing_key_service = hmac_sha256(&signing_key_region, service.as_bytes())?;
    hmac_sha256(&signing_key_service, AWS4_REQUEST.as_bytes())
}

/// Compute `HMAC-SHA256(key, data)`.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], S3Error> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|error| S3Error::Internal(format!("hmac key init: {error}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().into())
}

/// Compute the lowercase hex `SHA-256(data)`
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Trim leading/trailing whitespace and collapse internal runs of spaces to a
/// single space, per the SigV4 header-value normalization rules.
fn trim_collapse(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut previous_space = false;
    for character in value.trim().chars() {
        if character == ' ' {
            if !previous_space {
                out.push(' ');
                previous_space = true;
            }
        } else {
            out.push(character);
            previous_space = false;
        }
    }
    out
}

/// AWS `UriEncode`: percent-encode every byte except the unreserved set
/// (`A-Z a-z 0-9 - . _ ~`). 
fn aws_uri_encode(value: &str, should_encode_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            b'/' if !should_encode_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(hex_upper(byte >> 4));
                out.push(hex_upper(byte & 0x0f));
            }
        }
    }
    out
}

/// Best-effort percent-decode. Invalid `%XX` sequences are passed through
/// unchanged. `+` is left as-is (S3 query strings encode spaces as `%20`, not
/// `+`), so this is reused by the S3 listing query parser.
pub fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                out.push((high << 4) | low);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Map a nibble to its uppercase hex digit
fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Parse a single hex ASCII digit
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Length-checked constant-time byte comparison
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut difference = 0u8;
    for (x, y) in a.iter().zip(b) {
        difference |= x ^ y;
    }
    difference == 0
}

/// Reject a signed request whose timestamp is stale, future-dated, or (for a
/// presigned URL) past its lifetime — the SigV4 replay / expiry window.
fn check_request_freshness(presented: &PresentedSignature, now: i64) -> Result<(), S3Error> {
    let signed_at = parse_amz_timestamp(&presented.date).ok_or_else(|| {
        S3Error::AccessDenied(format!(
            "request timestamp '{}' is not a valid ISO-8601 basic UTC instant",
            presented.date
        ))
    })?;

    if presented.is_presigned {
        let expires = presented.expires_secs.ok_or_else(|| {
            S3Error::AccessDenied("presigned URL is missing X-Amz-Expires".to_string())
        })?;
        // Compare as u64 so a value above i64::MAX cannot wrap negative and slip
        // the upper bound.
        if expires == 0 || expires > MAX_PRESIGNED_EXPIRY_SECS as u64 {
            return Err(S3Error::AccessDenied(format!(
                "presigned URL X-Amz-Expires {expires} is out of range (1..={MAX_PRESIGNED_EXPIRY_SECS})"
            )));
        }
        // Not yet valid: dated more than the skew window into the future.
        if signed_at - now > MAX_CLOCK_SKEW_SECS {
            return Err(S3Error::AccessDenied(
                "presigned URL is not yet valid (its timestamp is in the future)".to_string(),
            ));
        }
        // Expired: presented after its signing time plus its declared lifetime.
        // `expires` is now bounded by MAX_PRESIGNED_EXPIRY_SECS, so the add cannot
        // overflow.
        if now > signed_at + expires as i64 {
            return Err(S3Error::AccessDenied("presigned URL has expired".to_string()));
        }
    } else if (now - signed_at).abs() > MAX_CLOCK_SKEW_SECS {
        return Err(S3Error::AccessDenied(format!(
            "request timestamp is outside the allowed {MAX_CLOCK_SKEW_SECS}s window"
        )));
    }

    Ok(())
}

/// Parse an AWS `YYYYMMDDTHHMMSSZ` timestamp (ISO-8601 basic, always UTC) into
/// unix seconds.
fn parse_amz_timestamp(stamp: &str) -> Option<i64> {
    let bytes = stamp.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let field = |range: std::ops::Range<usize>| -> Option<i64> {
        let slice = stamp.get(range)?;
        if slice.bytes().all(|byte| byte.is_ascii_digit()) {
            slice.parse::<i64>().ok()
        } else {
            None
        }
    };
    let year = field(0..4)?;
    let month = field(4..6)?;
    let day = field(6..8)?;
    let hour = field(9..11)?;
    let minute = field(11..13)?;
    let second = field(13..15)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    Some(
        days_from_civil(year, month, day) * SECONDS_PER_DAY
            + hour * SECONDS_PER_HOUR
            + minute * SECONDS_PER_MINUTE
            + second,
    )
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian date, via
/// Howard Hinnant's branch-free `days_from_civil`. Valid for any in-range date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = year - era * 400; // [0, 399]
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1; // [0, 365]
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year; // [0, 146096]
    era * 146_097 + day_of_era - 719_468
}


/// Shorthand for an `InvalidRequest` S3 error with a static detail message
fn invalid(detail: &str) -> S3Error {
    S3Error::InvalidRequest(detail.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;

    /// Build a SigV4 signature over a fully-specified canonical request, mirroring
    /// the verifier's own derivation so the round-trip is self-consistent.
    fn sign(
        secret: &str,
        date: &str,
        scope: &str,
        canonical_request: &str,
    ) -> String {
        let parts: Vec<&str> = scope.split('/').collect();
        let string_to_sign = format!(
            "{ALGORITHM}\n{date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let key = derive_signing_key(secret, parts[0], parts[1], parts[2]).expect("derive signing key");
        hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()).expect("hmac"))
    }

    fn verifier() -> Arc<SigV4Verifier> {
        SigV4Verifier::new(Some(SigV4Credential {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
        }))
    }

    // Auth-only verification helpers; production drives `authenticate_with_payload`
    // (which also returns the body hash), so these live with the tests that use them.
    impl SigV4Verifier {
        fn authenticate(&self, request: &Request) -> Result<Auth, S3Error> {
            self.authenticate_at(request, now_unix())
        }

        fn authenticate_at(&self, request: &Request, now: i64) -> Result<Auth, S3Error> {
            self.authenticate_with_payload(request, now)
                .map(|(auth, _)| auth)
        }
    }

    // a body matching the signed hash verifies
    #[test]
    fn body_hash_match() {
        let body = b"hello world";
        let signed = SignedPayloadHash(Some(sha256_hex(body)));

        assert!(verify_signed_body(&signed, body).is_ok());
    }

    // a body that does not match the signed hash is rejected
    #[test]
    fn body_hash_mismatch() {
        let signed = SignedPayloadHash(Some(sha256_hex(b"expected")));

        let result = verify_signed_body(&signed, b"tampered");

        assert!(matches!(result, Err(S3Error::ContentSha256Mismatch)));
    }

    // the UNSIGNED-PAYLOAD sentinel skips body verification
    #[test]
    fn body_hash_unsigned() {
        let signed = SignedPayloadHash(Some(UNSIGNED_PAYLOAD.to_string()));

        assert!(verify_signed_body(&signed, b"anything").is_ok());
    }

    // a streaming-payload sentinel skips body verification
    #[test]
    fn body_hash_streaming() {
        let signed = SignedPayloadHash(Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_string()));

        assert!(verify_signed_body(&signed, b"chunked").is_ok());
    }

    // an absent signed hash (anonymous / presigned) skips body verification
    #[test]
    fn body_hash_absent() {
        let signed = SignedPayloadHash(None);

        assert!(verify_signed_body(&signed, b"anything").is_ok());
    }

    // an unsigned read is allowed (anonymous)
    #[test]
    fn anonymous_read() {
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .body(Body::empty())
            .expect("test setup");
        assert!(matches!(
            verifier().authenticate(&request),
            Ok(Auth::Anonymous)
        ));
    }

    // an unsigned write is denied
    #[test]
    fn anonymous_write() {
        let request = HttpRequest::builder()
            .method(Method::PUT)
            .uri("/bucket/key")
            .body(Body::empty())
            .expect("test setup");
        assert!(matches!(
            verifier().authenticate(&request),
            Err(S3Error::AccessDenied(_))
        ));
    }

    // a correctly header-signed request verifies
    #[test]
    fn header_signed() {
        let date = "20240101T000000Z";
        let scope = "20240101/us-east-1/s3/aws4_request";
        let host = "gateway.example.com";
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        // GET with no body -> empty-payload hash.
        let canonical_request = format!(
            "GET\n/bucket/key\n\nhost:{host}\nx-amz-content-sha256:{EMPTY_PAYLOAD_SHA256}\nx-amz-date:{date}\n\n{signed_headers}\n{EMPTY_PAYLOAD_SHA256}"
        );
        let signature = sign(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            date,
            scope,
            &canonical_request,
        );

        let authorization = format!(
            "{ALGORITHM} Credential=AKIDEXAMPLE/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        );
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("host", host)
            .header(AMZ_DATE, date)
            .header(AMZ_CONTENT_SHA256, EMPTY_PAYLOAD_SHA256)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .expect("test setup");

        // Inject a clock at the signing instant so the freshness window passes
        // deterministically regardless of when the test runs.
        let now = parse_amz_timestamp(date).expect("test setup");
        assert!(matches!(
            verifier().authenticate_at(&request, now),
            Ok(Auth::Verified(_))
        ));
    }

    // a wrong header signature is rejected
    #[test]
    fn header_bad() {
        let date = "20240101T000000Z";
        let scope = "20240101/us-east-1/s3/aws4_request";
        let authorization = format!(
            "{ALGORITHM} Credential=AKIDEXAMPLE/{scope}, SignedHeaders=host;x-amz-date, Signature=deadbeef"
        );
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("host", "gateway.example.com")
            .header(AMZ_DATE, date)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .expect("test setup");

        assert!(matches!(
            verifier().authenticate(&request),
            Err(S3Error::SignatureDoesNotMatch)
        ));
    }

    // a signed request with no configured credential is rejected
    #[test]
    fn no_credential() {
        let no_cred = SigV4Verifier::new(None);
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("host", "gateway.example.com")
            .header(AMZ_DATE, "20240101T000000Z")
            .header(
                header::AUTHORIZATION,
                format!(
                    "{ALGORITHM} Credential=AKIDEXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef"
                ),
            )
            .body(Body::empty())
            .expect("test setup");

        assert!(matches!(
            no_cred.authenticate(&request),
            Err(S3Error::SignatureDoesNotMatch)
        ));
    }

    // a correctly presigned URL verifies
    #[test]
    fn presigned_ok() {
        let date = "20240101T000000Z";
        let scope = "20240101/us-east-1/s3/aws4_request";
        let host = "gateway.example.com";

        // Canonical query is sorted and excludes X-Amz-Signature; presigned
        // requests use the UNSIGNED-PAYLOAD sentinel.
        let canonical_query = format!(
            "X-Amz-Algorithm={ALGORITHM}\
             &X-Amz-Credential=AKIDEXAMPLE%2F20240101%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date={date}\
             &X-Amz-Expires=86400\
             &X-Amz-SignedHeaders=host"
        );
        let canonical_request = format!(
            "GET\n/bucket/key\n{canonical_query}\nhost:{host}\n\nhost\n{UNSIGNED_PAYLOAD}"
        );
        let signature = sign(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            date,
            scope,
            &canonical_request,
        );

        let uri = format!(
            "/bucket/key?X-Amz-Algorithm={ALGORITHM}\
             &X-Amz-Credential=AKIDEXAMPLE%2F20240101%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date={date}\
             &X-Amz-Expires=86400\
             &X-Amz-SignedHeaders=host\
             &X-Amz-Signature={signature}"
        );
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri(uri)
            .header("host", host)
            .body(Body::empty())
            .expect("test setup");

        // A clock one hour after signing is still well inside the 24h presigned
        // lifetime (X-Amz-Expires=86400).
        let now = parse_amz_timestamp(date).expect("test setup") + 3_600;
        assert!(matches!(
            verifier().authenticate_at(&request, now),
            Ok(Auth::Verified(_))
        ));
    }

    // AWS URI encoding matches the spec for slashes and spaces
    #[test]
    fn uri_encode() {
        assert_eq!(aws_uri_encode("a b/c~d", true), "a%20b%2Fc~d");
        assert_eq!(aws_uri_encode("a b/c~d", false), "a%20b/c~d");
    }

    // percent-decoding handles valid and malformed escapes
    #[test]
    fn percent_decoding() {
        assert_eq!(percent_decode("a%20b%2Fc"), "a b/c");
        assert_eq!(percent_decode("plain"), "plain");
        // A malformed trailing escape is passed through unchanged.
        assert_eq!(percent_decode("ab%2"), "ab%2");
    }

    // constant-time compare matches only equal-length equal bytes
    #[test]
    fn constant_time() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }

    /// The published `get-vanilla` case from AWS's official SigV4 test suite
    /// (`aws-sig-v4-test-suite`), which ships the exact canonical request, its
    /// SHA-256, the string-to-sign, and the final `Authorization` signature.
    ///
    /// Unlike the self-consistent round-trip tests above, this pins the gateway's
    /// intermediate canonical-request hash and final signature against AWS's
    /// *published, known-good* values, so a regression in the canonical-request
    /// assembly, its SHA-256, the signing-key derivation, or the HMAC is caught even
    /// though our own signer produced the comparison value. (Credentials are the
    /// suite's published `AKIDEXAMPLE` / `wJalr...` pair; service `service`, region
    /// `us-east-1`, date `20150830`.)
    #[test]
    fn aws_vector() {
        let access_key_id = "AKIDEXAMPLE";
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let date = "20150830T123600Z";
        let scope = "20150830/us-east-1/service/aws4_request";

        // The exact canonical request AWS publishes for get-vanilla (no body, so
        // the payload hash is the empty-payload SHA-256).
        assert_eq!(
            EMPTY_PAYLOAD_SHA256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let canonical_request = "GET\n\
            /\n\
            \n\
            host:example.amazonaws.com\n\
            x-amz-date:20150830T123600Z\n\
            \n\
            host;x-amz-date\n\
            e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        // AWS-published SHA-256 of that canonical request (get-vanilla.sts).
        assert_eq!(
            sha256_hex(canonical_request.as_bytes()),
            "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );

        // AWS-published final signature for get-vanilla (get-vanilla.authz).
        let expected_signature =
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31";
        assert_eq!(
            sign(secret, date, scope, canonical_request),
            expected_signature,
            "signing-key derivation + HMAC must reproduce AWS's published signature"
        );

        // End-to-end: the verifier must reconstruct the identical canonical request
        // from a real HTTP request and accept AWS's published signature. If the
        // gateway's canonical-request assembly diverged from AWS by even one byte,
        // the recomputed signature would not equal the published value below.
        let verifier = SigV4Verifier::new(Some(SigV4Credential {
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret.to_string(),
        }));
        let authorization = format!(
            "{ALGORITHM} Credential={access_key_id}/{scope}, \
             SignedHeaders=host;x-amz-date, Signature={expected_signature}"
        );
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/")
            .header("host", "example.amazonaws.com")
            .header(AMZ_DATE, date)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .expect("test setup");

        // The published vector is dated 2015; inject a clock at that instant so
        // the freshness window does not reject this known-good signature.
        let now = parse_amz_timestamp(date).expect("test setup");
        assert!(matches!(
            verifier.authenticate_at(&request, now),
            Ok(Auth::Verified(_))
        ));
    }

    // known instants parse to the right unix seconds
    #[test]
    fn timestamp_known() {
        // Epoch and small offsets, hand-verifiable.
        assert_eq!(parse_amz_timestamp("19700101T000000Z"), Some(0));
        assert_eq!(parse_amz_timestamp("19700101T000001Z"), Some(1));
        assert_eq!(parse_amz_timestamp("19700101T010000Z"), Some(3_600));
        assert_eq!(parse_amz_timestamp("19700102T000000Z"), Some(86_400));
        // 2024-01-01T00:00:00Z is 1_704_067_200 unix seconds.
        assert_eq!(parse_amz_timestamp("20240101T000000Z"), Some(1_704_067_200));
        // Monotonic across a leap-year boundary.
        let earlier = parse_amz_timestamp("20200228T235959Z").expect("test setup");
        let later = parse_amz_timestamp("20200229T000000Z").expect("test setup");
        assert_eq!(later - earlier, 1, "2020 is a leap year; Feb 29 exists");
    }

    // malformed timestamps are rejected
    #[test]
    fn timestamp_malformed() {
        for bad in [
            "",                  // empty
            "20240101000000Z",   // missing T
            "20240101T000000",   // missing Z
            "2024-01-01T00:00Z", // wrong shape
            "20241301T000000Z",  // month 13
            "20240132T000000Z",  // day 32
            "20240101T250000Z",  // hour 25
            "20240101T006000Z",  // minute 60
            "2024010aT000000Z",  // non-digit
        ] {
            assert_eq!(parse_amz_timestamp(bad), None, "should reject {bad:?}");
        }
    }

    /// A header-signed request built at `date`, used to exercise the freshness
    /// window at an injected `now`.
    fn signed_header_request(date: &str) -> Request {
        let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
        let host = "gateway.example.com";
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "GET\n/bucket/key\n\nhost:{host}\nx-amz-content-sha256:{EMPTY_PAYLOAD_SHA256}\nx-amz-date:{date}\n\n{signed_headers}\n{EMPTY_PAYLOAD_SHA256}"
        );
        let signature = sign(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            date,
            &scope,
            &canonical_request,
        );
        let authorization = format!(
            "{ALGORITHM} Credential=AKIDEXAMPLE/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        );
        HttpRequest::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("host", host)
            .header(AMZ_DATE, date)
            .header(AMZ_CONTENT_SHA256, EMPTY_PAYLOAD_SHA256)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .expect("test setup")
    }

    // a request inside the clock-skew window verifies
    #[test]
    fn within_skew() {
        let date = "20240101T120000Z";
        let signed_at = parse_amz_timestamp(date).expect("test setup");
        let request = signed_header_request(date);
        // Just inside the window on both sides.
        for now in [signed_at - MAX_CLOCK_SKEW_SECS, signed_at + MAX_CLOCK_SKEW_SECS] {
            assert!(matches!(
                verifier().authenticate_at(&request, now),
                Ok(Auth::Verified(_))
            ));
        }
    }

    // a request past the replay window is denied
    #[test]
    fn stale_request() {
        let date = "20240101T120000Z";
        let signed_at = parse_amz_timestamp(date).expect("test setup");
        let request = signed_header_request(date);
        // One second past the replay window: a validly-signed-but-stale request.
        let now = signed_at + MAX_CLOCK_SKEW_SECS + 1;
        assert!(matches!(
            verifier().authenticate_at(&request, now),
            Err(S3Error::AccessDenied(_))
        ));
    }

    // a future-dated request beyond skew is denied
    #[test]
    fn future_request() {
        let date = "20240101T120000Z";
        let signed_at = parse_amz_timestamp(date).expect("test setup");
        let request = signed_header_request(date);
        // Dated further into the future than the skew tolerance.
        let now = signed_at - MAX_CLOCK_SKEW_SECS - 1;
        assert!(matches!(
            verifier().authenticate_at(&request, now),
            Err(S3Error::AccessDenied(_))
        ));
    }

    // a presigned URL past its lifetime is denied
    #[test]
    fn presigned_expired() {
        let presented = PresentedSignature {
            access_key_id: "AKIDEXAMPLE".to_string(),
            scope: "20240101/us-east-1/s3/aws4_request".to_string(),
            date: "20240101T000000Z".to_string(),
            signed_headers: vec!["host".to_string()],
            signature: "deadbeef".to_string(),
            payload_hash: UNSIGNED_PAYLOAD.to_string(),
            is_presigned: true,
            expires_secs: Some(3_600),
        };
        let signed_at = parse_amz_timestamp(&presented.date).expect("test setup");
        // One second past signed_at + expires.
        assert!(matches!(
            check_request_freshness(&presented, signed_at + 3_601),
            Err(S3Error::AccessDenied(_))
        ));
        // Inside the lifetime: accepted.
        assert!(check_request_freshness(&presented, signed_at + 3_599).is_ok());
    }

    // presigned freshness checks are fail-closed
    #[test]
    fn presigned_fail_closed() {
        let base = PresentedSignature {
            access_key_id: "AKIDEXAMPLE".to_string(),
            scope: "20240101/us-east-1/s3/aws4_request".to_string(),
            date: "20240101T000000Z".to_string(),
            signed_headers: vec!["host".to_string()],
            signature: "deadbeef".to_string(),
            payload_hash: UNSIGNED_PAYLOAD.to_string(),
            is_presigned: true,
            expires_secs: Some(3_600),
        };
        let signed_at = parse_amz_timestamp(&base.date).expect("test setup");

        // Missing X-Amz-Expires denies.
        let missing = PresentedSignature { expires_secs: None, ..base.clone() };
        assert!(matches!(
            check_request_freshness(&missing, signed_at),
            Err(S3Error::AccessDenied(_))
        ));
        // Zero / over-7-days expiry denies. The large values (above i64::MAX, and
        // 1<<63) would have wrapped negative past the bound and overflowed the
        // expiry add under a signed cast; they must deny cleanly.
        for bad in [0, MAX_PRESIGNED_EXPIRY_SECS as u64 + 1, 1u64 << 63, u64::MAX] {
            let out_of_range = PresentedSignature { expires_secs: Some(bad), ..base.clone() };
            assert!(matches!(
                check_request_freshness(&out_of_range, signed_at),
                Err(S3Error::AccessDenied(_))
            ));
        }
        // Dated far into the future (beyond skew) denies even before expiry.
        assert!(matches!(
            check_request_freshness(&base, signed_at - MAX_CLOCK_SKEW_SECS - 1),
            Err(S3Error::AccessDenied(_))
        ));
        // Malformed timestamp denies.
        let bad_date = PresentedSignature { date: "not-a-date".to_string(), ..base };
        assert!(matches!(
            check_request_freshness(&bad_date, signed_at),
            Err(S3Error::AccessDenied(_))
        ));
    }
}
