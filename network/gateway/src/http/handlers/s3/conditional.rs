//! Conditional-request evaluation for the S3 surface
//!
//! Reads the `If-Match` / `If-None-Match` / `If-Modified-Since` /
//! `If-Unmodified-Since` headers off a request and weighs them against the ETag
//! and last-modified second the key currently holds.

use axum::http::{HeaderMap, HeaderName, header};

use tape_crypto::Hash;

use super::clock::{
    SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, days_from_civil, month_number,
};
use super::error::S3Error;

/// Length of the date part of an IMF-fixdate, `06 Nov 1994 08:49:37`
const FIXDATE_LEN: usize = 20;

/// What a conditional read serves.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadCondition {
    /// The conditions hold; serve the object
    Serve,
    /// The client already has this copy; answer 304
    NotModified,
    /// A condition did not hold; answer 412
    Failed,
}

/// The conditional headers one request carries.
#[derive(Default)]
pub struct Preconditions {
    /// `If-Match` entity-tag list
    pub if_match: Option<String>,
    /// `If-None-Match` entity-tag list
    pub if_none_match: Option<String>,

    /// `If-Modified-Since` as unix seconds
    pub if_modified_since: Option<i64>,
    /// `If-Unmodified-Since` as unix seconds
    pub if_unmodified_since: Option<i64>,
}

impl Preconditions {
    /// Read the conditional headers off a request; an unreadable date is ignored, as RFC 7232 requires
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            if_match: header_text(headers, header::IF_MATCH),
            if_none_match: header_text(headers, header::IF_NONE_MATCH),
            if_modified_since: header_text(headers, header::IF_MODIFIED_SINCE)
                .as_deref()
                .and_then(parse_http_date),
            if_unmodified_since: header_text(headers, header::IF_UNMODIFIED_SINCE)
                .as_deref()
                .and_then(parse_http_date),
        }
    }

    /// Whether an entity-tag condition is present at all
    pub fn has_tag_condition(&self) -> bool {
        self.if_match.is_some() || self.if_none_match.is_some()
    }
}

/// Weigh a write's entity-tag conditions against what the key currently holds
pub fn check_write(preconditions: &Preconditions, current: Option<Hash>) -> Result<(), S3Error> {
    if let Some(header) = &preconditions.if_match {
        if !list_matches(header, current) {
            return Err(S3Error::PreconditionFailed);
        }
    }
    if let Some(header) = &preconditions.if_none_match {
        if list_matches(header, current) {
            return Err(S3Error::PreconditionFailed);
        }
    }
    Ok(())
}

/// Weigh a read's conditions in RFC 7232 order, where the tag headers win over the dates
pub fn check_read(
    preconditions: &Preconditions,
    etag: Hash,
    last_modified: Option<i64>,
) -> ReadCondition {
    if is_failed(preconditions, etag, last_modified) {
        return ReadCondition::Failed;
    }
    if is_not_modified(preconditions, etag, last_modified) {
        return ReadCondition::NotModified;
    }
    ReadCondition::Serve
}

/// Whether `If-Match` or, in its absence, `If-Unmodified-Since` refuses the read
fn is_failed(preconditions: &Preconditions, etag: Hash, last_modified: Option<i64>) -> bool {
    match (&preconditions.if_match, preconditions.if_unmodified_since) {
        (Some(header), _) => !list_matches(header, Some(etag)),
        (None, Some(since)) => last_modified.is_some_and(|modified| modified > since),
        (None, None) => false,
    }
}

/// Whether `If-None-Match` or, in its absence, `If-Modified-Since` says the client's copy is current
fn is_not_modified(preconditions: &Preconditions, etag: Hash, last_modified: Option<i64>) -> bool {
    match (&preconditions.if_none_match, preconditions.if_modified_since) {
        (Some(header), _) => list_matches(header, Some(etag)),
        (None, Some(since)) => last_modified.is_some_and(|modified| modified <= since),
        (None, None) => false,
    }
}

/// Whether an entity-tag list matches the object's ETag; `*` matches any object that exists
fn list_matches(header: &str, current: Option<Hash>) -> bool {
    let header = header.trim();
    if header == "*" {
        return current.is_some();
    }
    let Some(current) = current else {
        return false;
    };
    let current = current.to_string();
    for candidate in header.split(',') {
        if tag_matches(candidate, &current) {
            return true;
        }
    }
    false
}

/// Whether one list entry equals `current`; the gateway only ever serves strong tags, so `W/` never matches
fn tag_matches(candidate: &str, current: &str) -> bool {
    let candidate = candidate.trim();
    if candidate.starts_with("W/") {
        return false;
    }
    unquote(candidate) == current
}

/// Strip the surrounding quotes from an entity tag
fn unquote(tag: &str) -> &str {
    tag.strip_prefix('"')
        .and_then(|tag| tag.strip_suffix('"'))
        .unwrap_or(tag)
}

/// One header's value as text, when present and readable
fn header_text(headers: &HeaderMap, name: HeaderName) -> Option<String> {
    let value = headers.get(name)?;
    value.to_str().ok().map(str::to_string)
}

/// Parse an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) as unix seconds
fn parse_http_date(value: &str) -> Option<i64> {
    let (_, rest) = value.trim().split_once(", ")?;
    if rest.len() < FIXDATE_LEN {
        return None;
    }

    let day = date_field(rest, 0, 2)?;
    let month = month_number(rest.get(3..6)?)?;
    let year = date_field(rest, 7, 11)?;
    let hour = date_field(rest, 12, 14)?;
    let minute = date_field(rest, 15, 17)?;
    let second = date_field(rest, 18, FIXDATE_LEN)?;
    if !(1..=31).contains(&day) || hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    Some(
        days_from_civil(year, month, day) * SECONDS_PER_DAY
            + hour * SECONDS_PER_HOUR
            + minute * SECONDS_PER_MINUTE
            + second,
    )
}

/// One fixed-width numeric field of an IMF-fixdate
fn date_field(rest: &str, start: usize, end: usize) -> Option<i64> {
    rest.get(start..end)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn etag() -> Hash {
        Hash([7u8; 32])
    }

    fn other() -> Hash {
        Hash([8u8; 32])
    }

    fn quoted(hash: Hash) -> String {
        format!("\"{hash}\"")
    }

    fn headers(pairs: &[(HeaderName, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(name, HeaderValue::from_str(value).expect("header value"));
        }
        headers
    }

    fn if_none_match(value: &str) -> Preconditions {
        Preconditions::from_headers(&headers(&[(header::IF_NONE_MATCH, value)]))
    }

    fn if_match(value: &str) -> Preconditions {
        Preconditions::from_headers(&headers(&[(header::IF_MATCH, value)]))
    }

    // a star only admits a write when the key holds nothing
    #[test]
    fn star_absent() {
        assert!(check_write(&if_none_match("*"), None).is_ok());
        assert!(check_write(&if_none_match("*"), Some(etag())).is_err());
    }

    // if-match admits a write only against the same etag
    #[test]
    fn match_current() {
        assert!(check_write(&if_match(&quoted(etag())), Some(etag())).is_ok());
        assert!(check_write(&if_match(&quoted(other())), Some(etag())).is_err());
        assert!(check_write(&if_match(&quoted(etag())), None).is_err());
    }

    // if-match on a star needs an object to be there
    #[test]
    fn match_star() {
        assert!(check_write(&if_match("*"), Some(etag())).is_ok());
        assert!(check_write(&if_match("*"), None).is_err());
    }

    // an unquoted tag is accepted, a weak one never matches
    #[test]
    fn weak_rejected() {
        let bare = etag().to_string();
        assert!(check_write(&if_match(&bare), Some(etag())).is_ok());
        assert!(check_write(&if_match(&format!("W/{}", quoted(etag()))), Some(etag())).is_err());
        assert!(check_write(&if_none_match(&format!("W/{}", quoted(etag()))), Some(etag())).is_ok());
    }

    // if-none-match on a tag admits a write only when the object differs
    #[test]
    fn none_match_tag() {
        assert!(check_write(&if_none_match(&quoted(other())), Some(etag())).is_ok());
        assert!(check_write(&if_none_match(&quoted(etag())), Some(etag())).is_err());
        assert!(check_write(&if_none_match(&quoted(etag())), None).is_ok());
    }

    // a list matches on any of its entries
    #[test]
    fn tag_list() {
        let list = format!("{}, {}", quoted(other()), quoted(etag()));
        assert!(check_write(&if_match(&list), Some(etag())).is_ok());
        assert!(check_write(&if_none_match(&list), Some(etag())).is_err());
    }

    // an unconditional request carries no tag condition
    #[test]
    fn no_condition() {
        let preconditions = Preconditions::from_headers(&HeaderMap::new());

        assert!(!preconditions.has_tag_condition());
        assert!(check_write(&preconditions, Some(etag())).is_ok());
    }

    // a mismatch is the 412 the S3 surface answers with
    #[test]
    fn failure_status() {
        let refused = check_write(&if_none_match("*"), Some(etag()));

        assert!(matches!(refused, Err(S3Error::PreconditionFailed)));
    }
}
