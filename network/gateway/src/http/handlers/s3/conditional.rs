//! Conditional-request evaluation for the S3 surface
//!
//! Reads the `If-Match` / `If-None-Match` headers off a request and weighs them
//! against the ETag the key currently holds.

use axum::http::{HeaderMap, HeaderName, header};

use tape_crypto::Hash;

use super::error::S3Error;

/// The conditional headers one request carries.
#[derive(Default)]
pub struct Preconditions {
    /// `If-Match` entity-tag list
    pub if_match: Option<String>,
    /// `If-None-Match` entity-tag list
    pub if_none_match: Option<String>,
}

impl Preconditions {
    /// Read the conditional headers off a request
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            if_match: header_text(headers, header::IF_MATCH),
            if_none_match: header_text(headers, header::IF_NONE_MATCH),
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
