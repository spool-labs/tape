//! Static site serving over name-addressed objects
//!
//! Serves a site with only a tape address and a path: the path is looked up
//! in the tape's object index and the backing track is served inline so the
//! browser renders it. Directory paths resolve the site's index page, misses
//! fall back to its 404 page, and a download query switches back to
//! attachment behavior.

use axum::Extension;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use rpc::Rpc;
use serde::Deserialize;
use store::Store;
use tape_core::types::ContentType;
use tape_crypto::address::Address;
use tape_protocol::Api;

use super::resolve::{ResolvedObject, resolve_object};
use crate::http::error::RouteError;
use crate::http::handlers::object::{
    CachePolicy, ObjectResponseMetadata, cache_control_header, range_header,
    read_object_response,
};
use crate::http::handlers::store_error;
use crate::http::handlers::track::{parse_address, track_with_pending};
use crate::http::state::AppState;
use crate::meter::{MeterCaller, rate_limited_response};

pub const SITE_ROOT_PATH: &str = "/site/{tape}";
pub const SITE_INDEX_PATH: &str = "/site/{tape}/";
pub const SITE_PATH: &str = "/site/{tape}/{*path}";

const INDEX_OBJECT: &str = "index.html";
const NOT_FOUND_OBJECT: &str = "404.html";

// Same-origin everywhere, with inline styles and scripts allowed: static
// sites routinely inline both, and the policy's job here is keeping the
// page from reaching other origins, not hardening the site against itself.
const SITE_CONTENT_SECURITY_POLICY: &str =
    "default-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'";

#[derive(Deserialize)]
pub struct SiteQuery {
    download: Option<String>,
}

/// Redirect the bare site URL to its slash form so relative asset links in
/// the served page resolve under the site prefix.
pub async fn get_site_root(Path(tape): Path<String>) -> Result<Response, RouteError> {
    parse_address(&tape, "tape address")?;
    Ok(Redirect::permanent(&format!("/site/{tape}/")).into_response())
}

/// Serve the site's index page for the bare slash form, which the wildcard
/// route cannot match because its path segment would be empty.
pub async fn get_site_index<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path(tape): Path<String>,
    Query(query): Query<SiteQuery>,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    serve_site(state, caller, &tape, "", query, headers).await
}

pub async fn get_site_object<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Path((tape, path)): Path<(String, String)>,
    Query(query): Query<SiteQuery>,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    serve_site(state, caller, &tape, &path, query, headers).await
}

async fn serve_site<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    state: AppState<Db, Cluster, Blockchain>,
    caller: MeterCaller,
    tape: &str,
    path: &str,
    query: SiteQuery,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    let tape = parse_address(tape, "tape address")?;
    let name = resolve_site_name(path);

    let (resolved, status) = match lookup(&state, tape, &name)? {
        Some(resolved) => (resolved, StatusCode::OK),
        None => match lookup_miss(&state, tape, &name)? {
            Some(fallback) => fallback,
            None => return Err(RouteError::NotFound),
        },
    };

    // A revalidation hit answers before any decode work happens; the etag is
    // formatted once for both the comparison and the 304 headers.
    if status == StatusCode::OK {
        let etag = resolved.etag.to_string();
        if matches_etag(&headers, &etag) {
            return not_modified(&etag);
        }
    }

    let track = track_with_pending(&state, resolved.track_address)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::NotFound);
    }

    let metadata = site_metadata(&resolved, &name, query.download.as_deref());
    read_object_response(
        state,
        resolved.track_address,
        track,
        metadata,
        status,
        &caller,
        range_header(&headers).map(str::to_string),
        rate_limited_response,
    )
    .await
}

/// Map a request path to an object name: directory paths get the index page.
fn resolve_site_name(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        return format!("{path}{INDEX_OBJECT}");
    }
    path.to_string()
}

fn lookup<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    name: &str,
) -> Result<Option<ResolvedObject>, RouteError> {
    resolve_object(state, tape, name).map_err(store_error)
}

/// Resolve what a missing path serves: the index page when the single-page
/// fallback is on, else the site's 404 page with a 404 status.
fn lookup_miss<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    name: &str,
) -> Result<Option<(ResolvedObject, StatusCode)>, RouteError> {
    // When the index itself was the miss, asking the store again cannot help.
    if state.context.config.gateway.site.spa_fallback && name != INDEX_OBJECT {
        if let Some(resolved) = lookup(state, tape, INDEX_OBJECT)? {
            return Ok(Some((resolved, StatusCode::OK)));
        }
    }
    if let Some(resolved) = lookup(state, tape, NOT_FOUND_OBJECT)? {
        return Ok(Some((resolved, StatusCode::NOT_FOUND)));
    }
    Ok(None)
}

/// Response metadata for a site object: rendered inline with revalidation
/// caching, or a named download when the query asks for one. An unknown
/// stored content type falls back to what the path extension implies.
fn site_metadata(
    resolved: &ResolvedObject,
    name: &str,
    download: Option<&str>,
) -> ObjectResponseMetadata {
    let content_type = match resolved.content_type {
        ContentType::Unknown => ContentType::from_extension(name_extension(name)),
        recorded => recorded,
    };

    let is_download = download == Some("1");
    ObjectResponseMetadata {
        content_type,
        filename: is_download.then(|| name_filename(name).as_bytes().to_vec()),
        cache: CachePolicy::Revalidate,
    }
}

fn name_extension(name: &str) -> &str {
    let filename = name_filename(name);
    match filename.rsplit_once('.') {
        Some((_, extension)) => extension,
        None => "",
    }
}

fn name_filename(name: &str) -> &str {
    match name.rsplit_once('/') {
        Some((_, filename)) => filename,
        None => name,
    }
}

/// Whether the request's If-None-Match covers the current ETag
fn matches_etag(headers: &HeaderMap, current: &str) -> bool {
    let Some(candidates) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    for candidate in candidates.split(',') {
        let candidate = candidate
            .trim()
            .trim_start_matches("W/")
            .trim_matches('"');
        if candidate == current || candidate == "*" {
            return true;
        }
    }
    false
}

fn not_modified(etag: &str) -> Result<Response, RouteError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{etag}\""))
            .map_err(|error| RouteError::Internal(format!("etag header: {error}")))?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        cache_control_header(CachePolicy::Revalidate),
    );
    Ok((StatusCode::NOT_MODIFIED, headers).into_response())
}

/// Stamp the site security headers on every site-route response: no MIME
/// sniffing, same-origin content policy. Path-based hosting does not isolate
/// tenants from each other; that takes per-site subdomains.
pub async fn site_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY),
    );
    response
}

#[cfg(test)]
mod tests {
    use tape_crypto::Hash;

    use super::*;

    fn resolved(etag: Hash) -> ResolvedObject {
        ResolvedObject {
            track_address: Address::new_unique(),
            size: 4,
            etag,
            block_time: None,
            content_type: ContentType::Unknown,
        }
    }

    // empty and directory paths resolve the index page, files pass through
    #[test]
    fn name_resolution() {
        assert_eq!(resolve_site_name(""), "index.html");
        assert_eq!(resolve_site_name("docs/"), "docs/index.html");
        assert_eq!(resolve_site_name("docs/guide.html"), "docs/guide.html");
    }

    // unknown stored types are inferred from the path extension
    #[test]
    fn extension_inference() {
        let metadata = site_metadata(&resolved(Hash::default()), "assets/app.css", None);
        assert_eq!(metadata.content_type, ContentType::TextCss);
        assert!(metadata.filename.is_none());
    }

    // the download query switches to a named attachment
    #[test]
    fn download_query() {
        let metadata = site_metadata(&resolved(Hash::default()), "docs/guide.html", Some("1"));
        assert_eq!(metadata.filename.as_deref(), Some(b"guide.html".as_slice()));
    }

    // if-none-match matches strong, weak, quoted, and wildcard forms
    #[test]
    fn etag_matching() {
        let etag = Hash::new_unique().to_string();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_str(&format!("\"{etag}\"")).expect("quoted etag header"),
        );
        assert!(matches_etag(&headers, &etag));

        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_str(&format!("W/\"{etag}\"")).expect("weak etag header"),
        );
        assert!(matches_etag(&headers, &etag));

        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        assert!(matches_etag(&headers, &etag));

        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("\"other\""));
        assert!(!matches_etag(&headers, &etag));
    }
}
