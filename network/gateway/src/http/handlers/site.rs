//! Static site serving over name-addressed objects
//!
//! Serves a site with only a tape address and a path: the path is looked up
//! in the tape's object index and the backing track is served inline so the
//! browser renders it. Directory paths resolve the site's index page, misses
//! fall back to its 404 page, and a download query switches back to
//! attachment behavior. A site is reachable under the path prefix, a
//! configured custom hostname, or its own subdomain of the configured
//! suffix, where each site gets an isolated browser origin.

use axum::Extension;
use axum::extract::{Path, Query, Request, State};
use axum::http::uri::PathAndQuery;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use rpc::Rpc;
use serde::Deserialize;
use store::Store;
use tape_core::types::ContentType;
use tape_crypto::address::Address;
use tape_node::config::gateway::GatewaySiteConfig;
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
    let tape = parse_address(&tape, "tape address")?;
    serve_site(state, caller, tape, "", query.download.as_deref(), &headers).await
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
    let tape = parse_address(&tape, "tape address")?;
    serve_site(state, caller, tape, &path, query.download.as_deref(), &headers).await
}

/// Rewrite site-host requests into the site route before routing: when the
/// Host header maps to a tape, the whole request is that site's, from the
/// domain root, and it flows through the same routed pipeline as the path
/// form, readiness, metering, headers, and all.
pub async fn host_site_serving<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    mut req: Request,
    next: Next,
) -> Response {
    let site = &state.context.config.gateway.site;
    if site.domains.is_empty() && site.subdomain_suffix.is_none() {
        return next.run(req).await;
    }

    let tape = req
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|host| host_tape(site, host));
    let Some(tape) = tape else {
        return next.run(req).await;
    };

    match site_uri(tape, req.uri()) {
        Some(uri) => *req.uri_mut() = uri,
        None => return RouteError::BadRequest("unroutable site path".into()).into_response(),
    }
    next.run(req).await
}

/// The site-route form of a host-served request, query included.
fn site_uri(tape: Address, uri: &Uri) -> Option<Uri> {
    let path_and_query = uri.path_and_query().map_or("/", PathAndQuery::as_str);
    format!("/site/{tape}{path_and_query}").parse().ok()
}

async fn serve_site<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    state: AppState<Db, Cluster, Blockchain>,
    caller: MeterCaller,
    tape: Address,
    path: &str,
    download: Option<&str>,
    headers: &HeaderMap,
) -> Result<Response, RouteError> {
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
        if matches_etag(headers, &etag) {
            return not_modified(&etag);
        }
    }

    let track = track_with_pending(&state, resolved.track_address)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::NotFound);
    }

    let metadata = site_metadata(&resolved, &name, download);
    read_object_response(
        state,
        resolved.track_address,
        track,
        metadata,
        status,
        &caller,
        range_header(headers).map(str::to_string),
        rate_limited_response,
    )
    .await
}

/// The tape a Host header serves as a site: a configured custom domain, or a
/// subdomain label under the configured suffix. Config keys and the suffix
/// are canonicalized at load, so only an uppercase request host allocates.
pub fn host_tape(config: &GatewaySiteConfig, host: &str) -> Option<Address> {
    // A port suffix never survives into domain matching. IPv6 literal hosts
    // mangle here, but they can never match a configured domain anyway.
    let host = host.rsplit_once(':').map_or(host, |(name, _)| name);
    if host.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return lookup_host(config, &host.to_ascii_lowercase());
    }
    lookup_host(config, host)
}

fn lookup_host(config: &GatewaySiteConfig, host: &str) -> Option<Address> {
    if let Some(tape) = config.domains.get(host) {
        return Some(*tape);
    }

    let suffix = config.subdomain_suffix.as_deref()?;
    let label = host.strip_suffix(suffix)?.strip_suffix('.')?;
    if label.contains('.') {
        return None;
    }
    Address::try_from_subdomain_label(label)
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

/// Stamp the site headers on every site response: no MIME sniffing, the
/// same-origin content policy, and the cross-origin allowance when one is
/// configured. Path-based hosting does not isolate tenants from each other;
/// host-based serving gives each site its own origin.
pub async fn site_response_headers<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let site = &state.context.config.gateway.site;
    let cors = cors_origin(site, req.headers());
    let policy = site_content_security_policy(site);
    let mut response = next.run(req).await;

    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, policy);
    if let Some(origin) = cors {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(header::VARY, HeaderValue::from_static("origin"));
    }
    response
}

/// The content security policy for site responses: same-origin plus the
/// configured API origins hosted pages may call from the browser.
fn site_content_security_policy(config: &GatewaySiteConfig) -> HeaderValue {
    if config.connect_origins.is_empty() {
        return HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY);
    }

    let origins = config.connect_origins.join(" ");
    let policy = format!("{SITE_CONTENT_SECURITY_POLICY}; connect-src 'self' {origins}");
    // Config validation keeps origins header-safe; fall back to the closed
    // policy rather than fail the response if something slips through.
    HeaderValue::from_str(&policy)
        .unwrap_or_else(|_| HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY))
}

/// The Access-Control-Allow-Origin value a request earns, when cross-origin
/// site reads are configured.
fn cors_origin(config: &GatewaySiteConfig, headers: &HeaderMap) -> Option<HeaderValue> {
    if config.cors_origins.iter().any(|origin| origin == "*") {
        return Some(HeaderValue::from_static("*"));
    }

    let origin = headers.get(header::ORIGIN)?.to_str().ok()?;
    if config
        .cors_origins
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(origin))
    {
        return HeaderValue::from_str(origin).ok();
    }
    None
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
    fn download_attachment() {
        let metadata = site_metadata(&resolved(Hash::default()), "docs/guide.html", Some("1"));
        assert_eq!(metadata.filename.as_deref(), Some(b"guide.html".as_slice()));
    }

    // configured domains and subdomain labels map hosts to tapes
    #[test]
    fn host_mapping() {
        let tape = Address::new_unique();
        let mut config = GatewaySiteConfig::default();
        config.domains.insert("mysite.test".to_string(), tape);
        config.subdomain_suffix = Some("sites.test".to_string());

        assert_eq!(host_tape(&config, "mysite.test"), Some(tape));
        assert_eq!(host_tape(&config, "MySite.Test:8080"), Some(tape));
        assert_eq!(host_tape(&config, "other.test"), None);

        let label = tape.to_subdomain_label();
        assert_eq!(host_tape(&config, &format!("{label}.sites.test")), Some(tape));
        assert_eq!(host_tape(&config, &format!("{label}.sites.test:443")), Some(tape));
        assert_eq!(host_tape(&config, &format!("{label}.other.test")), None);
        assert_eq!(host_tape(&config, &format!("a.{label}.sites.test")), None);
        assert_eq!(host_tape(&config, "sites.test"), None);
    }

    // connect origins extend the policy; an empty list keeps it closed
    #[test]
    fn connect_policy() {
        let mut config = GatewaySiteConfig::default();
        assert_eq!(
            site_content_security_policy(&config),
            HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY)
        );

        config.connect_origins = vec![
            "https://api.devnet.solana.com".to_string(),
            "wss://api.devnet.solana.com".to_string(),
        ];
        let policy = site_content_security_policy(&config);
        let policy = policy.to_str().expect("policy is ascii");
        assert!(policy.starts_with(SITE_CONTENT_SECURITY_POLICY));
        assert!(policy.ends_with(
            "connect-src 'self' https://api.devnet.solana.com wss://api.devnet.solana.com"
        ));
    }

    // cors answers the wildcard or a listed origin, and nothing else
    #[test]
    fn cors_matching() {
        let mut config = GatewaySiteConfig::default();
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_static("https://app.example"));

        assert!(cors_origin(&config, &headers).is_none());

        config.cors_origins = vec!["https://app.example".to_string()];
        assert_eq!(
            cors_origin(&config, &headers),
            Some(HeaderValue::from_static("https://app.example"))
        );
        assert!(cors_origin(&config, &HeaderMap::new()).is_none());

        config.cors_origins = vec!["*".to_string()];
        assert_eq!(
            cors_origin(&config, &HeaderMap::new()),
            Some(HeaderValue::from_static("*"))
        );
    }

    // host requests rewrite into the site route with path and query intact
    #[test]
    fn host_rewrite() {
        let tape = Address::new_unique();

        let uri: Uri = "/assets/app.css?download=1".parse().expect("parse uri");
        assert_eq!(
            site_uri(tape, &uri).expect("rewrite uri").to_string(),
            format!("/site/{tape}/assets/app.css?download=1")
        );

        let root: Uri = "/".parse().expect("parse root uri");
        assert_eq!(
            site_uri(tape, &root).expect("rewrite root").to_string(),
            format!("/site/{tape}/")
        );
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
