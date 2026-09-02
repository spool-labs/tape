//! Static site serving over name-addressed objects
//!
//! Serves a site with only a tape address and a path: the path is looked up
//! in the tape's object index and the backing track is served inline so the
//! browser renders it. Directory paths resolve the site's index page, misses
//! fall back to its 404 page, and a download query switches back to
//! attachment behavior. A site is reachable under the path prefix, a
//! configured custom hostname, or its own subdomain of the configured
//! suffix, where each site gets an isolated browser origin. The well-known
//! namespace is the gateway's own on every served host and never comes
//! from tape content.

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

use super::policy::{TapeSitePolicy, tape_site_policy};
use crate::http::error::RouteError;
use crate::http::handlers::object::{
    CachePolicy, DEFAULT_SITE_MAX_AGE_SECS, ObjectResponseMetadata, cache_control_header,
    range_header, read_object_response,
};
use crate::http::handlers::object::response::object_response_ranged;
use crate::http::handlers::resolve::{Readable, resolve_readable};
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
    Extension(policy): Extension<TapeSitePolicy>,
    Path(tape): Path<String>,
    Query(query): Query<SiteQuery>,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    let tape = parse_address(&tape, "tape address")?;
    serve_site(state, caller, &policy, tape, "", query.download.as_deref(), &headers).await
}

pub async fn get_site_object<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Extension(caller): Extension<MeterCaller>,
    Extension(policy): Extension<TapeSitePolicy>,
    Path((tape, path)): Path<(String, String)>,
    Query(query): Query<SiteQuery>,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    let tape = parse_address(&tape, "tape address")?;
    serve_site(state, caller, &policy, tape, &path, query.download.as_deref(), &headers).await
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
    if site.domains.is_empty() && site.subdomain_suffix.is_none() && state.site_hosts.is_none() {
        return next.run(req).await;
    }

    // The well-known namespace never rewrites into a site. A hosted site
    // answering ACME challenges or domain-verification probes on a served
    // hostname could prove control of a shared or operator-owned domain and
    // pull certificates for it the day TLS terminates at the gateway.
    if is_well_known(req.uri().path()) {
        return next.run(req).await;
    }

    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    let mut tape = host.and_then(|host| host_tape(site, host));
    if tape.is_none() {
        if let (Some(host), Some(bindings)) = (host, &state.site_hosts) {
            tape = bindings.tape_for_host(strip_port(host)).await;
        }
    }
    let Some(tape) = tape else {
        return next.run(req).await;
    };

    match site_uri(tape, req.uri()) {
        Some(uri) => *req.uri_mut() = uri,
        None => return RouteError::BadRequest("unroutable site path".into()).into_response(),
    }
    next.run(req).await
}

/// Whether a path sits in the well-known namespace.
fn is_well_known(path: &str) -> bool {
    path == "/.well-known" || path.starts_with("/.well-known/")
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
    policy: &TapeSitePolicy,
    tape: Address,
    path: &str,
    download: Option<&str>,
    headers: &HeaderMap,
) -> Result<Response, RouteError> {
    let name = resolve_site_name(path);
    let config = &state.context.config.gateway.site;
    let is_spa = policy.spa_fallback.unwrap_or(config.spa_fallback);
    let max_age_secs = policy.max_age_secs.unwrap_or(DEFAULT_SITE_MAX_AGE_SECS);

    let (readable, status, served_name): (Readable, StatusCode, &str) =
        match lookup(&state, tape, &name)? {
            Some(readable) => (readable, StatusCode::OK, name.as_str()),
            None => {
                // Every static host redirects a directory path to its slash form,
                // and Astro's default build makes half a site's routes look
                // like this.
                if let Some(redirect) = directory_redirect(&state, tape, path)? {
                    return Ok(redirect);
                }
                match lookup_miss(&state, tape, &name, is_spa)? {
                    Some((readable, status, served)) => (readable, status, served),
                    None => return Err(RouteError::NotFound),
                }
            }
        };

    // A revalidation hit answers before any decode work happens; the etag is
    // formatted once for both the comparison and the 304 headers.
    if status == StatusCode::OK {
        let etag = readable.etag().to_string();
        if matches_etag(headers, &etag) {
            return not_modified(&etag, max_age_secs);
        }
    }

    // Fallback pages take their type and download name from the object served,
    // not the requested path, so a missing /route never mislabels index.html.
    let metadata = site_metadata(readable.content_type(), served_name, download, max_age_secs);
    let resolved = match readable {
        Readable::Queued(object) => {
            let bytes = staged_bytes(&state, tape, &name)?.ok_or(RouteError::NotFound)?;
            return object_response_ranged(
                bytes,
                &metadata,
                object.etag,
                range_header(headers),
                status,
            )
            .map_err(RouteError::from);
        }
        Readable::Track(resolved) => resolved,
    };

    let track = track_with_pending(&state, resolved.track_address)?.ok_or(RouteError::NotFound)?;
    if !track.is_certified() {
        return Err(RouteError::NotFound);
    }

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

/// A redirect to the slash form when `path` names a directory whose index page
/// exists, which the browser needs before relative links in that page resolve.
fn directory_redirect<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    path: &str,
) -> Result<Option<Response>, RouteError> {
    let Some(name) = directory_index_name(path) else {
        return Ok(None);
    };
    if lookup(state, tape, &name)?.is_none() {
        return Ok(None);
    }
    let Some(location) = slash_location(path) else {
        return Ok(None);
    };
    Ok(Some(
        (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, location)]).into_response(),
    ))
}

/// The index page a slashless path would serve as a directory, if it is one.
fn directory_index_name(path: &str) -> Option<String> {
    if path.is_empty() || path.ends_with('/') {
        return None;
    }
    Some(format!("{path}/{INDEX_OBJECT}"))
}

/// The slash form as a relative location, so it resolves correctly under the
/// path prefix and under a site host alike.
fn slash_location(path: &str) -> Option<HeaderValue> {
    let segment = path.rsplit('/').next().unwrap_or(path);
    HeaderValue::try_from(format!("{segment}/")).ok()
}

/// The tape a Host header serves as a site: a configured custom domain, or a
/// subdomain label under the configured suffix. Config keys and the suffix
/// are canonicalized at load, so only an uppercase request host allocates.
pub fn host_tape(config: &GatewaySiteConfig, host: &str) -> Option<Address> {
    let host = strip_port(host);
    if host.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return lookup_host(config, &host.to_ascii_lowercase());
    }
    lookup_host(config, host)
}

/// A port suffix never survives into domain matching. IPv6 literal hosts
/// mangle here, but they can never match a configured domain anyway.
fn strip_port(host: &str) -> &str {
    host.rsplit_once(':').map_or(host, |(name, _)| name)
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

/// Resolve a site object name, the write queue first, so a page overwritten a
/// moment ago serves its new bytes rather than the index's older row.
fn lookup<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    name: &str,
) -> Result<Option<Readable>, RouteError> {
    resolve_readable(
        state.context.store.as_ref(),
        state.staging.as_ref(),
        tape,
        name.as_bytes(),
    )
    .map_err(store_error)
}

/// The queued bytes behind a `Readable::Queued`.
fn staged_bytes<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    name: &str,
) -> Result<Option<Vec<u8>>, RouteError> {
    state.staging.bytes(tape, name.as_bytes()).map_err(store_error)
}

/// Resolve what a missing path serves: the index page when the single-page
/// fallback is on, else the site's 404 page with a 404 status.
fn lookup_miss<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    name: &str,
    is_spa: bool,
) -> Result<Option<(Readable, StatusCode, &'static str)>, RouteError> {
    // When the index itself was the miss, asking the store again cannot help.
    if is_spa && name != INDEX_OBJECT {
        if let Some(readable) = lookup(state, tape, INDEX_OBJECT)? {
            return Ok(Some((readable, StatusCode::OK, INDEX_OBJECT)));
        }
    }
    if let Some(readable) = lookup(state, tape, NOT_FOUND_OBJECT)? {
        return Ok(Some((readable, StatusCode::NOT_FOUND, NOT_FOUND_OBJECT)));
    }
    Ok(None)
}

/// Response metadata for a site object: rendered inline with revalidation
/// caching, or a named download when the query asks for one. An unknown
/// stored content type falls back to what the path extension implies.
fn site_metadata(
    recorded: ContentType,
    name: &str,
    download: Option<&str>,
    max_age_secs: u64,
) -> ObjectResponseMetadata {
    let content_type = match recorded {
        ContentType::Unknown => ContentType::from_extension(name_extension(name)),
        recorded => recorded,
    };

    let is_download = download == Some("1");
    ObjectResponseMetadata {
        content_type,
        filename: is_download.then(|| name_filename(name).as_bytes().to_vec()),
        cache: CachePolicy::Revalidate { max_age_secs },
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

fn not_modified(etag: &str, max_age_secs: u64) -> Result<Response, RouteError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{etag}\""))
            .map_err(|error| RouteError::Internal(format!("etag header: {error}")))?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        cache_control_header(CachePolicy::Revalidate { max_age_secs }),
    );
    Ok((StatusCode::NOT_MODIFIED, headers).into_response())
}

/// Stamp the site headers on every site response: no MIME sniffing, the
/// content policy, and the cross-origin allowance when one applies. The
/// tape's own policy is read here, once per request, and handed to the
/// handler through the request extensions. Path-based hosting does not
/// isolate tenants from each other; host-based serving gives each site its
/// own origin.
pub async fn site_response_headers<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    mut req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let site = &state.context.config.gateway.site;
    let policy = match path_tape(req.uri().path()) {
        Some(tape) => tape_site_policy(&state, tape),
        None => TapeSitePolicy::default(),
    };

    let cors_list = policy.cors_origins.as_deref().unwrap_or(&site.cors_origins);
    let cors = cors_origin(cors_list, req.headers());
    let vary_on_origin = varies_on_origin(cors_list);
    let connect_list = policy
        .connect_origins
        .as_deref()
        .unwrap_or(&site.connect_origins);
    let content_policy = site_content_security_policy(connect_list);

    req.extensions_mut().insert(policy);
    let mut response = next.run(req).await;

    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, content_policy);
    if let Some(origin) = cors {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    // A selective allow list answers some origins and not others, so a shared
    // cache must key on Origin whether or not this request matched one.
    if vary_on_origin {
        headers.insert(header::VARY, HeaderValue::from_static("origin"));
    }
    // Text responses declare utf-8 so browsers do not guess the encoding;
    // with nosniff set, a wrong or missing charset is never recovered.
    if let Some(typed) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(text_charset)
    {
        if let Ok(value) = HeaderValue::from_str(&typed) {
            headers.insert(header::CONTENT_TYPE, value);
        }
    }
    response
}

/// The tape a site-route path addresses, present on every site request
/// because host serving rewrites into the same route shape.
fn path_tape(path: &str) -> Option<Address> {
    let rest = path.strip_prefix("/site/")?;
    let label = rest.split('/').next()?;
    label.parse().ok()
}

/// The content security policy for site responses: same-origin plus the
/// API origins the effective policy lets hosted pages call.
fn site_content_security_policy(connect_origins: &[String]) -> HeaderValue {
    if connect_origins.is_empty() {
        return HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY);
    }

    let origins = connect_origins.join(" ");
    let policy = format!("{SITE_CONTENT_SECURITY_POLICY}; connect-src 'self' {origins}");
    // Origins are validated at config load and sanitized when tenant
    // supplied; fall back to the closed policy rather than fail a response.
    HeaderValue::from_str(&policy)
        .unwrap_or_else(|_| HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY))
}

/// The Access-Control-Allow-Origin value a request earns from the
/// effective allow list.
fn cors_origin(allowed: &[String], headers: &HeaderMap) -> Option<HeaderValue> {
    if allowed.iter().any(|origin| origin == "*") {
        return Some(HeaderValue::from_static("*"));
    }

    let origin = headers.get(header::ORIGIN)?.to_str().ok()?;
    if allowed.iter().any(|entry| entry.eq_ignore_ascii_case(origin)) {
        return HeaderValue::from_str(origin).ok();
    }
    None
}

/// Whether the allow list makes a response depend on the request Origin. A
/// specific list answers some origins and not others, so shared caches must
/// key on Origin; a wildcard or empty list answers everyone alike.
fn varies_on_origin(allowed: &[String]) -> bool {
    !allowed.is_empty() && !allowed.iter().any(|origin| origin == "*")
}

/// The content type with utf-8 appended, when it is a text-family type that
/// carries no charset yet. Returns None to leave the header untouched.
fn text_charset(content_type: &str) -> Option<String> {
    if content_type.to_ascii_lowercase().contains("charset") {
        return None;
    }
    let media = content_type.split(';').next().unwrap_or("").trim();
    let textual = media.starts_with("text/")
        || media.eq_ignore_ascii_case("image/svg+xml")
        || media.eq_ignore_ascii_case("application/xml");
    textual.then(|| format!("{content_type}; charset=utf-8"))
}


#[cfg(test)]
mod tests {
    use tape_crypto::Hash;

    use super::*;

    // empty and directory paths resolve the index page, files pass through
    #[test]
    fn name_resolution() {
        assert_eq!(resolve_site_name(""), "index.html");
        assert_eq!(resolve_site_name("docs/"), "docs/index.html");
        assert_eq!(resolve_site_name("docs/guide.html"), "docs/guide.html");
    }

    // a slashless path asks after its directory index, a slashed one does not
    #[test]
    fn directory_index() {
        assert_eq!(directory_index_name("about"), Some("about/index.html".to_string()));
        assert_eq!(
            directory_index_name("docs/guide"),
            Some("docs/guide/index.html".to_string())
        );
        assert_eq!(directory_index_name("about/"), None);
        assert_eq!(directory_index_name(""), None);
    }

    // the redirect is relative to the last segment, so both site routes land right
    #[test]
    fn slash_redirect() {
        assert_eq!(slash_location("about"), Some(HeaderValue::from_static("about/")));
        assert_eq!(slash_location("docs/guide"), Some(HeaderValue::from_static("guide/")));
        assert_eq!(slash_location("a\u{7f}b"), None);
    }

    // unknown stored types are inferred from the path extension
    #[test]
    fn extension_inference() {
        let metadata = site_metadata(ContentType::Unknown, "assets/app.css", None, 60);
        assert_eq!(metadata.content_type, ContentType::TextCss);
        assert!(metadata.filename.is_none());
    }

    // the download query switches to a named attachment
    #[test]
    fn download_attachment() {
        let metadata = site_metadata(ContentType::Unknown, "docs/guide.html", Some("1"), 60);
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
        assert_eq!(
            site_content_security_policy(&[]),
            HeaderValue::from_static(SITE_CONTENT_SECURITY_POLICY)
        );

        let origins = vec![
            "https://api.devnet.solana.com".to_string(),
            "wss://api.devnet.solana.com".to_string(),
        ];
        let policy = site_content_security_policy(&origins);
        let policy = policy.to_str().expect("policy is ascii");
        assert!(policy.starts_with(SITE_CONTENT_SECURITY_POLICY));
        assert!(policy.ends_with(
            "connect-src 'self' https://api.devnet.solana.com wss://api.devnet.solana.com"
        ));
    }

    // cors answers the wildcard or a listed origin, and nothing else
    #[test]
    fn cors_matching() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_static("https://app.example"));

        assert!(cors_origin(&[], &headers).is_none());

        let listed = vec!["https://app.example".to_string()];
        assert_eq!(
            cors_origin(&listed, &headers),
            Some(HeaderValue::from_static("https://app.example"))
        );
        assert!(cors_origin(&listed, &HeaderMap::new()).is_none());

        let any = vec!["*".to_string()];
        assert_eq!(
            cors_origin(&any, &HeaderMap::new()),
            Some(HeaderValue::from_static("*"))
        );
    }

    // the site path always names its tape, whichever way it was reached
    #[test]
    fn path_tape_extraction() {
        let tape = Address::new_unique();
        assert_eq!(path_tape(&format!("/site/{tape}/a/b.css")), Some(tape));
        assert_eq!(path_tape(&format!("/site/{tape}")), Some(tape));
        assert_eq!(path_tape("/object/xyz"), None);
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

    // the well-known namespace is reserved, bare or nested; lookalike and
    // non-root paths are not
    #[test]
    fn well_known_reservation() {
        assert!(is_well_known("/.well-known"));
        assert!(is_well_known("/.well-known/acme-challenge/token"));
        assert!(!is_well_known("/"));
        assert!(!is_well_known("/well-known"));
        assert!(!is_well_known("/.well-known-ish/file"));
        assert!(!is_well_known("/docs/.well-known/file"));
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

    // a selective allow list varies on origin even when nothing matched; a
    // wildcard or empty list answers everyone alike and does not
    #[test]
    fn vary_on_selective_allowlist() {
        assert!(varies_on_origin(&["https://app.example".to_string()]));
        assert!(!varies_on_origin(&["*".to_string()]));
        assert!(!varies_on_origin(&[]));
    }

    // text types gain a utf-8 charset; binary types, json, and already-tagged
    // values are left untouched
    #[test]
    fn text_types_declare_charset() {
        assert_eq!(text_charset("text/html").as_deref(), Some("text/html; charset=utf-8"));
        assert_eq!(
            text_charset("image/svg+xml").as_deref(),
            Some("image/svg+xml; charset=utf-8")
        );
        assert!(text_charset("image/png").is_none());
        assert!(text_charset("application/json").is_none());
        assert!(text_charset("text/html; charset=utf-8").is_none());
    }

    // a fallback page is typed from the object served, not the requested route:
    // an untyped index.html for an extensionless spa path still renders as html
    #[test]
    fn fallback_typed_from_served_object() {
        let meta = site_metadata(ContentType::Unknown, INDEX_OBJECT, None, 60);
        assert_eq!(meta.content_type, ContentType::TextHtml);
    }
}
