//! Routes: `POST /v1/reports` and `GET /v1/health`

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use tape_forwarded::caller::resolve_caller_ip;
use tape_forwarded::cidr::CidrBlock;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, warn};

use crate::api::error::ApiError;
use crate::api::limiter::CallerLimiter;
use crate::api::report::parse_report;
use crate::api::reporter::ReporterHasher;
use crate::config::settings::ApiConfig;
use crate::records::rows::NewReport;
use crate::records::store::Records;
use crate::types::timestamp::Timestamp;

pub const REPORTS_PATH: &str = "/v1/reports";
pub const HEALTH_PATH: &str = "/v1/health";

const ACKNOWLEDGED_BODY: &str = r#"{"acknowledged":true}"#;
const JSON_CONTENT: &str = "application/json";

/// Everything a report request needs
#[derive(Clone)]
pub struct ApiState {
    records: Records,
    limiter: Arc<CallerLimiter>,
    reporter: Arc<ReporterHasher>,
    trusted_proxies: Arc<Vec<CidrBlock>>,
}

impl ApiState {
    pub fn new(records: Records, config: &ApiConfig, salt: String) -> Self {
        Self {
            records,
            limiter: Arc::new(CallerLimiter::new(&config.caller_limit)),
            reporter: Arc::new(ReporterHasher::new(salt)),
            trusted_proxies: Arc::new(config.trusted_proxies.clone()),
        }
    }
}

pub fn router(state: ApiState, config: &ApiConfig) -> Result<Router, ApiError> {
    let mut origins = Vec::new();
    for origin in &config.allowed_origins {
        origins.push(HeaderValue::from_str(origin).map_err(|_| ApiError::Origin(origin.clone()))?);
    }
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::POST])
        .allow_headers([CONTENT_TYPE]);

    let router = Router::new()
        .route(REPORTS_PATH, post(submit_report))
        .route(HEALTH_PATH, get(health))
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .layer(cors)
        .with_state(state);
    Ok(router)
}

/// Limit the caller, validate, commit, then acknowledge
async fn submit_report(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = resolve_caller_ip(peer.ip(), &headers, &state.trusted_proxies);
    if !state.limiter.try_acquire(caller, Instant::now()) {
        debug!("report over its caller limit, acknowledged without recording");
        return acknowledged();
    }

    let report = match parse_report(&body) {
        Ok(report) => report,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let new_report = NewReport {
        target: report.target,
        reason: report.reason,
        note: report.note,
        idempotency_key: report.idempotency_key,
        reporter_hash: state.reporter.hash(caller),
    };

    match state.records.submit_report(new_report, Timestamp::now()).await {
        Ok(_) => acknowledged(),
        Err(error) => {
            warn!(%error, "report not recorded");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}

fn acknowledged() -> Response {
    (StatusCode::ACCEPTED, [(CONTENT_TYPE, JSON_CONTENT)], ACKNOWLEDGED_BODY).into_response()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use axum::body::{to_bytes, Body};
    use axum::http::header::{
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_HEADERS,
        ACCESS_CONTROL_REQUEST_METHOD, ORIGIN,
    };
    use axum::http::Request;
    use rusqlite::Connection;
    use serde_json::json;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::config::settings::CallerLimitConfig;
    use crate::types::ids::ReportCount;

    const ORIGIN_ALLOWED: &str = "https://explorer.example";
    const PROXY: &str = "10.0.0.1:443";
    const TRACK: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    struct Api {
        dir: TempDir,
        router: Router,
    }

    fn api() -> Api {
        let dir = tempfile::tempdir().expect("temp dir");
        let records = Records::open(&dir.path().join("records.sqlite")).expect("open records");
        let config = ApiConfig {
            listen: "127.0.0.1:0".parse().expect("listen"),
            allowed_origins: vec![ORIGIN_ALLOWED.to_string()],
            trusted_proxies: vec!["10.0.0.1".parse().expect("proxy")],
            reporter_salt_file: PathBuf::new(),
            max_body_bytes: 4_096,
            caller_limit: CallerLimitConfig {
                reports: ReportCount(2),
                window: Duration::from_secs(3_600),
                max_callers: 100,
            },
        };
        let state = ApiState::new(records, &config, "salt".to_string());
        Api {
            dir,
            router: router(state, &config).expect("router"),
        }
    }

    fn report(key: &str) -> String {
        json!({
            "target": {"kind": "track", "address": TRACK},
            "reason": "sexual",
            "note": "optional context",
            "idempotency_key": key,
        })
        .to_string()
    }

    async fn post_report(api: &Api, peer: &str, forwarded: Option<&str>, body: String) -> Response {
        let mut builder = Request::post(REPORTS_PATH).header(CONTENT_TYPE, JSON_CONTENT);
        if let Some(forwarded) = forwarded {
            builder = builder.header("x-forwarded-for", forwarded);
        }
        let mut request = builder.body(Body::from(body)).expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo(peer.parse::<SocketAddr>().expect("peer")));
        api.router.clone().oneshot(request).await.expect("response")
    }

    async fn assert_acknowledged(response: Response) {
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.expect("body");
        assert_eq!(body.as_ref(), ACKNOWLEDGED_BODY.as_bytes());
    }

    fn count(api: &Api, sql: &str) -> u64 {
        let connection = Connection::open(api.dir.path().join("records.sqlite")).expect("open database");
        connection.query_row(sql, [], |row| row.get(0)).expect("count")
    }

    // an accepted report is committed with its job before the 202
    #[tokio::test]
    async fn accepted() {
        let api = api();

        let response = post_report(&api, "203.0.113.5:5000", None, report("key-accepted-00001")).await;

        assert_acknowledged(response).await;
        assert_eq!(count(&api, "SELECT COUNT(*) FROM reports"), 1);
        assert_eq!(count(&api, "SELECT COUNT(*) FROM jobs WHERE target_kind = 'track'"), 1);
    }

    // records are durable: another connection sees the report under wal and full sync
    #[tokio::test]
    async fn committed() {
        let api = api();

        let response = post_report(&api, "203.0.113.5:5000", None, report("key-committed-0001")).await;
        let connection = Connection::open(api.dir.path().join("records.sqlite")).expect("open database");
        let key: String = connection
            .query_row("SELECT idempotency_key FROM reports", [], |row| row.get(0))
            .expect("committed report");
        let journal: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode");

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(key, "key-committed-0001");
        assert_eq!(journal, "wal");
    }

    // a repeated idempotency key is acknowledged but stored once
    #[tokio::test]
    async fn duplicate() {
        let api = api();

        let first = post_report(&api, "203.0.113.5:5000", None, report("key-duplicate-0001")).await;
        let second = post_report(&api, "203.0.113.5:5000", None, report("key-duplicate-0001")).await;

        assert_acknowledged(first).await;
        assert_acknowledged(second).await;
        assert_eq!(count(&api, "SELECT COUNT(*) FROM reports"), 1);
    }

    // reports over the caller limit are acknowledged but not stored
    #[tokio::test]
    async fn limited() {
        let api = api();

        for index in 0..3 {
            let response = post_report(&api, "203.0.113.5:5000", None, report(&format!("key-limited-000{index}"))).await;
            assert_acknowledged(response).await;
        }

        assert_eq!(count(&api, "SELECT COUNT(*) FROM reports"), 2);
    }

    // malformed bodies and targets answer 400
    #[tokio::test]
    async fn malformed() {
        let api = api();
        let bodies = [
            "{".to_string(),
            json!({"target": {"kind": "blob", "address": TRACK}, "reason": "hate", "idempotency_key": "key-malformed-0001"}).to_string(),
            json!({"target": {"kind": "track", "address": "0OIl"}, "reason": "hate", "idempotency_key": "key-malformed-0002"}).to_string(),
            json!({"target": {"kind": "track", "address": TRACK}, "reason": "spam", "idempotency_key": "key-malformed-0003"}).to_string(),
        ];

        for (index, body) in bodies.into_iter().enumerate() {
            let peer = format!("198.51.100.{}:5000", index + 1);
            let response = post_report(&api, &peer, None, body).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        assert_eq!(count(&api, "SELECT COUNT(*) FROM reports"), 0);
    }

    // preflights are answered for configured origins only
    #[tokio::test]
    async fn preflight() {
        let api = api();
        let preflight = |origin: &str| {
            Request::builder()
                .method(Method::OPTIONS)
                .uri(REPORTS_PATH)
                .header(ORIGIN, origin)
                .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                .body(Body::empty())
                .expect("preflight")
        };

        let allowed = api.router.clone().oneshot(preflight(ORIGIN_ALLOWED)).await.expect("allowed");
        let other = api.router.clone().oneshot(preflight("https://elsewhere.example")).await.expect("other");

        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(
            allowed.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).and_then(|value| value.to_str().ok()),
            Some(ORIGIN_ALLOWED)
        );
        let methods = allowed.headers().get(ACCESS_CONTROL_ALLOW_METHODS).and_then(|value| value.to_str().ok());
        assert!(methods.is_some_and(|methods| methods.contains("POST")));
        assert!(other.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
    }

    // behind a trusted proxy each forwarded client has its own limit; elsewhere forwarding is ignored
    #[tokio::test]
    async fn trusted_proxy() {
        let api = api();

        for index in 0..2 {
            let key = format!("key-forwarded-a-00{index}");
            assert_acknowledged(post_report(&api, PROXY, Some("198.51.100.20"), report(&key)).await).await;
        }
        assert_acknowledged(post_report(&api, PROXY, Some("198.51.100.21"), report("key-forwarded-b-000")).await).await;
        for index in 0..3 {
            let key = format!("key-spoofed-000{index}");
            let forged = format!("192.0.2.{index}");
            assert_acknowledged(post_report(&api, "203.0.113.9:5000", Some(&forged), report(&key)).await).await;
        }

        assert_eq!(count(&api, "SELECT COUNT(*) FROM reports"), 5);
        assert_eq!(count(&api, "SELECT COUNT(DISTINCT reporter_hash) FROM reports"), 3);
    }

    // health answers without a body to parse
    #[tokio::test]
    async fn health_check() {
        let api = api();
        let request = Request::get(HEALTH_PATH).body(Body::empty()).expect("request");

        let response = api.router.clone().oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
