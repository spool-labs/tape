use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_node::config::cidr::{CidrBlock, resolve_caller_ip};
use tape_protocol::Api;
use tracing::debug;

use super::{GatewayMeterDecision, MeterCaller};
use crate::http::AppState;

impl MeterCaller {
    /// Resolve the metered identity for a request: the trusted-proxy-resolved
    /// caller IP metered at the route's grade, plus the verified access key
    /// and its assigned grade when the request was signed.
    pub fn resolve(
        peer: IpAddr,
        headers: &HeaderMap,
        trusted: &[CidrBlock],
        ip_grade: String,
        access_key: Option<String>,
        grade: Option<String>,
    ) -> Self {
        Self {
            ip: resolve_caller_ip(peer, headers, trusted),
            ip_grade,
            access_key,
            grade,
        }
    }
}

/// Meter native object reads at the anonymous grade and stash the caller in
/// the request extensions so the handler charges the same identity for bytes.
pub async fn object_read_metering<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let grade = state.context.config.gateway.metering.anonymous_grade.clone();
    read_metering(state, grade, req, next).await
}

/// Meter site-route reads at the site grade, whose buckets are independent of
/// plain object reads so a multi-asset page load has its own headroom.
pub async fn site_read_metering<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let grade = state.context.config.gateway.metering.site_grade.clone();
    read_metering(state, grade, req, next).await
}

async fn read_metering<Db, Cluster, Blockchain>(
    state: AppState<Db, Cluster, Blockchain>,
    ip_grade: String,
    mut req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let trusted = &state.context.config.gateway.metering.trusted_proxies;
    let caller = MeterCaller::resolve(peer_ip(&req), req.headers(), trusted, ip_grade, None, None);
    match state.meter.check_object_request(&caller) {
        GatewayMeterDecision::Allowed => {
            // feed the atlas display: gateway reads are user fetches
            state.context.atlas.push_ip(caller.ip, false);
            req.extensions_mut().insert(caller);
            next.run(req).await
        }
        GatewayMeterDecision::RateLimited { retry_after } => {
            debug!(ip = %caller.ip, grade = %caller.ip_grade, retry_after_secs = retry_after.as_secs(), "gateway meter rejected read");
            rate_limited_response(retry_after)
        }
    }
}

fn peer_ip(req: &Request) -> IpAddr {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub fn rate_limited_response(retry_after: Duration) -> Response {
    let retry_after_secs = retry_after.as_secs().max(1).to_string();
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, retry_after_secs)],
        "rate limited",
    )
        .into_response()
}

