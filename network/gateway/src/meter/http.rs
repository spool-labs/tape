use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tracing::debug;

use super::GatewayMeterDecision;
use crate::http::AppState;

pub(crate) async fn object_read_metering<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let ip = caller_ip(&req);
    match state.meter.check_object_request(ip) {
        GatewayMeterDecision::Allowed => next.run(req).await,
        GatewayMeterDecision::RateLimited { retry_after } => {
            debug!(%ip, retry_after_secs = retry_after.as_secs(), "gateway meter rejected object request");
            rate_limited_response(retry_after)
        }
    }
}

pub(crate) fn caller_ip(req: &Request) -> IpAddr {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    if !peer.is_loopback() {
        return peer;
    }
    // Behind the local reverse proxy every socket peer is 127.0.0.1; the
    // rightmost X-Forwarded-For entry is the one nginx appended and the only
    // one a client can't forge.
    forwarded_ip(req).unwrap_or(peer)
}

fn forwarded_ip(req: &Request) -> Option<IpAddr> {
    req.headers()
        .get("x-forwarded-for")?
        .to_str()
        .ok()?
        .rsplit(',')
        .next()?
        .trim()
        .parse()
        .ok()
}

pub(crate) fn rate_limited_response(retry_after: Duration) -> Response {
    let retry_after_secs = retry_after.as_secs().max(1).to_string();
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, retry_after_secs)],
        "rate limited",
    )
        .into_response()
}
