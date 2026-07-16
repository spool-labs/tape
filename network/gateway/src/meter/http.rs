use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tracing::debug;

use super::{GatewayMeterDecision, MeterCaller};
use crate::http::AppState;

impl MeterCaller {
    /// Resolve the metered identity for a request: the trusted-proxy-resolved
    /// caller IP, plus the verified access key and its assigned grade when the
    /// request was signed.
    pub(crate) fn resolve(
        peer: IpAddr,
        headers: &HeaderMap,
        trusted: &[IpAddr],
        access_key: Option<String>,
        grade: Option<String>,
    ) -> Self {
        Self {
            ip: resolve_caller_ip(peer, headers, trusted),
            access_key,
            grade,
        }
    }
}

/// Meter native object reads by resolved caller IP and stash the caller in the
/// request extensions so the handler charges the same identity for bytes.
pub(crate) async fn object_read_metering<Db, Cluster, Blockchain>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    mut req: Request,
    next: Next,
) -> Response
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let trusted = &state.context.config.gateway.metering.trusted_proxies;
    let caller = MeterCaller::resolve(peer_ip(&req), req.headers(), trusted, None, None);
    match state.meter.check_object_request(&caller) {
        GatewayMeterDecision::Allowed => {
            req.extensions_mut().insert(caller);
            next.run(req).await
        }
        GatewayMeterDecision::RateLimited { retry_after } => {
            debug!(ip = %caller.ip, retry_after_secs = retry_after.as_secs(), "gateway meter rejected object request");
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

/// Resolve the caller IP to meter: the socket peer, unless the peer is a
/// trusted proxy, in which case the nearest X-Forwarded-For hop that is not
/// itself a trusted proxy wins. Unparseable or fully-trusted chains fall back
/// to the peer.
fn resolve_caller_ip(peer: IpAddr, headers: &HeaderMap, trusted: &[IpAddr]) -> IpAddr {
    if !trusted.contains(&peer) {
        return peer;
    }
    headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|hop| hop.trim().parse().ok())
        .rev()
        .find(|hop| !trusted.contains(hop))
        .unwrap_or(peer)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    fn forwarded(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", value.parse().unwrap());
        headers
    }

    #[test]
    fn untrusted_peer_is_the_caller() {
        let headers = forwarded("198.51.100.9");
        assert_eq!(
            resolve_caller_ip(addr("203.0.113.5"), &headers, &[]),
            addr("203.0.113.5")
        );
    }

    #[test]
    fn trusted_peer_yields_the_forwarded_client() {
        let trusted = [addr("10.0.0.1")];
        let headers = forwarded("198.51.100.9");
        assert_eq!(
            resolve_caller_ip(addr("10.0.0.1"), &headers, &trusted),
            addr("198.51.100.9")
        );
    }

    // The rightmost non-trusted hop wins, so a client cannot spoof an
    // arbitrary IP by prepending entries to the chain.
    #[test]
    fn spoofed_prefix_hops_are_ignored() {
        let trusted = [addr("10.0.0.1"), addr("10.0.0.2")];
        let headers = forwarded("1.2.3.4, 198.51.100.9, 10.0.0.2");
        assert_eq!(
            resolve_caller_ip(addr("10.0.0.1"), &headers, &trusted),
            addr("198.51.100.9")
        );
    }

    #[test]
    fn garbage_forwarded_header_falls_back_to_peer() {
        let trusted = [addr("10.0.0.1")];
        let headers = forwarded("not-an-ip");
        assert_eq!(
            resolve_caller_ip(addr("10.0.0.1"), &headers, &trusted),
            addr("10.0.0.1")
        );
    }
}
