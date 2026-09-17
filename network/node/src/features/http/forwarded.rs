//! The caller of an axum request, resolved behind trusted proxies

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::extract::{ConnectInfo, Request};
use tape_forwarded::caller::resolve_caller_ip;
use tape_forwarded::cidr::CidrBlock;

/// The caller of a request: the socket peer, or the client it forwarded for
/// when the peer is a trusted proxy.
pub fn caller_ip(req: &Request, trusted: &[CidrBlock]) -> IpAddr {
    resolve_caller_ip(peer_ip(req), req.headers(), trusted)
}

/// The address on the other end of the socket, before any forwarding.
pub fn peer_ip(req: &Request) -> IpAddr {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}
