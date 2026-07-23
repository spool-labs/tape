//! Resolving the real caller behind a reverse proxy.
//!
//! Both the node's admission control and the gateway's meter need the client
//! that actually made the request, not the proxy that relayed it, so the
//! resolution lives here once and both call it.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::extract::{ConnectInfo, Request};
use axum::http::HeaderMap;

use crate::config::cidr::CidrBlock;

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

/// Resolve the caller address behind a reverse proxy: the socket peer, unless
/// the peer is a trusted proxy, in which case the nearest forwarded hop that
/// is not itself a trusted proxy wins. Unparseable or fully-trusted chains
/// fall back to the peer.
pub fn resolve_caller_ip(peer: IpAddr, headers: &HeaderMap, trusted: &[CidrBlock]) -> IpAddr {
    if !is_trusted(peer, trusted) {
        return peer;
    }
    headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|hop| hop.trim().parse().ok())
        .rev()
        .find(|hop| !is_trusted(*hop, trusted))
        .unwrap_or(peer)
}

fn is_trusted(address: IpAddr, trusted: &[CidrBlock]) -> bool {
    trusted.iter().any(|block| block.contains(address))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(value: &str) -> IpAddr {
        value.parse().expect("address should parse")
    }

    fn block(value: &str) -> CidrBlock {
        value.parse().expect("cidr block should parse")
    }

    fn forwarded(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", value.parse().expect("header should parse"));
        headers
    }

    // with no proxy trusted, the socket peer is always the caller
    #[test]
    fn untrusted_peer_is_the_caller() {
        let headers = forwarded("198.51.100.9");

        assert_eq!(
            resolve_caller_ip(address("203.0.113.5"), &headers, &[]),
            address("203.0.113.5")
        );
    }

    // behind a trusted proxy the forwarded client becomes the caller
    #[test]
    fn trusted_peer_yields_the_forwarded_client() {
        let trusted = [block("10.0.0.1")];
        let headers = forwarded("198.51.100.9");

        assert_eq!(
            resolve_caller_ip(address("10.0.0.1"), &headers, &trusted),
            address("198.51.100.9")
        );
    }

    // the rightmost untrusted hop wins, so a client cannot spoof an arbitrary
    // address by prepending entries to the chain
    #[test]
    fn spoofed_prefix_hops_are_ignored() {
        let trusted = [block("10.0.0.1"), block("10.0.0.2")];
        let headers = forwarded("1.2.3.4, 198.51.100.9, 10.0.0.2");

        assert_eq!(
            resolve_caller_ip(address("10.0.0.1"), &headers, &trusted),
            address("198.51.100.9")
        );
    }

    // a trusted CIDR range covers every proxy inside it
    #[test]
    fn trusted_range() {
        let trusted = [block("173.245.48.0/20")];
        let headers = forwarded("198.51.100.9");

        assert_eq!(
            resolve_caller_ip(address("173.245.52.10"), &headers, &trusted),
            address("198.51.100.9")
        );
    }

    // an unparseable chain falls back to the peer rather than dropping the caller
    #[test]
    fn garbage_forwarded_header_falls_back_to_peer() {
        let trusted = [block("10.0.0.1")];
        let headers = forwarded("not-an-ip");

        assert_eq!(
            resolve_caller_ip(address("10.0.0.1"), &headers, &trusted),
            address("10.0.0.1")
        );
    }

    // a loopback proxy hop resolves to the real client, the case a same-host
    // reverse proxy would otherwise collapse to a filtered local address
    #[test]
    fn loopback_proxy_resolves_the_client() {
        let trusted = [block("127.0.0.1")];
        let headers = forwarded("198.51.100.9");

        assert_eq!(
            resolve_caller_ip(address("127.0.0.1"), &headers, &trusted),
            address("198.51.100.9")
        );
    }
}
