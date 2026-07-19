//! CIDR blocks for config fields that trust proxy address ranges
//!
//! Accepts a bare address or an address with a prefix length, so existing
//! single-address configs keep parsing while CDN ranges become expressible.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use axum::http::HeaderMap;
use serde::Deserialize;
use serde::de::Error as DeserializeError;

/// An address range in CIDR notation; a bare address is a full-length prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CidrBlock {
    address: IpAddr,
    prefix_length: u8,
}

impl CidrBlock {
    /// Whether the address falls inside this block. Mixed families never
    /// match.
    pub fn contains(&self, candidate: IpAddr) -> bool {
        match (self.address, candidate) {
            (IpAddr::V4(network), IpAddr::V4(candidate)) => {
                let mask = prefix_mask_v4(self.prefix_length);
                u32::from(network) & mask == u32::from(candidate) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(candidate)) => {
                let mask = prefix_mask_v6(self.prefix_length);
                u128::from(network) & mask == u128::from(candidate) & mask
            }
            (IpAddr::V4(_), IpAddr::V6(_)) | (IpAddr::V6(_), IpAddr::V4(_)) => false,
        }
    }
}

fn prefix_mask_v4(prefix_length: u8) -> u32 {
    u32::MAX
        .checked_shl(32 - u32::from(prefix_length))
        .unwrap_or(0)
}

fn prefix_mask_v6(prefix_length: u8) -> u128 {
    u128::MAX
        .checked_shl(128 - u32::from(prefix_length))
        .unwrap_or(0)
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

impl FromStr for CidrBlock {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = match value.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (value, None),
        };

        let address: IpAddr = address
            .trim()
            .parse()
            .map_err(|error| format!("invalid address in {value:?}: {error}"))?;
        let family_bits = match address {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };

        let prefix_length = match prefix {
            None => family_bits,
            Some(prefix) => prefix
                .trim()
                .parse()
                .ok()
                .filter(|length| *length <= family_bits)
                .ok_or_else(|| format!("invalid prefix length in {value:?}"))?,
        };

        Ok(Self {
            address,
            prefix_length,
        })
    }
}

impl fmt::Display for CidrBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.address, self.prefix_length)
    }
}

impl<'de> Deserialize<'de> for CidrBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(DeserializeError::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(value: &str) -> CidrBlock {
        value.parse().expect("cidr block should parse")
    }

    fn address(value: &str) -> IpAddr {
        value.parse().expect("address should parse")
    }

    // a bare address parses as a full-length prefix matching only itself
    #[test]
    fn bare_address() {
        let single = block("203.0.113.7");

        assert!(single.contains(address("203.0.113.7")));
        assert!(!single.contains(address("203.0.113.8")));
    }

    // a v4 range contains its members and nothing outside
    #[test]
    fn v4_range() {
        let range = block("173.245.48.0/20");

        assert!(range.contains(address("173.245.48.1")));
        assert!(range.contains(address("173.245.63.254")));
        assert!(!range.contains(address("173.245.64.1")));
        assert!(!range.contains(address("2400:cb00::1")));
    }

    // a v6 range contains its members and never a v4 address
    #[test]
    fn v6_range() {
        let range = block("2400:cb00::/32");

        assert!(range.contains(address("2400:cb00::1")));
        assert!(range.contains(address("2400:cb00:ffff::1")));
        assert!(!range.contains(address("2400:cb01::1")));
        assert!(!range.contains(address("173.245.48.1")));
    }

    // a zero prefix matches every address in its family
    #[test]
    fn zero_prefix() {
        let all = block("0.0.0.0/0");

        assert!(all.contains(address("8.8.8.8")));
        assert!(!all.contains(address("::1")));
    }

    // out-of-range prefixes and junk are rejected
    #[test]
    fn invalid_input() {
        assert!("10.0.0.0/33".parse::<CidrBlock>().is_err());
        assert!("::/129".parse::<CidrBlock>().is_err());
        assert!("not-an-address".parse::<CidrBlock>().is_err());
        assert!("10.0.0.0/x".parse::<CidrBlock>().is_err());
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
