use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Deserializer};
use tape_protocol::api::SLICE_BODY_LIMIT;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct NetworkConfig {
    #[serde(default)]
    pub host: Option<String>,

    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            host: None,
            port: default_port(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HttpConfig {
    /// Address the plaintext HTTP listener binds to.
    #[serde(
        default = "default_http_listen",
        deserialize_with = "deserialize_socket_addr"
    )]
    pub listen: SocketAddr,

    /// Global request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Global concurrent request limit.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Maximum request body size for slice upload requests.
    #[serde(default = "default_slice_max_bytes")]
    pub slice_max_bytes: usize,

    /// Maximum request body size for peer protocol POST requests.
    #[serde(default = "default_peer_max_bytes")]
    pub peer_max_bytes: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            listen: default_http_listen(),
            timeout_secs: default_timeout_secs(),
            concurrency: default_concurrency(),
            slice_max_bytes: default_slice_max_bytes(),
            peer_max_bytes: default_peer_max_bytes(),
        }
    }
}

fn default_port() -> u16 {
    default_https_listen().port()
}

fn default_http_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3420)
}

pub fn default_https_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3430)
}

fn default_timeout_secs() -> u64 {
    60
}

fn default_concurrency() -> usize {
    2048
}

fn default_slice_max_bytes() -> usize {
    SLICE_BODY_LIMIT
}

fn default_peer_max_bytes() -> usize {
    1024 * 1024
}

fn deserialize_socket_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}
