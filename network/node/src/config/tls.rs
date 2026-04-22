use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::helpers::deserialize_pathbuf;

/// TLS configuration for peer-to-peer HTTPS.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// Path to the node's persistent Ed25519 TLS keypair. Generated on first
    /// boot if missing; its public key is what gets published on-chain as
    /// `Node.metadata.network_tls`.
    #[serde(
        default = "default_identity_keypair",
        deserialize_with = "deserialize_pathbuf"
    )]
    pub identity_keypair: PathBuf,

    /// If the on-chain `network_tls` differs from the local keypair's public
    /// key, automatically emit a `SetNetworkTls` transaction to overwrite it.
    ///
    /// Default: `true` (safe for dev/simnet/devnet). Operators running
    /// testnet/mainnet should set this to `false` and rotate the key via
    /// explicit `SetNetworkTls` submissions.
    #[serde(default = "default_auto_update")]
    pub auto_update: bool,

    /// Optional loopback-only plain-HTTP listener for operator tooling
    /// (readiness probes, `curl` health checks, local Prometheus scrapes).
    /// Only serves `/v1/health`, `/v1/stats`, and (when the `metrics` feature
    /// is enabled) `/metrics`. Rejected at startup if the configured address
    /// is not loopback.
    #[serde(default)]
    pub local_plaintext_listen: Option<SocketAddr>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            identity_keypair: default_identity_keypair(),
            auto_update: default_auto_update(),
            local_plaintext_listen: None,
        }
    }
}

fn default_identity_keypair() -> PathBuf {
    super::helpers::expand_path("~/.tape/tls.key")
}

fn default_auto_update() -> bool {
    true
}
