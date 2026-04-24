use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::helpers::{deserialize_optional_pathbuf, deserialize_pathbuf};

/// TLS configuration for peer-to-peer HTTPS.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// Path to the node's persistent P-256 (secp256r1) TLS keypair, in PKCS#8
    /// PEM format. Generated on first boot if missing; its public key is what
    /// gets published on-chain as `Node.metadata.network_tls`.
    ///
    /// The same file can be handed to `certbot` to issue a CA-signed
    /// certificate for a domain without re-keying — see `certificate_path`.
    #[serde(
        default = "default_identity_keypair",
        deserialize_with = "deserialize_pathbuf"
    )]
    pub identity_keypair: PathBuf,

    /// Optional path to a PEM-encoded certificate chain issued to the same
    /// keypair as `identity_keypair`. When set, the node serves that cert
    /// instead of generating a self-signed one, so browsers and plain `curl`
    /// can reach public routes over HTTPS via the CA chain.
    ///
    /// The server validates at startup that the leaf cert's SubjectPublicKeyInfo
    /// matches the local keypair's SPKI; a mismatch aborts boot (a CA cert
    /// issued to a different key would serve browsers correctly but silently
    /// break every SDK pin).
    ///
    /// When `None`, the node generates a self-signed cert from the keypair.
    /// Protocol clients work in both modes because they pin against on-chain
    /// `network_tls`; only browser/curl trust depends on the CA chain.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_pathbuf"
    )]
    pub certificate_path: Option<PathBuf>,

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
            certificate_path: None,
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
