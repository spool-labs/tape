use serde::{Deserialize, Deserializer};
use std::net::SocketAddr;
use std::path::PathBuf;

use super::helpers::deserialize_pathbuf;
use super::http::default_https_listen;

/// HTTPS listener settings. Always bound. Serves both anonymous and peer-only
/// routes; peer-only routes are gated by the `PeerCommitteeMember` extractor
/// which looks up the client cert's SPKI in the on-chain committee.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HttpsConfig {
    /// Address the HTTPS listener binds to.
    #[serde(
        default = "default_https_listen",
        deserialize_with = "deserialize_socket_addr"
    )]
    pub listen: SocketAddr,

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
}

impl Default for HttpsConfig {
    fn default() -> Self {
        Self {
            listen: default_https_listen(),
            identity_keypair: default_identity_keypair(),
            auto_update: default_auto_update(),
        }
    }
}

fn default_identity_keypair() -> PathBuf {
    super::helpers::expand_path("~/.tape/tls.key")
}

fn default_auto_update() -> bool {
    true
}

fn deserialize_socket_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}
