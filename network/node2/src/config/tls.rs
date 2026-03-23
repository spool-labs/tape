use serde::Deserialize;
use std::path::PathBuf;

use super::helpers::{deserialize_optional_pathbuf, deserialize_pathbuf};

/// TLS and peer pinning configuration.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// Path to the node TLS identity keypair.
    #[serde(default = "default_identity_keypair", deserialize_with = "deserialize_pathbuf")]
    pub identity_keypair: PathBuf,

    /// Path to a PEM certificate file, when externally managed.
    #[serde(default, deserialize_with = "deserialize_optional_pathbuf")]
    pub certificate_path: Option<PathBuf>,

    /// Path to a PEM private key file, when externally managed.
    #[serde(default, deserialize_with = "deserialize_optional_pathbuf")]
    pub key_path: Option<PathBuf>,

    /// Whether to generate a self-signed certificate when files are absent.
    #[serde(default = "default_self_signed")]
    pub self_signed: bool,

    /// Whether peer identity should be enforced once TLS is wired.
    #[serde(default = "default_verify_peer_id")]
    pub verify_peer_id: bool,

    /// Grace period for retained pins during key rotation.
    #[serde(default = "default_pin_ttl")]
    pub pin_ttl: u64,

    /// Maximum number of pins retained per peer.
    #[serde(default = "default_max_pins")]
    pub max_pins: usize,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            identity_keypair: default_identity_keypair(),
            certificate_path: None,
            key_path: None,
            self_signed: default_self_signed(),
            verify_peer_id: default_verify_peer_id(),
            pin_ttl: default_pin_ttl(),
            max_pins: default_max_pins(),
        }
    }
}

fn default_identity_keypair() -> PathBuf {
    super::helpers::expand_path("~/.tape/tls.key")
}

fn default_self_signed() -> bool {
    true
}

fn default_verify_peer_id() -> bool {
    true
}

fn default_pin_ttl() -> u64 {
    90
}

fn default_max_pins() -> usize {
    2
}
