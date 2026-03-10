use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to TLS certificate file.
    #[serde(default)]
    pub certificate_path: Option<PathBuf>,

    /// Path to TLS key file.
    #[serde(default)]
    pub key_path: Option<PathBuf>,

    /// Whether to generate a self-signed certificate.
    #[serde(default = "self_signed_default")]
    pub generate_self_signed: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            certificate_path: None,
            key_path: None,
            generate_self_signed: true,
        }
    }
}

fn self_signed_default() -> bool {
    true
}
