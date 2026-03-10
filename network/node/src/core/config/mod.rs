//! Node configuration.

mod api;
mod node;
mod recovery;
mod tls;

use std::path::PathBuf;

pub use api::{IngressLimitsConfig, NodeApiConfig, TransportSecurityConfig};
pub use node::{ConfigError, NodeConfig, default_config_path};
pub use recovery::RecoveryConfig;
pub use tls::TlsConfig;

fn expand_path(path: &str) -> PathBuf {
    shellexpand::full(path)
        .map(|s| PathBuf::from(s.as_ref()))
        .unwrap_or_else(|_| PathBuf::from(path))
}
