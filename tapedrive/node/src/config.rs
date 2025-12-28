//! Node configuration.

use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Error type for configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("failed to parse YAML config: {0}")]
    ParseYaml(#[from] serde_yaml::Error),

    #[error("invalid pubkey: {0}")]
    InvalidPubkey(String),

    #[error("invalid bind address: {0}")]
    InvalidBindAddress(String),
}

/// Configuration for a storage node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Node name for identification.
    pub name: String,

    /// Path to Ed25519 protocol keypair file.
    pub protocol_keypair: PathBuf,

    /// Path to Ed25519 network keypair file (for TLS).
    pub network_keypair: PathBuf,

    /// Path to BLS keypair file (for committee signing).
    pub bls_keypair: PathBuf,

    /// Address to bind the server to.
    pub bind_address: SocketAddr,

    /// Public hostname for this node.
    pub public_host: String,

    /// Public port for this node.
    pub public_port: u16,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Path to storage directory.
    pub storage_path: PathBuf,

    /// Storage capacity in bytes.
    pub storage_capacity: u64,

    /// Solana RPC URL.
    pub solana_rpc_url: String,

    /// Node authority pubkey on Solana.
    pub node_authority: Pubkey,
}

impl NodeConfig {
    /// Load configuration from a YAML file.
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        Self::from_yaml_str(&contents)
    }

    /// Load configuration from a YAML string.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let raw: RawNodeConfig = serde_yaml::from_str(yaml)?;
        raw.try_into()
    }

    /// Override the bind address.
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_address = addr;
        self
    }
}

/// Raw configuration as loaded from YAML (before validation).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNodeConfig {
    /// Node name for identification.
    pub name: String,

    /// Path to Ed25519 protocol keypair file.
    pub protocol_keypair: PathBuf,

    /// Path to Ed25519 network keypair file (for TLS).
    pub network_keypair: PathBuf,

    /// Path to BLS keypair file (for committee signing).
    pub bls_keypair: PathBuf,

    /// Address to bind the server to (as string for parsing).
    pub bind_address: String,

    /// Public hostname for this node.
    pub public_host: String,

    /// Public port for this node.
    pub public_port: u16,

    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,

    /// Path to storage directory.
    pub storage_path: PathBuf,

    /// Storage capacity in bytes.
    #[serde(default = "default_storage_capacity")]
    pub storage_capacity: u64,

    /// Solana RPC URL.
    pub solana_rpc_url: String,

    /// Node authority pubkey on Solana (as base58 string).
    pub node_authority: String,
}

fn default_storage_capacity() -> u64 {
    1_000_000_000 // 1 GB default
}

impl TryFrom<RawNodeConfig> for NodeConfig {
    type Error = ConfigError;

    fn try_from(raw: RawNodeConfig) -> Result<Self, Self::Error> {
        let bind_address = raw
            .bind_address
            .parse()
            .map_err(|_| ConfigError::InvalidBindAddress(raw.bind_address.clone()))?;

        let node_authority = Pubkey::from_str(&raw.node_authority)
            .map_err(|_| ConfigError::InvalidPubkey(raw.node_authority.clone()))?;

        Ok(Self {
            name: raw.name,
            protocol_keypair: raw.protocol_keypair,
            network_keypair: raw.network_keypair,
            bls_keypair: raw.bls_keypair,
            bind_address,
            public_host: raw.public_host,
            public_port: raw.public_port,
            tls: raw.tls,
            storage_path: raw.storage_path,
            storage_capacity: raw.storage_capacity,
            solana_rpc_url: raw.solana_rpc_url,
            node_authority,
        })
    }
}

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
    #[serde(default = "default_generate_self_signed")]
    pub generate_self_signed: bool,
}

fn default_generate_self_signed() -> bool {
    true
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

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_CONFIG: &str = r#"
name: "tape-node-1"
protocol_keypair: "/etc/tape/protocol.key"
network_keypair: "/etc/tape/network.key"
bls_keypair: "/etc/tape/bls.key"
bind_address: "0.0.0.0:8080"
public_host: "node1.tapedrive.io"
public_port: 443
tls:
  generate_self_signed: true
storage_path: "/var/lib/tape/data"
storage_capacity: 1000000
solana_rpc_url: "https://api.mainnet-beta.solana.com"
node_authority: "11111111111111111111111111111111"
"#;

    #[test]
    fn test_parse_yaml_config() {
        let config = NodeConfig::from_yaml_str(EXAMPLE_CONFIG).unwrap();

        assert_eq!(config.name, "tape-node-1");
        assert_eq!(config.protocol_keypair, PathBuf::from("/etc/tape/protocol.key"));
        assert_eq!(config.network_keypair, PathBuf::from("/etc/tape/network.key"));
        assert_eq!(config.bls_keypair, PathBuf::from("/etc/tape/bls.key"));
        assert_eq!(config.bind_address.to_string(), "0.0.0.0:8080");
        assert_eq!(config.public_host, "node1.tapedrive.io");
        assert_eq!(config.public_port, 443);
        assert!(config.tls.generate_self_signed);
        assert_eq!(config.storage_path, PathBuf::from("/var/lib/tape/data"));
        assert_eq!(config.storage_capacity, 1000000);
        assert_eq!(config.solana_rpc_url, "https://api.mainnet-beta.solana.com");
    }

    #[test]
    fn test_override_bind_address() {
        let config = NodeConfig::from_yaml_str(EXAMPLE_CONFIG).unwrap();
        let new_addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let config = config.with_bind_address(new_addr);

        assert_eq!(config.bind_address.to_string(), "127.0.0.1:9090");
    }

    #[test]
    fn test_invalid_pubkey() {
        let yaml = r#"
name: "test"
protocol_keypair: "/test"
network_keypair: "/test"
bls_keypair: "/test"
bind_address: "0.0.0.0:8080"
public_host: "test"
public_port: 443
storage_path: "/test"
solana_rpc_url: "http://localhost"
node_authority: "invalid_pubkey"
"#;
        let result = NodeConfig::from_yaml_str(yaml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::InvalidPubkey(_)));
    }

    #[test]
    fn test_invalid_bind_address() {
        let yaml = r#"
name: "test"
protocol_keypair: "/test"
network_keypair: "/test"
bls_keypair: "/test"
bind_address: "not_an_address"
public_host: "test"
public_port: 443
storage_path: "/test"
solana_rpc_url: "http://localhost"
node_authority: "11111111111111111111111111111111"
"#;
        let result = NodeConfig::from_yaml_str(yaml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::InvalidBindAddress(_)));
    }
}
