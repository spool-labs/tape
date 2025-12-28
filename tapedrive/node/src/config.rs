//! Node configuration.

use solana_program::pubkey::Pubkey;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Configuration for a storage node.
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

    /// Solana RPC URL.
    pub solana_rpc_url: String,

    /// Node authority pubkey on Solana.
    pub node_authority: Pubkey,
}

/// TLS configuration.
pub struct TlsConfig {
    /// Path to TLS certificate file.
    pub certificate_path: Option<PathBuf>,

    /// Path to TLS key file.
    pub key_path: Option<PathBuf>,

    /// Whether to generate a self-signed certificate.
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
