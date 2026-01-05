//! Cluster/network selection with shorthand notation.

use std::str::FromStr;

/// Solana cluster/network selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cluster {
    Localnet,
    Mainnet,
    Devnet,
    Testnet,
    Custom(String),
}

impl Cluster {
    /// Get the RPC URL for this cluster.
    pub fn rpc_url(&self) -> String {
        match self {
            Cluster::Localnet => "http://127.0.0.1:8899".to_string(),
            Cluster::Mainnet => "https://api.mainnet-beta.solana.com".to_string(),
            Cluster::Devnet => "https://api.devnet.solana.com".to_string(),
            Cluster::Testnet => "https://api.testnet.solana.com".to_string(),
            Cluster::Custom(url) => url.clone(),
        }
    }

    /// Get the WebSocket URL for this cluster.
    pub fn ws_url(&self) -> String {
        match self {
            Cluster::Localnet => "ws://127.0.0.1:8900".to_string(),
            Cluster::Mainnet => "wss://api.mainnet-beta.solana.com".to_string(),
            Cluster::Devnet => "wss://api.devnet.solana.com".to_string(),
            Cluster::Testnet => "wss://api.testnet.solana.com".to_string(),
            Cluster::Custom(url) => {
                // Convert http(s) to ws(s)
                url.replace("https://", "wss://")
                    .replace("http://", "ws://")
            }
        }
    }

    /// Get the display name for this cluster.
    pub fn name(&self) -> &str {
        match self {
            Cluster::Localnet => "localnet",
            Cluster::Mainnet => "mainnet-beta",
            Cluster::Devnet => "devnet",
            Cluster::Testnet => "testnet",
            Cluster::Custom(_) => "custom",
        }
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Cluster::Devnet
    }
}

impl FromStr for Cluster {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "l" | "local" | "localnet" => Ok(Cluster::Localnet),
            "m" | "main" | "mainnet" | "mainnet-beta" => Ok(Cluster::Mainnet),
            "d" | "dev" | "devnet" => Ok(Cluster::Devnet),
            "t" | "test" | "testnet" => Ok(Cluster::Testnet),
            s if s.starts_with("http://") || s.starts_with("https://") => {
                Ok(Cluster::Custom(s.to_string()))
            }
            _ => Err(format!(
                "Invalid cluster: '{}'. Use l/m/d/t or a valid RPC URL",
                s
            )),
        }
    }
}

impl std::fmt::Display for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cluster::Localnet => write!(f, "localnet"),
            Cluster::Mainnet => write!(f, "mainnet"),
            Cluster::Devnet => write!(f, "devnet"),
            Cluster::Testnet => write!(f, "testnet"),
            Cluster::Custom(url) => write!(f, "{}", url),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_shorthand() {
        assert_eq!(Cluster::from_str("l").unwrap(), Cluster::Localnet);
        assert_eq!(Cluster::from_str("m").unwrap(), Cluster::Mainnet);
        assert_eq!(Cluster::from_str("d").unwrap(), Cluster::Devnet);
        assert_eq!(Cluster::from_str("t").unwrap(), Cluster::Testnet);
    }

    #[test]
    fn test_cluster_custom_url() {
        let cluster = Cluster::from_str("https://my-rpc.com").unwrap();
        assert_eq!(cluster.rpc_url(), "https://my-rpc.com");
    }

    #[test]
    fn test_cluster_invalid() {
        assert!(Cluster::from_str("invalid").is_err());
    }
}
