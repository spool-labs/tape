use rpc_solana::{redact_url_query, EndpointStrategy};
use serde::Deserialize;
use tape_core::types::SlotNumber;

/// Solana RPC and block-ingest settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SolanaConfig {
    /// Solana RPC endpoints used by the node runtime, tried in order.
    ///
    /// List more than one so a failing endpoint hands off to the next rather
    /// than stalling the ingestor until its retry budget runs out.
    #[serde(default = "default_rpc")]
    pub rpc: Vec<String>,

    /// How a fresh operation picks the endpoint it starts on.
    ///
    /// prefer-primary suits a paid primary with cheaper fallbacks,
    /// failover-sticky stays wherever the last failover landed, and
    /// round-robin spreads operations across equivalent endpoints.
    #[serde(default)]
    pub rpc_strategy: EndpointStrategy,

    /// Optional override for the first slot the ingestor should process.
    /// When absent the bootstrap phase derives the start slot from
    /// on-chain state (replay tail → local sync cursor → current
    /// epoch's `start_slot`). Only set this to override those defaults
    /// for surgery or testing.
    #[serde(default)]
    pub start_slot: Option<SlotNumber>,
}

impl SolanaConfig {
    /// The primary endpoint with its query string redacted, safe to log
    ///
    /// The query part carries the RPC api key.
    pub fn rpc_display(&self) -> String {
        self.rpc
            .first()
            .map(|endpoint| redact_url_query(endpoint))
            .unwrap_or_default()
    }
}

impl Default for SolanaConfig {
    fn default() -> Self {
        Self {
            rpc: default_rpc(),
            rpc_strategy: EndpointStrategy::default(),
            start_slot: None,
        }
    }
}

fn default_rpc() -> Vec<String> {
    vec!["http://127.0.0.1:8899".to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;

    // a fallback endpoint parses alongside the primary
    #[test]
    fn parses_endpoint_list() {
        let yaml = "rpc:\n  - \"http://cache:8899\"\n  - \"https://devnet.helius-rpc.com/?api-key=k\"\n";

        let config: SolanaConfig = serde_yaml::from_str(yaml).expect("parse");

        assert_eq!(config.rpc.len(), 2);
        assert_eq!(config.rpc[0], "http://cache:8899");
    }

    // a strategy name parses and an absent one falls back to prefer-primary
    #[test]
    fn parses_strategy() {
        let yaml = "rpc:\n  - \"http://cache:8899\"\nrpc_strategy: round-robin\n";
        let config: SolanaConfig = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(config.rpc_strategy, EndpointStrategy::RoundRobin);

        let yaml = "rpc:\n  - \"http://cache:8899\"\n";
        let config: SolanaConfig = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(config.rpc_strategy, EndpointStrategy::PreferPrimary);
    }

    // the logged endpoint never carries the api key
    #[test]
    fn display_hides_api_key() {
        let config = SolanaConfig {
            rpc: vec!["https://devnet.helius-rpc.com/?api-key=secret".to_string()],
            ..SolanaConfig::default()
        };

        assert_eq!(config.rpc_display(), "https://devnet.helius-rpc.com/?<redacted>");
    }
}
