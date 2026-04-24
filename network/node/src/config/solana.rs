use serde::Deserialize;
use tape_core::types::SlotNumber;

/// Solana RPC and block-ingest settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SolanaConfig {
    /// Solana RPC URL used by the node runtime.
    #[serde(default = "default_rpc")]
    pub rpc: String,

    /// Optional override for the first slot the ingestor should process.
    /// When absent the bootstrap phase derives the start slot from
    /// on-chain state (replay tail → local sync cursor → current
    /// epoch's `start_slot`). Only set this to override those defaults
    /// for surgery or testing.
    #[serde(default)]
    pub start_slot: Option<SlotNumber>,
}

impl Default for SolanaConfig {
    fn default() -> Self {
        Self {
            rpc: default_rpc(),
            start_slot: None,
        }
    }
}

fn default_rpc() -> String {
    "http://127.0.0.1:8899".to_string()
}
