use serde::Deserialize;
use tape_core::types::SlotNumber;

/// Solana RPC and block-ingest settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SolanaConfig {
    /// Solana RPC URL used by the node runtime.
    #[serde(default = "default_rpc")]
    pub rpc: String,

    /// Optional override for the first slot the ingestor should process.
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

impl SolanaConfig {
    /// Resolve the block-ingest start slot using the configured override or the default.
    pub fn block_start_slot(&self) -> SlotNumber {
        self.start_slot.unwrap_or_else(default_start_slot)
    }
}

fn default_rpc() -> String {
    "http://127.0.0.1:8899".to_string()
}

fn default_start_slot() -> SlotNumber {
    SlotNumber(1)
}
