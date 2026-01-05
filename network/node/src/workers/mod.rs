//! Worker threads for the storage node.
//!
//! The node runs three main worker threads coordinated by the orchestrator:
//! - **Thread A (live_updates)**: Polls Solana blocks, parses transactions, updates control plane
//! - **Thread B (network_sync)**: Handles epoch transitions and spool synchronization
//! - **Thread C (challenges)**: Responds to storage proof challenges

pub mod challenges;
pub mod live_updates;
pub mod network_sync;
pub mod orchestrator;
pub mod tx_parser;

pub use challenges::ChallengeError;
pub use live_updates::LiveUpdateError;
pub use network_sync::NetworkSyncError;
pub use orchestrator::{run, OrchestratorError};
pub use tx_parser::{parse_block, ParseError, ParsedBlock, ParsedInstruction};
