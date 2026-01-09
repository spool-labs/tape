//! Worker threads for the storage node.
//!
//! The node runs several worker threads coordinated by the orchestrator:
//! - **Thread B (network_sync)**: Handles epoch transitions and spool synchronization
//! - **Thread C (challenges)**: Responds to storage proof challenges
//! - **Thread D (recovery)**: Recovers slices via erasure coding when sync fails
//!
//! Note: The block processor (Thread A) is in the `block_processor` module.

pub mod challenges;
pub mod network_sync;
pub mod orchestrator;
pub mod recovery;

pub use challenges::ChallengeError;
pub use network_sync::NetworkSyncError;
pub use orchestrator::{run, OrchestratorError};
pub use recovery::RecoveryError;
