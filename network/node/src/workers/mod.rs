//! Worker orchestration.
//!
//! The orchestrator spawns and coordinates all node threads.
//! Individual workers are in `features/`:
//! - `features::block_processing` - Thread A: Block processing
//! - `features::epoch_sync` - Thread B: Network/epoch sync
//! - `features::challenges` - Thread C: Storage proofs
//! - `features::recovery` - Thread D: Erasure recovery

pub mod orchestrator;

pub use orchestrator::{run, OrchestratorError};
