//! Thread D - Erasure Recovery
//!
//! Handles recovery of slices that failed to sync from previous owners.
//!
//! NOTE: This worker is currently a stub pending storage layer redesign.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::core::context::NodeContext;

/// Recovery polling interval.
const RECOVERY_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Error type for recovery operations.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("no committee members available")]
    NoCommittee,

    #[error("RPC error: {0}")]
    Rpc(String),
}

/// Run the recovery worker loop.
///
/// NOTE: Currently a stub - just polls and logs.
pub async fn run(
    _ctx: Arc<NodeContext>,
    cancel: CancellationToken,
) -> Result<(), RecoveryError> {
    info!("Recovery thread starting (stub)");

    let mut interval = tokio::time::interval(RECOVERY_POLL_INTERVAL);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Recovery thread shutting down");
                break;
            }
            _ = interval.tick() => {
                debug!("Recovery poll (stub) - no pending recoveries");
            }
        }
    }

    Ok(())
}
