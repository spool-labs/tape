//! Thread C - Challenges (Stub)
//!
//! Handles storage challenges against other nodes based on on-chain nonce values.
//!
//! TODO: Implement challenge logic:
//! 1. Read nonce values from chain
//! 2. Select random nodes to challenge
//! 3. Request proofs of storage
//! 4. Verify proofs against commitments
//! 5. Submit fraud proofs if invalid

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::context::NodeContext;

/// Default interval between challenge rounds.
const DEFAULT_CHALLENGE_INTERVAL_SECS: u64 = 60;

/// Error type for challenge operations.
#[derive(Debug, thiserror::Error)]
pub enum ChallengeError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("verification failed: {0}")]
    Verification(String),
}

/// Run the challenges loop.
///
/// This is Thread C's main entry point (currently a stub).
pub async fn run(_ctx: Arc<NodeContext>, cancel: CancellationToken) -> Result<(), ChallengeError> {
    info!("Challenges thread starting (stub implementation)");

    let interval = Duration::from_secs(DEFAULT_CHALLENGE_INTERVAL_SECS);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Challenges thread shutting down");
                break;
            }
            _ = tokio::time::sleep(interval) => {
                // TODO: Implement challenge logic
                //
                // 1. Check if we're in the committee
                // if !ctx.is_in_committee() {
                //     continue;
                // }
                //
                // 2. Read nonce from chain
                // let nonce = ctx.rpc.get_challenge_nonce().await?;
                //
                // 3. Derive which nodes/slices to challenge based on nonce
                // let challenges = derive_challenges(&ctx.control_plane, nonce);
                //
                // 4. For each challenge:
                //    a. Request proof from target node
                //    b. Verify proof against on-chain commitment
                //    c. If invalid, submit fraud proof
                //
                // 5. Update metrics
                // ctx.metrics.challenges_issued.inc();
            }
        }
    }

    Ok(())
}

// TODO: Implement these functions when challenge protocol is defined

/// Derive which nodes/slices to challenge based on nonce.
#[allow(dead_code)]
fn derive_challenges(
    _control_plane: &crate::control_plane::ControlPlane,
    _nonce: [u8; 32],
) -> Vec<ChallengeTarget> {
    Vec::new()
}

/// A challenge target.
#[allow(dead_code)]
struct ChallengeTarget {
    /// Node to challenge.
    node_id: tape_core::prelude::NodeId,
    /// Spool to challenge.
    spool_idx: tape_core::spooler::SpoolIndex,
    /// Specific track to challenge (if any).
    track: Option<tape_crypto::Pubkey>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_compiles() {
        // Just verify the stub compiles
    }
}
