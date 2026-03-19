use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_advance_epoch;
use crate::core::chain_tx::{TxOutcome, classify_tx};
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit an AdvanceEpoch transaction to advance the network
//          to the next epoch. Any committee member can submit this.
//
// The on-chain program checks:
//   - Phase is Active
//   - now >= last_epoch + EPOCH_DURATION (enough time has passed)
//   - committee_next has enough members (>= MIN_COMMITTEE_SIZE)
//
// Algorithm:
// 1. Loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit AdvanceEpoch transaction via rpc.send_instructions:
//       - build_advance_epoch_ix(fee_payer, authority)
//       - Wrap with compute budget instruction.
//    c. On success → return Done.
//       The epoch has advanced. The EpochManager will observe the
//       AdvanceEpoch instruction in the block stream, fetch new
//       protocol state, and publish it via state_rx. The lifecycle
//       worker will then see the new epoch and reset.
//    d. On TooSoon → not enough time has elapsed.
//       Sleep for a portion of the remaining time, then retry.
//    e. On InsufficientCommittee → not enough nodes have joined.
//       Retry with backoff. Other nodes may still be joining.
//    f. On SnapshotIncomplete → snapshot not yet finalized.
//       Retry with backoff.
//    g. On BadEpochState → phase is not Active.
//       Retry with backoff. This shouldn't happen if the lifecycle
//       decision function is working correctly, but it's recoverable.
//    h. On BadSchedule → return Rejected. This is a permanent error
//       indicating an on-chain bug or misconfiguration. The lifecycle
//       worker will re-evaluate and respawn (since we never give up),
//       but it will keep hitting this until the epoch advances via
//       another node.
//    i. On retriable errors → backoff and retry.
//
// Note: Multiple nodes may attempt AdvanceEpoch simultaneously.
// Only the first one to land will succeed. The others will observe
// the epoch advance via the block stream and the lifecycle worker
// will reset. This is fine — the on-chain check is idempotent in
// the sense that a stale AdvanceEpoch attempt simply fails.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch != epoch {
            info!(epoch = epoch.0, "advance_epoch: epoch already advanced");
            return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
        }

        info!(epoch = epoch.0, "advance_epoch: submitting");
        let result = submit_advance_epoch(&ctx).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "advance_epoch: confirmed");
                return TaskDone::Done(Action::AdvanceEpoch, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "advance_epoch: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "advance_epoch: transport error");
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::AdvanceEpoch, epoch);
}

