use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_advance_pool;
use crate::core::chain_tx::{TxOutcome, classify_tx};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit an AdvancePool transaction to settle rewards for this
//          node's staking pool. This is required for any node that was
//          in the current or previous committee.
//
// AdvancePool is permissionless — anyone can call it. But nodes call it
// for themselves because each node needs its pool advanced before it can
// earn rewards and before JoinNetwork can be called.
//
// Algorithm:
// 1. Loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit AdvancePool transaction via rpc.send_instructions:
//       - build_advance_pool_ix(fee_payer, authority, node_address)
//       - Wrap with compute budget instruction.
//    c. On success → return Done.
//    d. On AlreadyAdvanced → return Done (idempotent).
//    e. On BadEpochState → the on-chain program rejected the call.
//       This happens if the phase hasn't reached Settling yet (we were
//       scheduled too early) or if the epoch state is somehow wrong.
//       Retry with backoff — the phase may advance soon.
//    f. On NoRewards / RewardsOverflow / PoolAccountingFailed → stop retrying
//       for this epoch. These are deterministic program failures, not
//       transport/transient errors.
//    g. On retriable errors → backoff and retry.
//
// Unlike SyncEpoch, AdvancePool has no preconditions beyond being in
// the right epoch phase. No spool readiness check needed.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch != epoch {
            info!(epoch = epoch.0, "advance_pool: wrong epoch");
            return TaskDone::Rejected(Action::AdvancePool, epoch);
        }

        info!(epoch = epoch.0, "advance_pool: submitting");
        let result = submit_advance_pool(&ctx).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "advance_pool: confirmed");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Program(TapeError::AlreadyAdvanced) => {
                info!(epoch = epoch.0, "advance_pool: already advanced");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Program(
                err @ (
                    TapeError::NoRewards
                    | TapeError::RewardsOverflow
                    | TapeError::PoolAccountingFailed
                )
            ) => {
                warn!(epoch = epoch.0, ?err, "advance_pool: terminal program error");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "advance_pool: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "advance_pool: transport error");
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::AdvancePool, epoch);
}
