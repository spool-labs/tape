use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_join_network;
use crate::core::chain_tx::{TxOutcome, classify_tx};
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit a JoinNetwork transaction to volunteer for the next
//          epoch's committee. This signals the node intends to serve
//          in the upcoming epoch.
//
// Timing gate: JoinNetwork should not be submitted until 90% of the
// epoch duration has elapsed. This prevents committing to a potentially
// week-long epoch too early, reducing the risk of being unavailable
// at the epoch transition point. The on-chain program will enforce
// this constraint in the future.
//
// Algorithm:
// 1. Wait for timing gate:
//    a. Compute threshold = last_epoch + (EPOCH_DURATION * 90 / 100).
//    b. Loop: check cancel, compute now vs threshold.
//       If now < threshold → sleep for min(remaining, poll_interval).
//       If now >= threshold → proceed.
//    Note: last_epoch and EPOCH_DURATION come from protocol state.
//    EPOCH_DURATION is a compile-time constant from tape_api.
//    last_epoch needs to be in ProtocolState (requires adding it
//    to fetch_state).
//
// 2. Submit loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit JoinNetwork transaction via rpc.send_instructions:
//       - build_join_network_ix(fee_payer, authority, node_address)
//       - Wrap with compute budget instruction.
//    c. On success → return Done.
//    d. On UnexpectedState → the node might already be in committee_next.
//       Query the on-chain state to check:
//       - If already in committee_next → return Done.
//       - If not → retry with backoff.
//    e. On NodeStale / NotStaked → return Rejected.
//       These indicate the node's on-chain state is not suitable for
//       joining. The lifecycle worker re-evaluates.
//    f. On BadEpochState → retry with backoff.
//       Phase may not have reached the required state yet.
//    g. On retriable errors → backoff and retry.
//
// Prerequisite: AdvancePool must be done. This is enforced by the
// lifecycle decision function (next_action), not by this task.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        let has_joined = ctx.state()
            .find_member_next(ctx.node_id()).is_some();

        if ctx.state().epoch != epoch {
            info!(epoch = epoch.0, "advance_pool: wrong epoch");
            return TaskDone::Rejected(Action::JoinNetwork, epoch);
        }

        if has_joined {
            info!(epoch = epoch.0, "join_network: already in next committee");
            return TaskDone::Done(Action::JoinNetwork, epoch);
        }

        info!(epoch = epoch.0, "join_network: submitting");

        let result = submit_join_network(&ctx).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "join_network: confirmed");
                return TaskDone::Done(Action::JoinNetwork, epoch);
            }
            TxOutcome::Program(
                err @ (TapeError::NodeStale | TapeError::NotStaked),
            ) => {
                warn!(epoch = epoch.0, ?err, "join_network: rejected, node prerequisites not met");
                return TaskDone::Rejected(Action::JoinNetwork, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "join_network: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "join_network: transport error");
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::JoinNetwork, epoch);
}
