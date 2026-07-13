use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_advance_epoch;
use crate::core::chain_tx::{submit_if_at_tip, wait_by_pace, TxOutcome, TxRejectionKind};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit an AdvanceEpoch transaction to advance the network
//          to the next epoch. Any committee member can submit this.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    // AdvanceEpoch waits on state for the quorum precondition (BadEpochState),
    // but a stale account reference clears over several blocks with no single
    // signal, so those retries back off instead of resubmitting every block.
    let mut state_rx = ctx.subscribe_state();
    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "advance_epoch: epoch already advanced");
            return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
        }

        info!(epoch = epoch.0, "advance_epoch: submitting");
        let outcome =
            submit_if_at_tip(&ctx.ingest, "advance_epoch", submit_advance_epoch(&ctx)).await;
        let pace = outcome.retry_pace();

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "advance_epoch: confirmed");
                return TaskDone::Done(Action::AdvanceEpoch, epoch);
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err),
                ..
            } => {
                warn!(epoch = epoch.0, ?err, "advance_epoch: program error");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_epoch: stale submission ignored");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_epoch: concurrent submission already applied");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_epoch: transaction rejected");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_epoch: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "advance_epoch: ingest stale, deferring");
                return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
            }
        }

        if wait_by_pace(pace, &mut backoff, &mut state_rx, &cancel).await {
            break;
        }
    }

    TaskDone::Cancelled(Action::AdvanceEpoch, epoch)
}
