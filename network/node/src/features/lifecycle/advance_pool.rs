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
use crate::core::chain_tx::{TxOutcome, TxRejectionKind, submit_if_at_tip};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit an AdvancePool transaction to settle rewards for this
//          node's staking pool. This is required for any node that was
//          in the current or previous committee.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "advance_pool: wrong epoch");
            return TaskDone::Rejected(Action::AdvancePool, epoch);
        }

        info!(epoch = epoch.0, "advance_pool: submitting");
        let outcome = submit_if_at_tip(&ctx.ingest, submit_advance_pool(&ctx)).await;

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "advance_pool: confirmed");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::AlreadyAdvanced),
                ..
            } => {
                info!(epoch = epoch.0, "advance_pool: already advanced");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Rejected {
                kind:
                    TxRejectionKind::Program(
                        err @ (TapeError::RewardsOverflow | TapeError::PoolAccountingFailed),
                    ),
                ..
            } => {
                warn!(epoch = epoch.0, ?err, "advance_pool: terminal program error");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err),
                ..
            } => {
                warn!(epoch = epoch.0, ?err, "advance_pool: program error");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_pool: stale submission ignored");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_pool: concurrent submission already applied");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_pool: transaction rejected");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "advance_pool: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "advance_pool: ingest stale, deferring");
                return TaskDone::Rejected(Action::AdvancePool, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::AdvancePool, epoch);
}
