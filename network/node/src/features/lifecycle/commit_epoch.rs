use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_commit_epoch;
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, TxRejectionKind, submit_if_at_tip};
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit a CommitEpoch transaction after the active epoch duration
//          has elapsed. This captures the next epoch nonce and moves the
//          current epoch into Closing, where assignment finalization happens.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "commit_epoch: wrong epoch");
            return TaskDone::Rejected(Action::CommitEpoch, epoch);
        }

        match ctx.phase() {
            EpochPhase::Active => {}
            EpochPhase::Closing | EpochPhase::Completed => {
                info!(epoch = epoch.0, phase = ?ctx.phase(), "commit_epoch: already committed");
                return TaskDone::Done(Action::CommitEpoch, epoch);
            }
            phase => {
                info!(epoch = epoch.0, ?phase, "commit_epoch: outside active phase");
                return TaskDone::Rejected(Action::CommitEpoch, epoch);
            }
        }

        info!(epoch = epoch.0, "commit_epoch: submitting");
        let outcome = submit_if_at_tip(&ctx.ingest, submit_commit_epoch(&ctx)).await;

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "commit_epoch: confirmed");
                return TaskDone::Done(Action::CommitEpoch, epoch);
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(TapeError::BadEpochState),
                ..
            } => {
                debug!(epoch = epoch.0, "commit_epoch: phase already changed, waiting for state update");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err),
                ..
            } => {
                warn!(epoch = epoch.0, ?err, "commit_epoch: program error");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "commit_epoch: stale submission ignored");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "commit_epoch: concurrent submission already applied");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "commit_epoch: transaction rejected");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "commit_epoch: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "commit_epoch: ingest stale, deferring");
                return TaskDone::Rejected(Action::CommitEpoch, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
            break;
        }
    }

    TaskDone::Cancelled(Action::CommitEpoch, epoch)
}
