use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_join_committee;
use crate::core::chain_tx::{submit_if_at_tip, wait_by_pace, TxOutcome, TxRejectionKind};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit a JoinCommittee transaction to volunteer for the next
//          epoch's committee. This signals the node intends to serve
//          in the upcoming epoch.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut state_rx = ctx.subscribe_state();
    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        let has_joined = ctx.state().find_member_next(ctx.node_address()).is_some();

        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "join_committee: wrong epoch");
            return TaskDone::Rejected(Action::JoinCommittee, epoch);
        }

        if has_joined {
            info!(epoch = epoch.0, "join_committee: already in next committee");
            return TaskDone::Done(Action::JoinCommittee, epoch);
        }

        // Join is only accepted while the epoch is Active; once it closes the
        // program rejects it as BadEpochState, so stop rather than resubmit.
        if !matches!(ctx.phase(), EpochPhase::Active) {
            info!(epoch = epoch.0, phase = ?ctx.phase(), "join_committee: window closed");
            return TaskDone::Rejected(Action::JoinCommittee, epoch);
        }

        info!(epoch = epoch.0, "join_committee: submitting");

        let outcome =
            submit_if_at_tip(&ctx.ingest, "join_committee", submit_join_committee(&ctx)).await;
        let pace = outcome.retry_pace();

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "join_committee: confirmed");
                return TaskDone::Done(Action::JoinCommittee, epoch);
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err @ (TapeError::NodeStale | TapeError::NotStaked)),
                ..
            } => {
                warn!(epoch = epoch.0, ?err, "join_committee: rejected, node prerequisites not met");
                return TaskDone::Rejected(Action::JoinCommittee, epoch);
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(err),
                ..
            } => {
                warn!(epoch = epoch.0, ?err, "join_committee: program error");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownStaleState,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "join_committee: stale submission ignored");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::KnownContention,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "join_committee: concurrent submission already applied");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::UnknownExecution,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "join_committee: transaction rejected");
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Transport,
                err,
            } => {
                debug!(epoch = epoch.0, %err, "join_committee: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "join_committee: ingest stale, deferring");
                return TaskDone::Rejected(Action::JoinCommittee, epoch);
            }
        }

        if wait_by_pace(pace, &mut backoff, &mut state_rx, &cancel).await {
            break;
        }
    }

    TaskDone::Cancelled(Action::JoinCommittee, epoch)
}
