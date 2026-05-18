use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_join_committee;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
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

        info!(epoch = epoch.0, "join_committee: submitting");

        let outcome = submit_if_at_tip(&ctx.ingest, submit_join_committee(&ctx)).await;

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, ?sig, "join_committee: confirmed");
                return TaskDone::Done(Action::JoinCommittee, epoch);
            }
            TxOutcome::Program(
                err @ (TapeError::NodeStale | TapeError::NotStaked),
            ) => {
                warn!(epoch = epoch.0, ?err, "join_committee: rejected, node prerequisites not met");
                return TaskDone::Rejected(Action::JoinCommittee, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "join_committee: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "join_committee: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "join_committee: ingest stale, deferring");
                return TaskDone::Rejected(Action::JoinCommittee, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::JoinCommittee, epoch);
}
