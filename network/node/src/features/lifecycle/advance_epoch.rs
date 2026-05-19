use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_advance_epoch;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit an AdvanceEpoch transaction to advance the network
//          to the next epoch. Any committee member can submit this.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "advance_epoch: epoch already advanced");
            return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
        }

        info!(epoch = epoch.0, "advance_epoch: submitting");
        let outcome = submit_if_at_tip(&ctx.ingest, submit_advance_epoch(&ctx)).await;

        match outcome {
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
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "advance_epoch: ingest stale, deferring");
                return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::AdvanceEpoch, epoch);
}
