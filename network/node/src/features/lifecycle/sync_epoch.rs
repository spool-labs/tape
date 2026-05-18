use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::types::SpoolIndex;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_sync_epoch;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};
use crate::features::lifecycle::wait_spool_ready::{Readiness, check_readiness};

// Purpose: Submit a SyncEpoch transaction to attest that this node
//          has synced all its assigned spool data for the current epoch.
//
// Precondition: WaitSpoolReady must have completed before this task
// is spawned. The lifecycle manager enforces this ordering.
//
// Algorithm:
// 1. Read current protocol state to get our committee index and
//    assigned spools. Build a sorted spool list.
// 2. Submit loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit SyncEpoch transaction via submit_sync_epoch.
//    c. On success → return Done.
//    d. On AlreadySynced → return Done (idempotent).
//    e. On BadEpochState → the phase has moved past Syncing.
//       Return Rejected. The lifecycle manager will re-evaluate and
//       skip to the next relevant action.
//    f. On NotInCommittee / BadSpoolHash / BadEpochId → return Rejected.
//    g. On retriable transport errors (RPC timeout, connection, etc.) →
//       backoff and retry within this loop.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let owned_spools = owned_spool_list(&ctx);
    if owned_spools.is_empty() {
        info!(epoch = epoch.0, "sync_epoch: no spools assigned");
        // We don't return early here because we still want to submit the SyncEpoch transaction
    }

    match check_readiness(&ctx) {
        Ok(Readiness::NotReady { ready, total }) => {
            debug!(epoch = epoch.0, ready, total, "sync_epoch: not ready to sync");
            return TaskDone::Rejected(Action::SyncEpoch, epoch);
        }
        Err(error) => {
            debug!(epoch = epoch.0, %error, "sync_epoch: readiness check failed");
            return TaskDone::Rejected(Action::SyncEpoch, epoch);
        }
        Ok(Readiness::Ready) => {}
    }

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch != epoch {
            info!(epoch = epoch.0, "sync_epoch: epoch already advanced");
            return TaskDone::Rejected(Action::SyncEpoch, epoch);
        }

        if ctx.phase() > EpochPhase::Syncing {
            info!(epoch = epoch.0, phase = ?ctx.phase(), "sync_epoch: past syncing phase");
            return TaskDone::Rejected(Action::SyncEpoch, epoch);
        }

        info!(epoch = epoch.0, "sync_epoch: submitting");
        let outcome = submit_if_at_tip(
            &ctx.ingest,
            submit_sync_epoch(&ctx, epoch, &owned_spools),
        )
        .await;

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, ?sig, "sync_epoch: confirmed");
                return TaskDone::Done(Action::SyncEpoch, epoch);
            }
            TxOutcome::Program(TapeError::AlreadySynced) => {
                info!(epoch = epoch.0, "sync_epoch: already synced");
                return TaskDone::Done(Action::SyncEpoch, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "sync_epoch: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "sync_epoch: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, "sync_epoch: ingest stale, deferring");
                return TaskDone::Rejected(Action::SyncEpoch, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::SyncEpoch, epoch);
}

fn owned_spool_list<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
) -> Vec<SpoolIndex> {

    let state = ctx.state();
    let Some((member_index, _)) = state.find_member(ctx.node_id()) else {
        return Vec::new();
    };

    let mut spools = state.member_spools(member_index);
    spools.sort_unstable();
    spools
}
