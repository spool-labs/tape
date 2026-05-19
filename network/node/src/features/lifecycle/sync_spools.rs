use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::system::EpochPhase;
use tape_core::types::BitmapRead;
use tape_core::types::EpochNumber;
use tape_core::types::SpoolIndex;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_sync_spool;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::context::NodeContext;
use crate::features::lifecycle::types::{Action, TaskDone};
use crate::features::lifecycle::wait_spool_ready::{Readiness, check_readiness};

// Purpose: Submit SyncSpool transactions to attest that this node has synced
//          its assigned spool data for the current epoch.
//
// Precondition: WaitSpoolReady must have completed before this task
// is spawned. The lifecycle manager enforces this ordering.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {

    let mut completed = HashSet::new();

    if owned_spool_list(&ctx).is_empty() {
        info!(epoch = epoch.0, "sync_spools: no spools assigned");
        return TaskDone::Done(Action::SyncSpools, epoch);
    }

    match check_readiness(&ctx) {
        Ok(Readiness::NotReady { ready, total }) => {
            debug!(epoch = epoch.0, ready, total, "sync_spools: not ready to sync");
            return TaskDone::Rejected(Action::SyncSpools, epoch);
        }
        Err(error) => {
            debug!(epoch = epoch.0, %error, "sync_spools: readiness check failed");
            return TaskDone::Rejected(Action::SyncSpools, epoch);
        }
        Ok(Readiness::Ready) => {}
    }

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "sync_spools: epoch already advanced");
            return TaskDone::Rejected(Action::SyncSpools, epoch);
        }

        if ctx.phase() > EpochPhase::Sync {
            info!(epoch = epoch.0, phase = ?ctx.phase(), "sync_spools: past syncing phase");
            return TaskDone::Rejected(Action::SyncSpools, epoch);
        }

        let Some(spool) = pending_spool_list(&ctx, &completed).into_iter().next() else {
            info!(epoch = epoch.0, "sync_spools: all spools synced");
            return TaskDone::Done(Action::SyncSpools, epoch);
        };

        info!(epoch = epoch.0, %spool, "sync_spools: submitting");
        let outcome = submit_if_at_tip(&ctx.ingest, submit_sync_spool(&ctx, epoch, spool)).await;

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %spool, %sig, "sync_spools: confirmed");
                completed.insert(spool);
            }
            TxOutcome::Program(TapeError::AlreadySynced) => {
                info!(epoch = epoch.0, %spool, "sync_spools: already synced");
                completed.insert(spool);
            }
            TxOutcome::Program(
                err @ (TapeError::BadEpochState
                | TapeError::BadEpochId
                | TapeError::NotInCommittee
                | TapeError::BadSpoolHash),
            ) => {
                warn!(epoch = epoch.0, %spool, ?err, "sync_spools: rejected");
                return TaskDone::Rejected(Action::SyncSpools, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, %spool, ?err, "sync_spools: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %spool, %err, "sync_spools: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, %spool, "sync_spools: ingest stale, deferring");
                return TaskDone::Rejected(Action::SyncSpools, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::SyncSpools, epoch);
}

fn owned_spool_list<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
) -> Vec<SpoolIndex> {

    let state = ctx.state();
    let node = ctx.node_address();
    if state.find_member(node).is_none() {
        return Vec::new();
    }

    let mut spools = state.member_spools(node);
    spools.sort_unstable();
    spools
}

fn pending_spool_list<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    completed: &HashSet<SpoolIndex>,
) -> Vec<SpoolIndex> {
    let state = ctx.state();
    let mut spools = owned_spool_list(ctx)
        .into_iter()
        .filter(|spool| !completed.contains(spool))
        .filter(|spool| {
            let group = tape_core::spooler::GroupIndex::containing(*spool);
            let Some(position) = group.position_of(*spool) else {
                return true;
            };
            state
                .group(group)
                .map(|group| !group.synced.is_set(position))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    spools.sort_unstable();
    spools
}
