use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::spooler::GroupIndex;
use tape_core::system::EpochPhase;
use tape_core::types::BitmapRead;
use tape_core::types::EpochNumber;
use tape_core::types::SpoolIndex;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_settle_spool;
use crate::context::NodeContext;
use crate::core::chain_tx::{TxOutcome, submit_if_at_tip};
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit one SettleSpool transaction per spool this node owned in the
//          previous epoch, crediting per-spool rewards into the local pool.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    let mut completed = HashSet::new();

    if owned_spool_list(&ctx).is_empty() {
        info!(epoch = epoch.0, "settle_spools: no previous spools assigned");
        return TaskDone::Done(Action::SettleSpools, epoch);
    }

    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "settle_spools: epoch already advanced");
            return TaskDone::Rejected(Action::SettleSpools, epoch);
        }

        match ctx.phase() {
            EpochPhase::Settle | EpochPhase::Snapshot | EpochPhase::Active => {}
            phase => {
                info!(epoch = epoch.0, ?phase, "settle_spools: outside settlement window");
                return TaskDone::Rejected(Action::SettleSpools, epoch);
            }
        }

        let Some(spool) = pending_spool_list(&ctx, &completed).into_iter().next() else {
            info!(epoch = epoch.0, "settle_spools: all previous spools settled");
            return TaskDone::Done(Action::SettleSpools, epoch);
        };

        info!(epoch = epoch.0, %spool, "settle_spools: submitting");
        let outcome = submit_if_at_tip(&ctx.ingest, submit_settle_spool(&ctx, spool)).await;

        match outcome {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %spool, ?sig, "settle_spools: confirmed");
                completed.insert(spool);
            }
            TxOutcome::Program(TapeError::AlreadySettled) => {
                info!(epoch = epoch.0, %spool, "settle_spools: already settled");
                completed.insert(spool);
            }
            TxOutcome::Program(
                err @ (TapeError::BadEpochId
                | TapeError::NotInCommittee
                | TapeError::BadSpoolHash
                | TapeError::RewardsOverflow),
            ) => {
                warn!(epoch = epoch.0, %spool, ?err, "settle_spools: rejected");
                return TaskDone::Rejected(Action::SettleSpools, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, %spool, ?err, "settle_spools: program error");
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %spool, %err, "settle_spools: transport error");
            }
            TxOutcome::SkippedStale => {
                debug!(epoch = epoch.0, %spool, "settle_spools: ingest stale, deferring");
                return TaskDone::Rejected(Action::SettleSpools, epoch);
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
            break;
        }
    }

    TaskDone::Cancelled(Action::SettleSpools, epoch)
}

fn owned_spool_list<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
) -> Vec<SpoolIndex> {
    let state = ctx.state();
    let node = ctx.node_address();
    if state.find_member_prev(node).is_none() {
        return Vec::new();
    }

    let mut spools = state.member_spools_prev(node);
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
            let group = GroupIndex::containing(*spool);
            let Some(position) = group.position_of(*spool) else {
                return true;
            };
            state
                .group_prev(group)
                .map(|group| !group.settled.is_set(position))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    spools.sort_unstable();
    spools
}
