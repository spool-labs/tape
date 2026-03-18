use std::collections::HashSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SpoolOps;
use tape_store::types::SpoolStatus;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};

use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Wait until all spools assigned to this node are Active.
//          This is a precondition for submitting SyncEpoch — the node
//          should not attest that it has synced until all spool data
//          is actually ready.
//
// Algorithm:
// 1. Loop (checking cancel each iteration):
//    a. Read current protocol state to get our committee index.
//    b. Get our assigned spools via state.member_spools(index).
//       If not in committee or no spools assigned → return Done
//       immediately (nothing to wait for).
//    c. For each assigned spool, read SpoolState from store.
//       A spool is ready if its status is Active.
//       A spool with no persisted state is not ready.
//    d. If all assigned spools are Active → return Done.
//    e. Otherwise → sleep for spool_poll_interval, then retry.
//
// The poll is cheap: at most 50 spools (MAX_SPOOL_ALLOCATION),
// each a single store lookup. The interval is configurable
// (default: 1 second).
//
// This task does not submit any transactions. It only reads
// local state. It exits via Done or Cancelled.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    
    let mut backoff = Backoff::new(RetryConfig::infinite());

    loop {
        if ctx.state().epoch != epoch {
            info!(epoch = epoch.0, "advance_epoch: epoch already advanced");
            return TaskDone::Rejected(Action::WaitSpoolReady, epoch);
        }

        match check_readiness(&ctx) {
            Readiness::Ready => {
                info!(epoch = epoch.0, "wait_spool_ready: all spools active");
                return TaskDone::Done(Action::WaitSpoolReady, epoch);
            }
            Readiness::NotReady { ready, total } => {
                debug!(epoch = epoch.0, ready, total, "wait_spool_ready: polling");
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
           break;
        }
    }

    return TaskDone::Cancelled(Action::WaitSpoolReady, epoch);
}

pub enum Readiness {
    Ready,
    NotReady { ready: usize, total: usize },
}

pub fn check_readiness<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
) -> Readiness {
    let state = ctx.state();
    let node_id = ctx.node_id();

    let Some((member_index, _)) = state.find_member(node_id) else {
        return Readiness::Ready;
    };

    let assigned: Vec<SpoolIndex> = state.member_spools(member_index);
    if assigned.is_empty() {
        return Readiness::Ready;
    }

    let assigned_set: HashSet<SpoolIndex> = assigned.iter().copied().collect();
    let persisted = ctx.store.iter_all_spools().unwrap_or_default();

    let ready_count = persisted
        .iter()
        .filter(|(spool, s)| assigned_set.contains(spool) && s.status == SpoolStatus::Active)
        .count();

    if ready_count >= assigned.len() {
        Readiness::Ready
    } else {
        Readiness::NotReady {
            ready: ready_count,
            total: assigned.len(),
        }
    }
}

