use std::collections::HashSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use rpc::Rpc;
use store::Store;
use tape_core::types::SpoolIndex;
use tape_core::system::{EpochPhase, SpoolStatus};
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SpoolOps;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::lifecycle::types::{Action, TaskDone};
use crate::features::spool::manager::has_pending_work;

// Purpose: Wait until all spools assigned to this node are Active.
//          This is a precondition for submitting SyncSpool, the node
//          should not attest that it has synced until all spool data
//          is actually ready.
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
        if ctx.state().epoch() != epoch {
            info!(epoch = epoch.0, "wait_spool_ready: epoch already advanced");
            return TaskDone::Rejected(Action::WaitSpoolReady, epoch);
        }

        if ctx.phase() > EpochPhase::Sync {
            info!(epoch = epoch.0, phase = ?ctx.phase(), "wait_spool_ready: past syncing phase");
            return TaskDone::Rejected(Action::WaitSpoolReady, epoch);
        }

        match check_readiness(&ctx) {
            Ok(Readiness::Ready) => {
                info!(epoch = epoch.0, "wait_spool_ready: all spools active");
                return TaskDone::Done(Action::WaitSpoolReady, epoch);
            }
            Ok(Readiness::NotReady { ready, total }) => {
                debug!(epoch = epoch.0, ready, total, "wait_spool_ready: polling");
            }
            Err(error) => {
                debug!(epoch = epoch.0, %error, "wait_spool_ready: store error, retrying");
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
) -> Result<Readiness, NodeError> {
    let state = ctx.state();
    let node = ctx.node_address();

    if state.find_member(node).is_none() {
        return Ok(Readiness::Ready);
    }

    let assigned: Vec<SpoolIndex> = state.member_spools(node);
    if assigned.is_empty() {
        return Ok(Readiness::Ready);
    }

    let assigned_set: HashSet<SpoolIndex> = assigned.iter().copied().collect();
    let persisted = ctx.store
        .iter_all_spools()
        .map_err(|e| NodeError::Store(format!("iter_all_spools: {e}")))?;

    let mut ready_count = 0usize;
    for (spool, s) in &persisted {
        if !assigned_set.contains(spool) || s.status != SpoolStatus::Active {
            continue;
        }

        let (has_repair, has_recovery) = has_pending_work(&ctx.store, *spool)?;
        if !has_repair && !has_recovery {
            ready_count += 1;
        }
    }

    if ready_count >= assigned.len() {
        info!(ready = ready_count, total = assigned.len(), "check_readiness: all assigned spools are active");
        Ok(Readiness::Ready)
    } else {
        info!(ready = ready_count, total = assigned.len(), "check_readiness: not all assigned spools are active");
        Ok(Readiness::NotReady {
            ready: ready_count,
            total: assigned.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;
    use super::*;
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::system::{CommitteeMember, EpochPhase};
    use tape_core::types::NodeId;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_protocol::ProtocolState;

    use crate::context::test_utils::test_context;

    const EPOCH: EpochNumber = EpochNumber(2);

    fn owned_state(spools: &[SpoolIndex]) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EPOCH;
        state.phase = EpochPhase::Syncing;
        state
            .committee
            .push(CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1000)));

        let mut mapping = [255u8; SPOOL_COUNT];
        for &spool in spools {
            mapping[spool as usize] = 0;
        }
        state.spools = SpoolAssignment::new(mapping);
        state
    }

    #[test]
    fn active_with_pending_not_ready() {
        let ctx = test_context();
        ctx.set_state(owned_state(&[5])).unwrap();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store.add_pending_repair(5, Address::from([1; 32])).unwrap();

        let result = check_readiness(&ctx).unwrap();
        assert!(matches!(result, Readiness::NotReady { .. }));
    }
}
