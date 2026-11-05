use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::spooler::{GroupFold, GroupIndex};
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_protocol::Api;
use tape_recover::state::is_ready_to_attest;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tape_store::error::TapeStoreError;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Wait until this node is ready to attest every spool it owns.
//          This is a precondition for submitting SyncSpool, the node
//          should not attest that it has synced until it has pulled in
//          what was on each spool when it took the spool over.
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
                info!(epoch = epoch.0, "wait_spool_ready: every spool ready to attest");
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

    TaskDone::Cancelled(Action::WaitSpoolReady, epoch)
}

pub enum Readiness {
    Ready,
    NotReady { ready: usize, total: usize },
}

/// Whether every live spool this node owns is ready to attest
pub fn check_readiness<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
) -> Result<Readiness, NodeError> {
    let state = ctx.state();
    let node = ctx.node_address();

    if state.find_member(node).is_none() {
        return Ok(Readiness::Ready);
    }

    let owned = state.member_live_spools(node);
    let Some(fold) = state.fold() else {
        return Ok(Readiness::Ready);
    };

    let mut ready = 0usize;
    for spool in &owned {
        if is_spool_ready(&ctx.store, *spool, fold).map_err(store_error)? {
            ready += 1;
        }
    }

    if ready == owned.len() {
        info!(ready, total = owned.len(), "check_readiness: every owned spool ready to attest");
        return Ok(Readiness::Ready);
    }

    info!(ready, total = owned.len(), "check_readiness: not every owned spool ready to attest");
    Ok(Readiness::NotReady { ready, total: owned.len() })
}

/// Whether a live spool is ready to attest, by the spool_states of the
/// stored spools it carries and its handover missing tracks
fn is_spool_ready<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    fold: GroupFold,
) -> Result<bool, TapeStoreError> {
    let states = store.get_live_spool_states(spool, fold)?;
    let has_handover = store.has_handover_tracks(spool, fold)?;
    let carried = fold.carried_count(GroupIndex::containing(spool));
    Ok(is_ready_to_attest(&states, carried, has_handover))
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::address::Address;
    use tape_recover::state::SpoolState;
    use tape_recover::track::MissingTrack;
    use tape_recover::time::WallTime;

    use crate::harness::{NodeHarness, TestContext};

    const EPOCH: EpochNumber = EpochNumber(2);

    async fn harness() -> NodeHarness {
        NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
    }

    fn track() -> Address {
        Address::from([1; 32])
    }

    /// Whether the gate opens once every owned spool's filling is done and
    /// `change` has run on the first one
    async fn is_ready_with(change: fn(&TestContext, SpoolIndex)) -> bool {
        let harness = harness().await;
        let ctx = harness.ctx_for(0);
        let spools = harness.owned_spools(0);
        let done = SpoolState { is_scanning: false, ..SpoolState::new_scanning() };
        let mut rows = Vec::new();
        for spool in &spools {
            rows.push((*spool, done));
        }
        ctx.store.put_spool_states(&rows).expect("spool rows");
        change(&ctx, spools[0]);
        matches!(check_readiness(&ctx).expect("readiness"), Readiness::Ready)
    }

    // a fresh upload's entry leaves the gate open, and a handover entry or a
    // spool still filling keeps it shut
    #[tokio::test]
    async fn gate_by_entries() {
        assert!(is_ready_with(|_, _| {}).await);
        assert!(
            is_ready_with(|ctx, spool| {
                let upload = MissingTrack::new_upload(spool, track(), WallTime(1));
                ctx.store.put_missing_track(&upload).expect("entry");
            })
            .await
        );
        assert!(
            !is_ready_with(|ctx, spool| {
                let handover = MissingTrack::new_handover(spool, track(), WallTime(1));
                ctx.store.put_missing_track(&handover).expect("entry");
            })
            .await
        );
        assert!(
            !is_ready_with(|ctx, spool| {
                let scanning = SpoolState::new_scanning();
                ctx.store.put_spool_states(&[(spool, scanning)]).expect("row");
            })
            .await
        );
    }

    // a spool with no rows yet is not ready
    #[tokio::test]
    async fn no_rows() {
        let harness = harness().await;
        let ctx = harness.ctx_for(0);

        let result = check_readiness(&ctx).expect("readiness");

        assert!(matches!(result, Readiness::NotReady { .. }));
    }
}
