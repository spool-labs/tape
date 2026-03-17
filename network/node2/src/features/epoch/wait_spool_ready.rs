use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SpoolOps;
use tape_store::types::SpoolStatus;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::core::config::EpochLifecycleConfig;
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
    config: EpochLifecycleConfig,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    loop {
        if cancel.is_cancelled() {
            return TaskDone::Cancelled(Action::WaitSpoolReady, epoch);
        }

        match check_readiness(&ctx) {
            Readiness::Ready => {
                info!(epoch = epoch.0, "wait_spool_ready: all spools active");
                return TaskDone::Done(Action::WaitSpoolReady, epoch);
            }
            Readiness::NotReady { ready, total } => {
                debug!(epoch = epoch.0, ready, total, "wait_spool_ready: polling");
                tokio::select! {
                    _ = cancel.cancelled() => {
                        return TaskDone::Cancelled(Action::WaitSpoolReady, epoch);
                    }
                    _ = tokio::time::sleep(config.spool_poll_interval) => {}
                }
            }
        }
    }
}

enum Readiness {
    Ready,
    NotReady { ready: usize, total: usize },
}

fn check_readiness<Db: Store, Cluster: Api, Blockchain: Rpc>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::{CommitteeMember, EpochPhase};
    use tape_core::types::NodeId;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_protocol::ProtocolState;
    use tape_store::types::SpoolState;

    use crate::core::context::test_utils::{TestContext, test_context};

    const EPOCH: EpochNumber = EpochNumber(3);

    /// ProtocolState where node_id=0 is member 0, assigned the given spools.
    fn syncing_state(spools: &[SpoolIndex]) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EPOCH;
        state.phase = EpochPhase::Syncing;
        state
            .committee
            .push(CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1000)));
        let mut mapping = [255u8; SPOOL_COUNT];
        for &s in spools {
            mapping[s as usize] = 0;
        }
        state.spools = SpoolAssignment::new(mapping);
        state
    }

    fn set_spools_active(
        ctx: &TestContext,
        spools: &[SpoolIndex],
    ) {
        for &s in spools {
            ctx.store
                .set_spool_state(s, SpoolState::new(SpoolStatus::Active, EPOCH))
                .unwrap();
        }
    }

    // ── check_readiness unit tests ──────────────────────────────────

    #[test]
    fn readiness_all_active() {
        let ctx = test_context();
        let spools = vec![0, 20, 40];
        ctx.set_state(syncing_state(&spools)).unwrap();
        set_spools_active(&ctx, &spools);

        assert!(matches!(check_readiness(&ctx), Readiness::Ready));
    }

    #[test]
    fn readiness_partial() {
        let ctx = test_context();
        let spools = vec![0, 20, 40];
        ctx.set_state(syncing_state(&spools)).unwrap();
        set_spools_active(&ctx, &[0, 40]);

        match check_readiness(&ctx) {
            Readiness::NotReady { ready, total } => {
                assert_eq!(ready, 2);
                assert_eq!(total, 3);
            }
            Readiness::Ready => panic!("expected not ready"),
        }
    }

    #[test]
    fn readiness_none_persisted() {
        let ctx = test_context();
        let spools = vec![0, 20];
        ctx.set_state(syncing_state(&spools)).unwrap();
        // No spool states in store at all.

        match check_readiness(&ctx) {
            Readiness::NotReady { ready, total } => {
                assert_eq!(ready, 0);
                assert_eq!(total, 2);
            }
            Readiness::Ready => panic!("expected not ready"),
        }
    }

    #[test]
    fn readiness_not_in_committee() {
        let ctx = test_context();
        // Default state: empty committee, node_id=0 not a member.
        assert!(matches!(check_readiness(&ctx), Readiness::Ready));
    }

    #[test]
    fn readiness_no_spools_assigned() {
        let ctx = test_context();
        // In committee but no spools mapped to member 0.
        let mut state = ProtocolState::default();
        state.phase = EpochPhase::Syncing;
        state
            .committee
            .push(CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1000)));
        state.spools = SpoolAssignment::new([255u8; SPOOL_COUNT]);
        ctx.set_state(state).unwrap();

        assert!(matches!(check_readiness(&ctx), Readiness::Ready));
    }

    #[test]
    fn readiness_wrong_status() {
        let ctx = test_context();
        let spools = vec![0, 20];
        ctx.set_state(syncing_state(&spools)).unwrap();
        set_spools_active(&ctx, &[0]);
        ctx.store
            .set_spool_state(20, SpoolState::new(SpoolStatus::Repair, EPOCH))
            .unwrap();

        match check_readiness(&ctx) {
            Readiness::NotReady { ready, total } => {
                assert_eq!(ready, 1);
                assert_eq!(total, 2);
            }
            Readiness::Ready => panic!("expected not ready"),
        }
    }

    // ── run() integration tests ─────────────────────────────────────

    #[tokio::test]
    async fn immediate_cancel() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(
            result,
            TaskDone::Cancelled(Action::WaitSpoolReady, _)
        ));
    }

    #[tokio::test]
    async fn already_ready() {
        let ctx = test_context();
        let spools = vec![0, 20, 40];
        ctx.set_state(syncing_state(&spools)).unwrap();
        set_spools_active(&ctx, &spools);

        let result =
            run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(
            result,
            TaskDone::Done(Action::WaitSpoolReady, _)
        ));
    }

    #[tokio::test]
    async fn not_in_committee() {
        let ctx = test_context();
        // Default state: not in committee → ready immediately.
        let result =
            run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(
            result,
            TaskDone::Done(Action::WaitSpoolReady, _)
        ));
    }

    #[tokio::test]
    async fn polls_until_ready() {
        let ctx = test_context();
        let spools = vec![0, 20];
        ctx.set_state(syncing_state(&spools)).unwrap();
        set_spools_active(&ctx, &[0]); // spool 20 not ready yet

        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            ctx_clone
                .store
                .set_spool_state(20, SpoolState::new(SpoolStatus::Active, EPOCH))
                .unwrap();
        });

        let mut config = EpochLifecycleConfig::default();
        config.spool_poll_interval = Duration::from_millis(10);

        let result = run(ctx, config, EPOCH, CancellationToken::new()).await;
        assert!(matches!(
            result,
            TaskDone::Done(Action::WaitSpoolReady, _)
        ));
    }

    #[tokio::test]
    async fn cancel_while_polling() {
        let ctx = test_context();
        let spools = vec![0, 20];
        ctx.set_state(syncing_state(&spools)).unwrap();
        // spool 20 never becomes ready

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let mut config = EpochLifecycleConfig::default();
        config.spool_poll_interval = Duration::from_millis(10);

        let result = run(ctx, config, EPOCH, cancel).await;
        assert!(matches!(
            result,
            TaskDone::Cancelled(Action::WaitSpoolReady, _)
        ));
    }
}
