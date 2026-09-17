use std::sync::Arc;
use std::time::Duration;

use tape_rpc::{NUM_CONSECUTIVE_LEADER_SLOTS, Rpc};
use tape_store_api::Store;
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
use crate::features::lifecycle::manager::next_committee_filled;
use crate::features::lifecycle::types::{Action, TaskDone};

// Purpose: Submit a JoinCommittee transaction to volunteer for the next
//          epoch's committee. This signals the node intends to serve
//          in the upcoming epoch.

/// Slots past the tip known at confirmation by which the seating block is ingested
const JOIN_VISIBLE_SLOTS: u64 = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
/// How often the seating wait re-reads ingest when no state change wakes it
const SEATING_POLL: Duration = Duration::from_secs(1);

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
                // Ingest past this slot has seen the seating block
                let progress = ctx.ingest.progress();
                let landed_by = progress
                    .known_tip()
                    .unwrap_or(0)
                    .max(progress.last_dispatched_slot())
                    .saturating_add(JOIN_VISIBLE_SLOTS);
                // Hold Done until the membership shows up in ingested state.
                // The planner replans the moment a task returns, and until the
                // join is observed it still reads this node as absent, so
                // returning on confirmation alone resubmits the join once per
                // replan for as long as ingest lags the chain.
                loop {
                    let state = ctx.state();
                    if state.epoch() != epoch {
                        return TaskDone::Rejected(Action::JoinCommittee, epoch);
                    }
                    if state.find_member_next(ctx.node_address()).is_some() {
                        return TaskDone::Done(Action::JoinCommittee, epoch);
                    }
                    // A confirmed join does not promise a seat: a full committee
                    // seats by stake, so a later higher-stake join can bump this
                    // node back out. Once the committee reads full without us
                    // the wait would never end, and holding the task here starves
                    // the very replan that rejoins when an eviction frees a seat.
                    if next_committee_filled(&state) {
                        info!(epoch = epoch.0, "join_committee: outcompeted, seated members all outrank us");
                        return TaskDone::Done(Action::JoinCommittee, epoch);
                    }
                    // Unseated by a later join with a seat open since, which an
                    // eviction landing behind the joins leaves; return so the
                    // planner can join again.
                    if ctx.ingest.progress().last_dispatched_slot() > landed_by {
                        info!(epoch = epoch.0, "join_committee: unseated with a seat open, rejoining");
                        return TaskDone::Rejected(Action::JoinCommittee, epoch);
                    }
                    // Ingest moves without a state change when nothing lands,
                    // which is exactly the wedge, so the wait also wakes on time.
                    tokio::select! {
                        changed = state_rx.changed() => {
                            if changed.is_err() {
                                return TaskDone::Cancelled(Action::JoinCommittee, epoch);
                            }
                        }
                        _ = tokio::time::sleep(SEATING_POLL) => {}
                        _ = cancel.cancelled() => {
                            return TaskDone::Cancelled(Action::JoinCommittee, epoch);
                        }
                    }
                }
            }
            TxOutcome::Rejected {
                kind: TxRejectionKind::Program(
                    err @ (TapeError::NodeStale | TapeError::NotStaked | TapeError::NodeSuspended),
                ),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::{run, JOIN_VISIBLE_SLOTS};
    use crate::core::ingest::IngestState;
    use crate::features::lifecycle::types::{Action, TaskDone};
    use crate::harness::{NodeHarness, TestContext};

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;
    const CAPACITY: usize = 20;
    const TIP: u64 = 10;

    // a next committee one seat short of capacity that this node is not in,
    // with ingest at a known tip so the join submits
    async fn short_committee() -> (NodeHarness, TestContext) {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .next_committee_size(CAPACITY)
            .next_committee_nodes(0..CAPACITY - 1)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        ctx.ingest.publish(IngestState::AtTip);
        ctx.ingest.progress().record_tip(TIP);
        ctx.ingest.progress().record_dispatched(TIP);
        (harness, ctx)
    }

    // seated by its own join, unseated by a later one, and a seat freed since:
    // once ingest is past the seating block the task returns so the planner joins again
    #[tokio::test]
    async fn unseated_with_a_seat_open_returns() {
        let (_harness, ctx) = short_committee().await;
        let mut task = tokio::spawn(run(ctx.clone(), EPOCH, CancellationToken::new()));
        assert!(timeout(Duration::from_secs(2), &mut task).await.is_err(), "waiting on the seat");

        // Ingest moves past the seating block with no state change
        ctx.ingest.progress().record_dispatched(TIP + JOIN_VISIBLE_SLOTS + 1);

        let done = timeout(Duration::from_secs(10), task)
            .await
            .expect("the task returned")
            .expect("join task");

        assert!(matches!(done, TaskDone::Rejected(Action::JoinCommittee, EPOCH)), "{done:?}");
        let committee = ctx.rpc.get_committee(EPOCH.next()).await.expect("next committee");
        assert!(committee.iter().any(|member| member.node == ctx.node_address()), "the join landed");
    }

    // before ingest reaches the seating block, absence is lag and the task keeps waiting
    #[tokio::test]
    async fn absent_before_the_seating_block_waits() {
        let (_harness, ctx) = short_committee().await;
        let cancel = CancellationToken::new();
        let mut task = tokio::spawn(run(ctx.clone(), EPOCH, cancel.clone()));

        assert!(timeout(Duration::from_secs(3), &mut task).await.is_err(), "still waiting");
        let committee = ctx.rpc.get_committee(EPOCH.next()).await.expect("next committee");
        assert!(committee.iter().any(|member| member.node == ctx.node_address()), "the join landed");
        cancel.cancel();
    }
}
