use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::Backoff;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_sync_epoch;
use crate::core::chain_tx::{TxOutcome, backoff_or_cancel, classify_tx};
use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit a SyncEpoch transaction to attest that this node
//          has synced all its assigned spool data for the current epoch.
//
// Precondition: WaitSpoolReady must have completed before this task
// is spawned. The lifecycle worker enforces this ordering.
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
//       Return Rejected. The lifecycle worker will re-evaluate and
//       skip to the next relevant action.
//    f. On NotInCommittee / BadSpoolHash / BadEpochId → return Rejected.
//    g. On retriable transport errors (RPC timeout, connection, etc.) →
//       backoff and retry within this loop.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochLifecycleConfig,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    let owned_spools = owned_spool_list(&ctx);

    info!(epoch = epoch.0, spools = owned_spools.len(), "sync_epoch: submitting");

    let mut backoff = Backoff::new(config.tx_retry);

    loop {
        if cancel.is_cancelled() {
            return TaskDone::Cancelled(Action::SyncEpoch, epoch);
        }

        let result = submit_sync_epoch(&ctx, epoch, &owned_spools).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "sync_epoch: confirmed");
                return TaskDone::Done(Action::SyncEpoch, epoch);
            }
            TxOutcome::Program(TapeError::AlreadySynced) => {
                info!(epoch = epoch.0, "sync_epoch: already synced");
                return TaskDone::Done(Action::SyncEpoch, epoch);
            }
            TxOutcome::Program(
                err @ (TapeError::BadEpochState
                | TapeError::NotInCommittee
                | TapeError::BadSpoolHash
                | TapeError::BadEpochId),
            ) => {
                warn!(epoch = epoch.0, ?err, "sync_epoch: rejected");
                return TaskDone::Rejected(Action::SyncEpoch, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "sync_epoch: unexpected program error");
                return TaskDone::Rejected(Action::SyncEpoch, epoch);
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "sync_epoch: transport error, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::SyncEpoch, epoch);
                }
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::system::EpochPhase;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Done(Action::SyncEpoch, _)));
    }

    #[tokio::test]
    async fn already_synced() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .node(NODE, |node| node.latest_sync_epoch = EPOCH)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Done(Action::SyncEpoch, _)));
    }

    #[tokio::test]
    async fn phase_past_syncing() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Rejected(Action::SyncEpoch, _)));
    }

    #[tokio::test]
    async fn not_in_committee() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .current_committee_size(20)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(24),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Rejected(Action::SyncEpoch, _)));
    }

    #[tokio::test]
    async fn immediate_cancel() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .build()
            .await
            .expect("build harness");

        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            cancel,
        )
        .await;

        assert!(matches!(result, TaskDone::Cancelled(Action::SyncEpoch, _)));
    }
}
