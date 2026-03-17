use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tokio_util::sync::CancellationToken;

use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit a SyncEpoch transaction to attest that this node
//          has synced all its assigned spool data for the current epoch.
//
// Precondition: all owned spools must be in a ready state (Active, or
// at minimum past their Sync/Scan/Repair/Recover lifecycle). Readiness
// is determined by polling the store — no cross-feature coupling.
//
// Algorithm:
// 1. Loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Poll spool readiness:
//       - Read current protocol state to get our committee index.
//       - Get our assigned spools via state.member_spools(index).
//       - For each spool, read SpoolState from store.
//       - If any spool is not Active → sleep for poll_interval, retry.
//       - If all spools are Active (or we have no spools) → proceed.
//    c. Build owned spool list (sorted).
//    d. Submit SyncEpoch transaction via rpc.send_instructions:
//       - build_epoch_sync_ix(fee_payer, authority, node_address, epoch, &spools)
//       - Wrap with compute budget instruction.
//    e. On success → return Done.
//    f. On AlreadySynced → return Done (idempotent).
//    g. On BadEpochState → the phase has moved past Syncing.
//       Return Rejected. The lifecycle worker will re-evaluate and
//       skip to the next relevant action.
//    h. On NotInCommittee / BadSpoolHash / BadEpochId → return Rejected.
//       These indicate a fundamental mismatch. The lifecycle worker
//       will re-evaluate with current state.
//    i. On retriable errors (RPC timeout, connection, etc.) →
//       backoff and retry within this loop.
//
// The task never gives up on its own for retriable errors. It only exits
// via Done, Rejected, or cancellation.
//
// Spool readiness polling:
//   We poll store.iter_all_spools() and check each owned spool's status.
//   This is at most 50 spools (MAX_SPOOL_ALLOCATION). The poll interval
//   is configurable (default: 1 second). We do NOT use a watch channel
//   from SpoolManager — polling the store avoids brittle coupling between
//   the spool and epoch features.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochLifecycleConfig,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::spooler::SpoolIndex;
    use tape_store::ops::SpoolOps;
    use tape_store::types::{SpoolState, SpoolStatus};

    use crate::core::context::test_utils::test_context;

    const EPOCH: EpochNumber = EpochNumber(3);

    // Spools all Active → should proceed to submit and return Done.
    #[tokio::test]
    #[ignore] // run() not yet implemented
    async fn all_spools_ready() {
        let ctx = test_context();
        // TODO: set protocol state with node in committee + spool assignments
        // TODO: set all owned spools to SpoolStatus::Active in store
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::SyncEpoch, _)));
    }

    // Some spools still syncing → should poll until ready or cancelled.
    // Cancel after a short delay to verify it polls and exits cleanly.
    #[tokio::test]
    #[ignore]
    async fn spools_not_ready_then_cancel() {
        let ctx = test_context();
        // TODO: set protocol state with node in committee + spool assignments
        // Set one spool to Sync (not Active)
        ctx.store
            .set_spool_state(5 as SpoolIndex, SpoolState::new(SpoolStatus::Sync, EPOCH))
            .unwrap();

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(result, TaskDone::Cancelled(Action::SyncEpoch, _)));
    }

    // No spools assigned (empty committee member) → should proceed directly.
    #[tokio::test]
    #[ignore]
    async fn no_spools_assigned() {
        let ctx = test_context();
        // TODO: set protocol state with node in committee but 0 spools
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::SyncEpoch, _)));
    }

    // On-chain program returns AlreadySynced → should return Done.
    #[tokio::test]
    #[ignore]
    async fn already_synced() {
        let ctx = test_context();
        // TODO: set up on-chain state where node has already synced
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::SyncEpoch, _)));
    }

    // Phase has moved past Syncing → should return Rejected.
    #[tokio::test]
    #[ignore]
    async fn phase_past_syncing() {
        let ctx = test_context();
        // TODO: set up on-chain state where epoch phase is Settling/Active
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Rejected(Action::SyncEpoch, _)));
    }

    // Immediate cancel → should return Cancelled without submitting.
    #[tokio::test]
    #[ignore]
    async fn immediate_cancel() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(result, TaskDone::Cancelled(Action::SyncEpoch, _)));
    }
}
