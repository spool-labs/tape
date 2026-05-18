//! Chain-side discovery of which snapshot epochs this node needs to replay.
//!
//! The epoch-advance gate guarantees that at any point in time, the snapshot
//! for either `current - 1` or `current - 2` is `Finalized` on-chain.

use rpc::{Rpc, RpcError};
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tracing::debug;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Epoch 0 is the bootstrap base and has no snapshot.
const FIRST_SNAPSHOT_EPOCH: EpochNumber = EpochNumber(1);

/// Compute the ordered range of snapshot epochs this node still needs to
/// replay, oldest -> newest.
pub async fn discover_missing_epochs<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Result<Vec<EpochNumber>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let current = context.state().epoch();
    let Some(newest) = newest_finalized_epoch(context, current).await? else {
        debug!(
            current = current.0,
            "bootstrap: no finalized snapshots on chain yet"
        );
        return Ok(Vec::new());
    };

    let cursor = context
        .store
        .get_bootstrap_target_epoch()
        .map_err(|error| NodeError::Store(format!("get_bootstrap_target_epoch: {error}")))?;

    let start = match cursor {
        Some(c) => EpochNumber(c.0.saturating_add(1)),
        None => FIRST_SNAPSHOT_EPOCH,
    };

    if start > newest {
        debug!(
            cursor = cursor.map(|e| e.0),
            newest = newest.0,
            "bootstrap: cursor already past newest finalized snapshot"
        );
        return Ok(Vec::new());
    }

    let epochs: Vec<EpochNumber> = (start.0..=newest.0)
        .map(EpochNumber).collect();

    debug!(
        cursor = cursor.map(|e| e.0),
        newest = newest.0,
        count = epochs.len(),
        "bootstrap: epochs to replay"
    );
    Ok(epochs)
}

/// Walk back from `current` to find the newest epoch whose snapshot tape
/// exists on-chain.
async fn newest_finalized_epoch<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    current: EpochNumber,
) -> Result<Option<EpochNumber>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if current.0 == 0 {
        return Ok(None);
    }

    let prev = EpochNumber(current.0 - 1);
    if is_finalized(context, prev).await? {
        return Ok(Some(prev));
    }

    if current.0 < 2 {
        return Ok(None);
    }

    let prev_prev = EpochNumber(current.0 - 2);
    if is_finalized(context, prev_prev).await? {
        Ok(Some(prev_prev))
    } else {
        Ok(None)
    }
}

async fn is_finalized<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    epoch: EpochNumber,
) -> Result<bool, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    match context.rpc.get_snapshot_tape(epoch).await {
        Ok(tape) => Ok(tape.is_snapshot_tape(epoch)),
        Err(RpcError::AccountNotFound(_)) => Ok(false),
        Err(error) => Err(NodeError::Rpc(error)),
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::program::tapedrive::{self, snapshot_pda};
    use tape_api::state::Snapshot;
    use tape_core::snapshot::types::SnapshotState;
    use tape_core::types::{EpochNumber, GroupBitmap};
    use tape_protocol::ProtocolState;
    use tape_store::ops::MetaOps;

    use super::discover_missing_epochs;
    use crate::context::test_utils::{test_context, TestContext};

    fn set_epoch(context: &TestContext, epoch: EpochNumber) {
        let mut state = ProtocolState::default();
        state.epoch = epoch;
        context.set_state(state).expect("publish state");
    }

    fn write_snapshot(context: &TestContext, epoch: EpochNumber, state: SnapshotState) {
        let snapshot = Snapshot {
            epoch,
            state: state as u64,
            group_bitmap: GroupBitmap::zeroed(),
        };
        let (address, _) = snapshot_pda(epoch);
        context
            .rpc
            .rpc()
            .set_account_data(address, tapedrive::ID, &snapshot.pack())
            .expect("write snapshot account");
    }

    #[tokio::test]
    async fn empty_when_no_finalized_snapshots() {
        let context = test_context();
        set_epoch(&context, EpochNumber(3));
        // No Snapshot account written for prev or prev-prev.
        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert!(epochs.is_empty());
    }

    #[tokio::test]
    async fn empty_at_epoch_zero() {
        let context = test_context();
        set_epoch(&context, EpochNumber(0));
        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert!(epochs.is_empty());
    }

    #[tokio::test]
    async fn range_from_first_snapshot_when_no_cursor() {
        let context = test_context();
        set_epoch(&context, EpochNumber(4));
        // prev (3) is Finalized → newest = 3, no cursor → start = 1.
        write_snapshot(&context, EpochNumber(3), SnapshotState::Finalized);

        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert_eq!(
            epochs,
            vec![
                EpochNumber(1),
                EpochNumber(2),
                EpochNumber(3),
            ]
        );
    }

    #[tokio::test]
    async fn falls_back_to_prev_prev_when_prev_not_finalized() {
        let context = test_context();
        set_epoch(&context, EpochNumber(5));
        // prev (4) is PartiallyCertified, prev_prev (3) is Finalized.
        write_snapshot(&context, EpochNumber(4), SnapshotState::PartiallyCertified);
        write_snapshot(&context, EpochNumber(3), SnapshotState::Finalized);

        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert_eq!(epochs.last(), Some(&EpochNumber(3)));
        assert_eq!(epochs.len(), 3);
    }

    #[tokio::test]
    async fn resumes_from_cursor() {
        let context = test_context();
        set_epoch(&context, EpochNumber(10));
        write_snapshot(&context, EpochNumber(9), SnapshotState::Finalized);
        context
            .store
            .set_bootstrap_target_epoch(EpochNumber(6))
            .unwrap();

        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert_eq!(
            epochs,
            vec![
                EpochNumber(7),
                EpochNumber(8),
                EpochNumber(9),
            ]
        );
    }

    #[tokio::test]
    async fn empty_when_cursor_caught_up() {
        let context = test_context();
        set_epoch(&context, EpochNumber(10));
        write_snapshot(&context, EpochNumber(9), SnapshotState::Finalized);
        // Cursor already at or past the newest finalized epoch.
        context
            .store
            .set_bootstrap_target_epoch(EpochNumber(9))
            .unwrap();

        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert!(epochs.is_empty());
    }

    #[tokio::test]
    async fn prev_prev_not_finalized_returns_empty() {
        let context = test_context();
        set_epoch(&context, EpochNumber(5));
        // Neither prev (4) nor prev_prev (3) is Finalized.
        write_snapshot(&context, EpochNumber(4), SnapshotState::Registered);
        write_snapshot(&context, EpochNumber(3), SnapshotState::PartiallyCertified);

        let epochs = discover_missing_epochs(context.as_ref()).await.unwrap();
        assert!(epochs.is_empty());
    }
}
