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

/// Epoch 0 carries the bootstrap control-plane events needed after RPC
/// history has been pruned, so a finalized network snapshots it like every
/// other epoch.
const FIRST_SNAPSHOT_EPOCH: EpochNumber = EpochNumber(0);

/// Compute the ordered range of snapshot epochs this node still needs to
/// replay, oldest -> newest.
pub async fn discover_missing_epochs<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    current: EpochNumber,
) -> Result<Vec<EpochNumber>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
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
        Some(c) => c.next(),
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
    use tape_api::program::tapedrive::{self, snapshot_tape_pda};
    use tape_api::state::Tape;
    use tape_core::types::EpochNumber;
    use tape_store::ops::MetaOps;

    use super::discover_missing_epochs;
    use crate::harness::{NodeHarness, TestContext};

    async fn context_at(epoch: EpochNumber) -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .epoch(epoch)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    fn write_snapshot_tape(context: &TestContext, epoch: EpochNumber) {
        let tape = Tape::snapshot(epoch);
        let (address, _) = snapshot_tape_pda(epoch);
        context
            .rpc
            .rpc()
            .set_account_data(address, tapedrive::ID, &tape.pack())
            .expect("write snapshot tape");
    }

    #[tokio::test]
    async fn empty_when_no_finalized_snapshots() {
        let context = context_at(EpochNumber(3)).await;
        // No snapshot tape written for prev or prev-prev.
        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
        assert!(epochs.is_empty());
    }

    #[tokio::test]
    async fn empty_at_epoch_zero() {
        let context = context_at(EpochNumber(0)).await;
        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
        assert!(epochs.is_empty());
    }

    #[tokio::test]
    async fn range_from_first_snapshot_when_no_cursor() {
        let context = context_at(EpochNumber(4)).await;
        // prev (3) has a finalized snapshot tape, no cursor -> start = 0.
        write_snapshot_tape(&context, EpochNumber(3));

        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
        assert_eq!(
            epochs,
            vec![
                EpochNumber(0),
                EpochNumber(1),
                EpochNumber(2),
                EpochNumber(3),
            ]
        );
    }

    #[tokio::test]
    async fn falls_back_to_prev_prev_when_prev_missing() {
        let context = context_at(EpochNumber(5)).await;
        // prev (4) has no snapshot tape, prev_prev (3) does.
        write_snapshot_tape(&context, EpochNumber(3));

        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
        assert_eq!(epochs.last(), Some(&EpochNumber(3)));
        assert_eq!(epochs.len(), 4);
    }

    #[tokio::test]
    async fn resumes_from_cursor() {
        let context = context_at(EpochNumber(10)).await;
        write_snapshot_tape(&context, EpochNumber(9));
        context
            .store
            .set_bootstrap_target_epoch(EpochNumber(6))
            .unwrap();

        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
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
        let context = context_at(EpochNumber(10)).await;
        write_snapshot_tape(&context, EpochNumber(9));
        // Cursor already at or past the newest finalized epoch.
        context
            .store
            .set_bootstrap_target_epoch(EpochNumber(9))
            .unwrap();

        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
        assert!(epochs.is_empty());
    }

    #[tokio::test]
    async fn prev_prev_missing_returns_empty() {
        let context = context_at(EpochNumber(5)).await;
        // Neither prev (4) nor prev_prev (3) has a snapshot tape.

        let epochs = discover_missing_epochs(context.as_ref(), context.state().epoch())
            .await
            .unwrap();
        assert!(epochs.is_empty());
    }
}
