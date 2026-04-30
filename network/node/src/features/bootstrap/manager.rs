//! Bootstrap: runs before supervisor and replays missing finalized
//! snapshots so the live ingestor can resume at the slot right
//! after the last replayed snapshot's end.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use rpc::{CommitmentLevel, Rpc};
use store::Store;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_protocol::Api;
use tape_store::ops::MetaOps;

use crate::config::node::NodeConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::bootstrap::{discovery, fetch, replay};

/// Run the bootstrap phase and return the slot the live ingestor should
/// start from.
///
/// When replay runs, the returned slot is the one immediately after the
/// last replayed snapshot. Otherwise the slot is resolved from (in
/// order): the explicit `config.solana.start_slot` override, the
/// persisted `sync_cursor` from prior runs, or the current epoch's
/// on-chain `start_slot`.
pub async fn run<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &NodeConfig,
    cancel: &CancellationToken,
) -> Result<SlotNumber, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let epochs = discovery::discover_missing_epochs(context.as_ref()).await?;

    if epochs.is_empty() {
        let start_slot = resolve_no_replay_start(context, config).await?;
        debug!(
            node_id = context.node_id().0,
            start_slot = start_slot.0,
            "bootstrap: nothing to replay"
        );
        return Ok(start_slot);
    }

    info!(
        node_id = context.node_id().0,
        count = epochs.len(),
        first = epochs.first().map(|e| e.0),
        last = epochs.last().map(|e| e.0),
        "bootstrap: starting replay"
    );

    let mut last_end_slot: Option<SlotNumber> = None;
    for epoch in epochs {
        if cancel.is_cancelled() {
            return Err(NodeError::Store("bootstrap: cancelled".into()));
        }

        let log = fetch::fetch_and_decode_epoch(context, epoch, cancel).await?;
        replay::apply_snapshot_log(context.store.as_ref(), &log)?;
        advance_cursors(context, epoch, log.end_slot)?;
        last_end_slot = Some(log.end_slot);

        info!(
            epoch = epoch.0,
            entries = log.entries.len(),
            end_slot = log.end_slot.0,
            "bootstrap: epoch replayed"
        );
    }

    // Resume ingestion at the slot right after the last replayed snapshot's
    // end, so there's no gap and no double-application at the live boundary.
    let start_slot = match last_end_slot {
        Some(end) => SlotNumber(end.0.saturating_add(1)),
        // Defensive: if the epochs list was non-empty we must have set
        // last_end_slot at least once. Fall through to the same
        // resolver the no-replay path uses rather than assuming.
        None => resolve_no_replay_start(context, config).await?,
    };

    info!(
        start_slot = start_slot.0,
        "bootstrap: complete, handing start slot to ingestor"
    );
    Ok(start_slot)
}

/// Pick a start slot when no snapshots need replaying. Order:
/// 1. Explicit operator override (`config.solana.start_slot`).
/// 2. Persisted `sync_cursor` from prior runs (resume where we left off).
/// 3. Current epoch's `start_slot` from chain (fresh first run).
async fn resolve_no_replay_start<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    config: &NodeConfig,
) -> Result<SlotNumber, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if let Some(override_slot) = config.solana.start_slot {
        debug!(start_slot = override_slot.0, "bootstrap: using configured start_slot override");
        return Ok(override_slot);
    }

    let cursor = context
        .store
        .get_sync_cursor()
        .map_err(|error| NodeError::Store(format!("get_sync_cursor: {error}")))?;
    if let Some(c) = cursor {
        let resume = SlotNumber(c.0.saturating_add(1));
        debug!(
            cursor = c.0,
            start_slot = resume.0,
            "bootstrap: resuming from persisted sync_cursor"
        );
        return Ok(resume);
    }

    let epoch = context
        .rpc
        .get_epoch_with_commitment(CommitmentLevel::Finalized)
        .await?;

    debug!(
        epoch = epoch.id.0,
        start_slot = epoch.start_slot.0,
        "bootstrap: fresh node, using current epoch's start_slot"
    );

    Ok(epoch.start_slot)
}

fn advance_cursors<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    epoch: EpochNumber,
    end_slot: SlotNumber,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{

    context
        .store
        .set_bootstrap_target_epoch(epoch)
        .map_err(|error| NodeError::Store(format!("set_bootstrap_target_epoch: {error}")))?;

    context
        .store
        .set_sync_cursor(end_slot)
        .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))

}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_store::ops::MetaOps;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::config::node::NodeConfig;
    use crate::context::test_utils::test_context;

    use super::{advance_cursors, run};

    #[tokio::test]
    async fn override_wins_when_nothing_to_replay() {
        let context = test_context();
        let mut config = NodeConfig::default();
        config.solana.start_slot = Some(SlotNumber(42));
        let cancel = CancellationToken::new();

        let slot = timeout(Duration::from_secs(1), run(&context, &config, &cancel))
            .await
            .expect("bootstrap completed in time")
            .expect("bootstrap returned ok");

        assert_eq!(slot, SlotNumber(42));
    }

    #[tokio::test]
    async fn sync_cursor_used_when_no_override_and_nothing_to_replay() {
        let context = test_context();
        let config = NodeConfig::default();
        let cancel = CancellationToken::new();

        context.store.set_sync_cursor(SlotNumber(999)).unwrap();

        let slot = timeout(Duration::from_secs(1), run(&context, &config, &cancel))
            .await
            .expect("bootstrap completed in time")
            .expect("bootstrap returned ok");

        assert_eq!(slot, SlotNumber(1000));
    }

    #[tokio::test]
    async fn no_op_path_leaves_cursor_untouched() {
        let context = test_context();
        let mut config = NodeConfig::default();
        // Avoid the chain fetch by forcing the override path.
        config.solana.start_slot = Some(SlotNumber(1));
        let cancel = CancellationToken::new();

        let before = context.store.get_bootstrap_target_epoch().unwrap();
        run(&context, &config, &cancel).await.unwrap();
        let after = context.store.get_bootstrap_target_epoch().unwrap();

        assert_eq!(before, after);
        assert!(after.is_none());
    }

    #[test]
    fn advance_cursors_records_bootstrap_epoch_and_sync_slot() {
        let context = test_context();
        let epoch = EpochNumber(5);
        let end_slot = SlotNumber(1234);

        advance_cursors(&context, epoch, end_slot).unwrap();

        assert_eq!(
            context.store.get_bootstrap_target_epoch().unwrap(),
            Some(epoch)
        );
        assert_eq!(context.store.get_sync_cursor().unwrap(), Some(end_slot));
    }
}
