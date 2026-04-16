//! Bootstrap: runs before supervisor and replays missing finalized 
//! snapshots so the live ingestor can resume at the slot right 
//! after the last replayed snapshot's end.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::node::NodeConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::bootstrap::{discovery, fetch, replay};

/// Run the bootstrap phase and return the slot the live ingestor should
/// start from.
///
/// Returns `config.solana.block_start_slot()` when there is nothing to
/// replay (no finalized snapshots on-chain yet, or the cursor is already at
/// the tip).
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
        let start_slot = config.solana.block_start_slot();
        debug!(
            node_id = context.node_id().0,
            start_slot = start_slot.0,
            "bootstrap: nothing to replay, using configured start slot"
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
        advance_cursor(context, epoch)?;
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
        None => config.solana.block_start_slot(),
    };

    info!(
        start_slot = start_slot.0,
        "bootstrap: complete, handing start slot to ingestor"
    );
    Ok(start_slot)
}

fn advance_cursor<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    epoch: EpochNumber,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    context
        .store
        .set_bootstrap_target_epoch(epoch)
        .map_err(|error| NodeError::Store(format!("set_bootstrap_target_epoch: {error}")))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::config::node::NodeConfig;
    use crate::context::test_utils::test_context;

    use super::run;

    #[tokio::test]
    async fn returns_configured_start_slot_when_nothing_to_replay() {
        let context = test_context();
        let config = NodeConfig::default();
        let cancel = CancellationToken::new();

        let slot = timeout(Duration::from_secs(1), run(&context, &config, &cancel))
            .await
            .expect("bootstrap completed in time")
            .expect("bootstrap returned ok");

        assert_eq!(slot, config.solana.block_start_slot());
    }

    #[tokio::test]
    async fn no_op_path_leaves_cursor_untouched() {
        use tape_store::ops::MetaOps;

        let context = test_context();
        let config = NodeConfig::default();
        let cancel = CancellationToken::new();

        let before = context.store.get_bootstrap_target_epoch().unwrap();
        run(&context, &config, &cancel).await.unwrap();
        let after = context.store.get_bootstrap_target_epoch().unwrap();

        assert_eq!(before, after);
        assert!(after.is_none());
    }
}
