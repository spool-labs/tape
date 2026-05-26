//! Bootstrap catch-up runs before supervisor startup. It may fetch current
//! protocol state for peer discovery and planning, but historical blocks are
//! applied only through the replay/store path. Live services start after the
//! store has caught up to the checkpoint boundary.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use rpc::{CommitmentLevel, Rpc};
use store::Store;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_protocol::{fetch::fetch_state_with_commitment, Api, ProtocolState};
use tape_retry::{retry_if, RetryConfig};
use tape_store::ops::MetaOps;

use crate::config::node::NodeConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::bootstrap::{block, discovery, fetch};
use crate::features::replay::engine::ReplayEngine;

const BOOTSTRAP_EPOCH: EpochNumber = EpochNumber(0);
const FIRST_LIVE_EPOCH: EpochNumber = EpochNumber(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapReplayPhase {
    BlockReplay {
        start_slot: SlotNumber,
        end_slot: SlotNumber,
        start_epoch: EpochNumber,
    },
    SnapshotReplay {
        epoch: EpochNumber,
    },
    LiveReplay {
        start_slot: SlotNumber,
    },
}

#[derive(Debug, Clone)]
struct ProtocolCheckpoint {
    state: ProtocolState,
    slot: SlotNumber,
}

/// Run the bootstrap phase and return the slot the live ingestor should
/// start from.
///
/// The returned slot is always the first slot that live services should see.
/// Historical catch-up blocks are replayed directly into the store before the
/// supervisor starts.
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
    let checkpoint = fetch_protocol_checkpoint(context, cancel).await?;
    publish_protocol_checkpoint(context, &checkpoint).await?;

    let start_slot = run_replay_phases(
        context, 
        config, 
        &checkpoint, 
        cancel
    ).await?;

    info!(
        node_id = context.node_id().0,
        checkpoint_slot = checkpoint.slot.0,
        start_slot = start_slot.0,
        "bootstrap: complete, handing start slot to ingestor"
    );

    Ok(start_slot)
}

async fn run_replay_phases<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &NodeConfig,
    checkpoint: &ProtocolCheckpoint,
    cancel: &CancellationToken,
) -> Result<SlotNumber, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    if let Some(start_slot) = config.solana.start_slot {
        debug!(start_slot = start_slot.0, "bootstrap: live replay boundary");
        return Ok(start_slot);
    }

    let cursor = context
        .store
        .get_sync_cursor()
        .map_err(|error| NodeError::Store(format!("get_sync_cursor: {error}")))?;

    let mut replay = ReplayEngine::new(
        context.store.as_ref(), 
        BOOTSTRAP_EPOCH
    );

    if let Some(cursor) = cursor {
        let start_slot = if cursor < checkpoint.slot {
            let start_slot = cursor.next();
            let start_epoch = epoch_for_slot(context, checkpoint.state.epoch(), start_slot).await?;

            execute_block_phase(
                context,
                &mut replay,
                BootstrapReplayPhase::BlockReplay {
                    start_slot,
                    end_slot: checkpoint.slot,
                    start_epoch,
                },
                cancel,
            )
            .await?;

            checkpoint.slot.next()
        } else {
            cursor.next()
        };

        debug!(start_slot = start_slot.0, "bootstrap: live replay boundary");
        return Ok(start_slot);
    }

    let current_epoch = checkpoint.state.epoch();
    if current_epoch <= FIRST_LIVE_EPOCH {
        replay_base_epochs_to_checkpoint(
            context, 
            &mut replay, 
            current_epoch, 
            checkpoint.slot, 
            cancel
        ).await?;

        let start_slot = checkpoint.slot.next();
        debug!(start_slot = start_slot.0, "bootstrap: live replay boundary");

        return Ok(start_slot);
    }

    replay_epoch_zero_base(context, &mut replay, cancel).await?;

    let snapshot_epochs =
        discovery::discover_missing_epochs(context.as_ref(), current_epoch).await?;

    let mut last_snapshot: Option<(EpochNumber, SlotNumber)> = None;
    for epoch in snapshot_epochs {
        last_snapshot = Some(execute_snapshot_phase(
            context,
            &mut replay,
            BootstrapReplayPhase::SnapshotReplay { epoch },
            cancel,
        )
        .await?);
    }

    match last_snapshot {
        Some((epoch, end_slot)) => {
            execute_block_phase(
                context,
                &mut replay,
                BootstrapReplayPhase::BlockReplay {
                    start_slot: end_slot.next(),
                    end_slot: checkpoint.slot,
                    start_epoch: epoch,
                },
                cancel,
            )
            .await?;
        }
        None => {
            let epoch = context
                .rpc
                .get_epoch_with_commitment(FIRST_LIVE_EPOCH, CommitmentLevel::Finalized)
                .await?;

            execute_block_phase(
                context,
                &mut replay,
                BootstrapReplayPhase::BlockReplay {
                    start_slot: epoch.start_slot,
                    end_slot: checkpoint.slot,
                    start_epoch: FIRST_LIVE_EPOCH,
                },
                cancel,
            ).await?;
        }
    }

    let start_slot = checkpoint.slot.next();
    debug!(start_slot = start_slot.0, "bootstrap: live replay boundary");
    Ok(start_slot)
}

async fn replay_base_epochs_to_checkpoint<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    replay: &mut ReplayEngine<'_, Db>,
    current_epoch: EpochNumber,
    checkpoint_slot: SlotNumber,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if current_epoch.is_zero() {
        let epoch = context
            .rpc
            .get_epoch_with_commitment(BOOTSTRAP_EPOCH, CommitmentLevel::Finalized)
            .await?;

        execute_block_phase(
            context,
            replay,
            BootstrapReplayPhase::BlockReplay {
                start_slot: epoch.start_slot,
                end_slot: checkpoint_slot,
                start_epoch: BOOTSTRAP_EPOCH,
            },
            cancel,
        ).await?;

        return Ok(());
    }

    replay_epoch_zero_base(context, replay, cancel).await?;

    let epoch = context
        .rpc
        .get_epoch_with_commitment(FIRST_LIVE_EPOCH, CommitmentLevel::Finalized)
        .await?;

    execute_block_phase(
        context,
        replay,
        BootstrapReplayPhase::BlockReplay {
            start_slot: epoch.start_slot,
            end_slot: checkpoint_slot,
            start_epoch: FIRST_LIVE_EPOCH,
        },
        cancel,
    ).await
}

async fn replay_epoch_zero_base<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    replay: &mut ReplayEngine<'_, Db>,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let epoch0 = context
        .rpc
        .get_epoch_with_commitment(BOOTSTRAP_EPOCH, CommitmentLevel::Finalized)
        .await?;

    let epoch1 = context
        .rpc
        .get_epoch_with_commitment(FIRST_LIVE_EPOCH, CommitmentLevel::Finalized)
        .await?;

    let Some(end_slot) = epoch1.start_slot.checked_prev() else {
        return Ok(());
    };

    execute_block_phase(
        context,
        replay,
        BootstrapReplayPhase::BlockReplay {
            start_slot: epoch0.start_slot,
            end_slot,
            start_epoch: BOOTSTRAP_EPOCH,
        },
        cancel,
    ).await
}

async fn execute_block_phase<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    replay: &mut ReplayEngine<'_, Db>,
    phase: BootstrapReplayPhase,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let BootstrapReplayPhase::BlockReplay {
        start_slot,
        end_slot,
        start_epoch,
    } = phase else {
        return Ok(());
    };

    if start_slot > end_slot {
        return Ok(());
    }

    replay.set_current_epoch(start_epoch);
    let events = block::replay_finalized_range(
        context,
        replay,
        start_slot,
        end_slot,
        cancel,
    )
    .await?;

    info!(
        start_slot = start_slot.0,
        end_slot = end_slot.0,
        start_epoch = start_epoch.0,
        events,
        "bootstrap: base block replayed"
    );

    Ok(())
}

async fn execute_snapshot_phase<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    replay: &mut ReplayEngine<'_, Db>,
    phase: BootstrapReplayPhase,
    cancel: &CancellationToken,
) -> Result<(EpochNumber, SlotNumber), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let BootstrapReplayPhase::SnapshotReplay { epoch } = phase else {
        return Err(NodeError::Store("bootstrap: expected snapshot phase".into()));
    };

    if cancel.is_cancelled() {
        return Err(NodeError::Store("bootstrap: cancelled".into()));
    }

    let log = fetch::fetch_and_decode_epoch(context, epoch, cancel).await?;
    replay.apply_snapshot_log(&log)?;
    advance_cursors(context, epoch, log.end_slot)?;

    info!(
        epoch = epoch.0,
        entries = log.entries.len(),
        end_slot = log.end_slot.0,
        "bootstrap: snapshot replayed"
    );

    Ok((epoch, log.end_slot))
}

async fn fetch_protocol_checkpoint<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: &CancellationToken,
) -> Result<ProtocolCheckpoint, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let state = retry_if(
        RetryConfig::infinite(),
        Some(cancel),
        || fetch_state_with_commitment(&context.rpc, CommitmentLevel::Finalized),
        |error| error.is_retriable() && !error.is_skipped_slot(),
    )
    .await
    .map_err(NodeError::from)?;

    let slot = SlotNumber(context.rpc.get_finalized_slot().await?);
    Ok(ProtocolCheckpoint { state, slot })
}

async fn publish_protocol_checkpoint<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    checkpoint: &ProtocolCheckpoint,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    context.set_state(checkpoint.state.clone())?;
    if let Err(error) = context.refresh_peers().await {
        warn!(error = %error, "peer resolution failed during bootstrap");
    }

    debug!(
        epoch = checkpoint.state.epoch().0,
        slot = checkpoint.slot.0,
        phase = ?checkpoint.state.phase(),
        "bootstrap: published protocol checkpoint"
    );

    Ok(())
}

async fn epoch_for_slot<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    current_epoch: EpochNumber,
    slot: SlotNumber,
) -> Result<EpochNumber, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mut selected = BOOTSTRAP_EPOCH;
    for raw in 0..=current_epoch.0 {
        let epoch_number = EpochNumber(raw);
        let epoch = context
            .rpc
            .get_epoch_with_commitment(epoch_number, CommitmentLevel::Finalized)
            .await?;
        if epoch.start_slot <= slot {
            selected = epoch_number;
        } else {
            break;
        }
    }

    Ok(selected)
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
    use crate::harness::{NodeHarness, TestContext};

    use super::{advance_cursors, run, FIRST_LIVE_EPOCH};

    async fn test_context_at(epoch: EpochNumber) -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .epoch(epoch)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    async fn test_context() -> TestContext {
        test_context_at(EpochNumber(5)).await
    }

    #[tokio::test]
    async fn override_wins_when_nothing_to_replay() {
        let context = test_context().await;
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
        let context = test_context().await;
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
    async fn first_live_epoch_replays_to_checkpoint() {
        let context = test_context_at(FIRST_LIVE_EPOCH).await;
        let config = NodeConfig::default();
        let cancel = CancellationToken::new();
        let checkpoint = SlotNumber(context.rpc.get_finalized_slot().await.expect("finalized"));

        let slot = timeout(Duration::from_secs(1), run(&context, &config, &cancel))
            .await
            .expect("bootstrap completed in time")
            .expect("bootstrap returned ok");

        assert_eq!(slot, checkpoint.next());
        assert_eq!(context.store.get_sync_cursor().unwrap(), Some(checkpoint));
    }

    #[tokio::test]
    async fn no_op_path_leaves_cursor_untouched() {
        let context = test_context().await;
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

    #[tokio::test]
    async fn advance_cursors_records_bootstrap_epoch_and_sync_slot() {
        let context = test_context().await;
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
