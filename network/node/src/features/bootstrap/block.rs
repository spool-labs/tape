//! Finalized block replay used during bootstrap catch-up.
//!
//! This driver feeds historical blocks into the shared replay engine without
//! fanning them out to live protocol-state consumers.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use rpc::Rpc;
use store::Store;
use tape_core::types::SlotNumber;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::fetch::fetch_blocks_ordered;
use crate::features::replay::engine::{ReplayEngine, ReplayPersistFn};
use crate::features::store::manager::persist_batch;

const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);

pub async fn replay_finalized_range<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    replay: &mut ReplayEngine<'_, Db>,
    start_slot: SlotNumber,
    end_slot: SlotNumber,
    cancel: &CancellationToken,
) -> Result<usize, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    replay_finalized_range_with_persist(
        context,
        replay,
        start_slot,
        end_slot,
        cancel,
        persist_batch::<Db>,
    )
    .await
}

pub async fn replay_finalized_range_with_persist<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    replay: &mut ReplayEngine<'_, Db>,
    start_slot: SlotNumber,
    end_slot: SlotNumber,
    cancel: &CancellationToken,
    persist: ReplayPersistFn<Db>,
) -> Result<usize, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    if start_slot > end_slot {
        return Ok(0);
    }

    let mut event_count = 0usize;
    let mut last_progress_log = Instant::now();

    let mut blocks = fetch_blocks_ordered(
        Arc::clone(context),
        cancel.clone(),
        start_slot.0..=end_slot.0,
    );

    while let Some((slot, fetched)) = blocks.next().await {
        if cancel.is_cancelled() {
            return Err(NodeError::Store("bootstrap block replay: cancelled".into()));
        }

        match fetched? {
            Some(block) => {
                event_count = event_count.saturating_add(replay.apply_block_with(&block, persist)?);
            }
            None => context.bootstrap.record_skipped(),
        }

        context.bootstrap.record_slot(slot.0);

        if last_progress_log.elapsed() >= PROGRESS_LOG_INTERVAL {
            last_progress_log = Instant::now();
            let progress = context.bootstrap.snapshot();
            info!(
                slot = slot.0,
                target_slot = end_slot.0,
                percent = (progress.percent_done() * 10.0).round() / 10.0,
                slots_per_sec = (progress.slots_per_sec * 10.0).round() / 10.0,
                eta_secs = progress.eta_secs.unwrap_or(0),
                skipped = progress.skipped_slots,
                "bootstrap: block replay progress"
            );
        }
    }

    context
        .store
        .set_sync_cursor(end_slot)
        .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))?;

    Ok(event_count)
}
