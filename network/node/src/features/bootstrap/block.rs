//! Finalized block replay used during bootstrap catch-up.
//!
//! This driver feeds historical blocks into the shared replay engine without
//! fanning them out to live protocol-state consumers.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream;
use rpc::Rpc;
use store::Store;
use tape_blocks::parse_and_merge_with_sources;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::{ParsedBlock, FETCH_PIPELINE_DEPTH};
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

    // Fetch latency dominates replay wall time on distant RPC endpoints, so
    // keep a pipeline of requests in flight. Results arrive in slot order, so
    // blocks still apply strictly sequentially.
    let mut blocks = stream::iter((start_slot.0..=end_slot.0).map(SlotNumber))
        .map(|slot| {
            let context = Arc::clone(context);
            async move {
                let fetched = fetch_parsed_block(&context, slot).await;
                (slot, fetched)
            }
        })
        .buffered(FETCH_PIPELINE_DEPTH);

    while let Some((slot, fetched)) = blocks.next().await {
        if cancel.is_cancelled() {
            return Err(NodeError::Store("bootstrap block replay: cancelled".into()));
        }

        match fetched? {
            Some(block) => {
                event_count = event_count.saturating_add(replay.apply_block_with(&block, persist)?);
            }
            None => {
                context.bootstrap.record_skipped();
                debug!(slot = slot.0, "bootstrap: skipped slot during block replay");
            }
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

async fn fetch_parsed_block<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    slot: SlotNumber,
) -> Result<Option<ParsedBlock>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let block = match context.rpc.get_block(slot.0).await {
        Ok(block) => block,
        Err(error) if error.is_skipped_slot() => return Ok(None),
        Err(error) => {
            error!(
                slot = slot.0,
                error = %error,
                "bootstrap: get_block failed during replay"
            );
            return Err(NodeError::from(error));
        }
    };

    let blockhash = parse_chain_hash(slot, "blockhash", &block.blockhash)?;
    let previous_blockhash =
        parse_chain_hash(slot, "previous_blockhash", &block.previous_blockhash)?;
    let sourced = parse_and_merge_with_sources(&block).map_err(NodeError::from)?;
    let mut instructions = Vec::with_capacity(sourced.len());
    let mut instruction_tx_ids = Vec::with_capacity(sourced.len());
    for sourced in sourced {
        instruction_tx_ids.push(sourced.tx_id);
        instructions.push(sourced.instruction);
    }

    Ok(Some(ParsedBlock {
        slot,
        parent_slot: SlotNumber(block.parent_slot),
        blockhash,
        previous_blockhash,
        block_time: block.block_time,
        instructions,
        instruction_tx_ids,
    }))
}

fn parse_chain_hash(slot: SlotNumber, label: &str, encoded: &str) -> Result<Hash, NodeError> {
    Hash::from_str(encoded).map_err(|err| {
        error!(
            slot = slot.0,
            label,
            encoded,
            error = %err,
            "bootstrap: chain hash parse failed"
        );
        NodeError::BlockMalformed {
            slot: slot.0,
            reason: format!("{label}: {err}"),
        }
    })
}
