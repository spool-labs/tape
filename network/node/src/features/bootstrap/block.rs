//! Finalized block replay used during bootstrap catch-up.
//!
//! This driver feeds historical blocks into the shared replay engine without
//! fanning them out to live protocol-state consumers.

use std::str::FromStr;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::parse_and_merge_with_sources;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_store::ops::MetaOps;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::engine::ReplayEngine;

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
    if start_slot > end_slot {
        return Ok(0);
    }

    let mut event_count = 0usize;
    let mut slot = start_slot;
    while slot <= end_slot {
        if cancel.is_cancelled() {
            return Err(NodeError::Store("bootstrap block replay: cancelled".into()));
        }

        match fetch_parsed_block(context, slot).await? {
            Some(block) => {
                event_count = event_count.saturating_add(replay.apply_block(&block)?);
            }
            None => {
                debug!(slot = slot.0, "bootstrap: skipped slot during block replay");
            }
        }

        match slot.checked_next() {
            Some(next) => slot = next,
            None => break,
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
