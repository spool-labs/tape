//! Ordered concurrent block fetching shared by bootstrap replay and live
//! ingest catch-up.

use std::str::FromStr;
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};
use rpc::{NUM_CONSECUTIVE_LEADER_SLOTS, Rpc, RpcError};
use store::Store;
use tape_blocks::parse_and_merge_with_sources;
use tape_blocks::wire::Block as WireBlock;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel, retry_if};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;

/// Blocks kept in flight while fetching a slot range.
pub const FETCH_PIPELINE_DEPTH: usize = 16;

/// Longest absent run taken as a skip on getBlocks' word; a pruned ledger reads far longer
pub const TRUSTED_SKIP_RUN: u64 = 2 * NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;

/// Fetch the given slots through a bounded pipeline, in order; None marks a skipped slot
///
/// Fetch latency dominates wall time on distant RPC endpoints, so the slots
/// are fetched concurrently while callers still apply blocks sequentially.
pub fn fetch_blocks_ordered<Db, Cluster, Blockchain, Slots>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    slots: Slots,
) -> impl Stream<Item = (SlotNumber, Result<Option<Arc<ParsedBlock>>, NodeError>)>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
    Slots: IntoIterator<Item = u64>,
{
    stream::iter(slots.into_iter().map(SlotNumber))
        .map(move |slot| {
            let context = context.clone();
            let cancel = cancel.clone();
            async move {
                let fetched = fetch_and_parse_block(context, cancel, slot).await;
                (slot, fetched)
            }
        })
        .buffered(FETCH_PIPELINE_DEPTH)
}

/// Fetch and parse one block, retrying transient failures; None when the chain skipped the slot
///
/// A confirmed skip answers "not available" until it roots, so the produced-slot list settles it.
pub async fn fetch_and_parse_block<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    slot: SlotNumber,
) -> Result<Option<Arc<ParsedBlock>>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let progress = context.ingest.progress();
    let mut backoff = Backoff::new(RetryConfig::infinite());
    loop {
        progress.record_fetch(slot.0);
        let error = match context.rpc.get_block(slot.0).await {
            Ok(block) => {
                let parsed = Arc::new(parse_block(slot, &block)?);
                debug!(
                    slot = parsed.slot.0,
                    extracted = parsed.instructions.len(),
                    "parsed block"
                );
                return Ok(Some(parsed));
            }
            Err(error) => error,
        };
        if error.is_skipped_slot() {
            debug!(slot = slot.0, "slot skipped");
            return Ok(None);
        }
        if matches!(error, RpcError::BlockNotAvailable)
            && skipped_on_confirmed_chain(&context, &cancel, slot).await?
        {
            debug!(slot = slot.0, "slot skipped on the confirmed chain");
            return Ok(None);
        }
        if !error.is_retriable() || backoff_or_cancel(&mut backoff, &cancel).await {
            error!(slot = slot.0, error = %error, "block fetch failed: {}", error);
            return Err(NodeError::from(error));
        }
    }
}

/// Whether an absent run this short is a skip rather than a backend missing its history
fn trusted_skip(slot: u64, first_listed: u64) -> bool {
    first_listed > slot && first_listed - slot <= TRUSTED_SKIP_RUN
}

/// The listed slots, plus any absent run too long to trust, which the root settles
pub fn slots_to_fetch(from: u64, produced: &[u64]) -> Vec<u64> {
    let mut slots = Vec::with_capacity(produced.len());
    let mut cursor = from;
    for &listed in produced {
        if listed < cursor {
            continue;
        }
        if listed != cursor && !trusted_skip(cursor, listed) {
            slots.extend(cursor..listed);
        }
        slots.push(listed);
        cursor = listed + 1;
    }
    slots
}

/// Whether the confirmed chain lists a later slot within reach but none at this one
async fn skipped_on_confirmed_chain<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: &CancellationToken,
    slot: SlotNumber,
) -> Result<bool, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let produced = retry_if(
        RetryConfig::infinite(),
        Some(cancel),
        || context.rpc.get_blocks(slot.0, slot.0 + TRUSTED_SKIP_RUN),
        |error: &RpcError| error.is_retriable(),
    )
    .await?;

    let first = produced.first();
    Ok(first.is_some_and(|first| trusted_skip(slot.0, *first)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // short absent runs are skipped on the list's word, long ones go to the root
    #[test]
    fn skip_runs() {
        assert_eq!(slots_to_fetch(10, &[10, 11, 12]), vec![10, 11, 12]);
        assert_eq!(slots_to_fetch(10, &[13, 14]), vec![13, 14]);

        let long = 10 + TRUSTED_SKIP_RUN + 1;
        let mut expected: Vec<u64> = (10..long).collect();
        expected.push(long);
        assert_eq!(slots_to_fetch(10, &[long]), expected);

        // A hole in the middle is judged the same way as one at the front.
        let hole = 12 + TRUSTED_SKIP_RUN + 1;
        let mut expected = vec![10, 11];
        expected.extend(12..hole);
        expected.push(hole);
        assert_eq!(slots_to_fetch(10, &[10, 11, hole]), expected);
    }

    // nothing listed, or a list behind the cursor, fetches only from the cursor on
    #[test]
    fn empty_list() {
        assert!(slots_to_fetch(10, &[]).is_empty());
        // A list below the cursor is a backend answering a stale question.
        assert_eq!(slots_to_fetch(10, &[8, 9, 10]), vec![10]);
    }

    // the serial check and the list walk share one reach
    #[test]
    fn skip_reach() {
        assert!(!trusted_skip(10, 10));
        assert!(trusted_skip(10, 11));
        assert!(trusted_skip(10, 10 + TRUSTED_SKIP_RUN));
        assert!(!trusted_skip(10, 10 + TRUSTED_SKIP_RUN + 1));
    }
}

/// Convert a fetched block into the replay input shared by live ingest and
/// offline tooling
pub fn parse_block(slot: SlotNumber, block: &WireBlock) -> Result<ParsedBlock, NodeError> {
    let parent_slot = SlotNumber(block.parent_slot);
    let blockhash = parse_chain_hash(slot, "blockhash", &block.blockhash)?;
    let previous_blockhash =
        parse_chain_hash(slot, "previous_blockhash", &block.previous_blockhash)?;

    let sourced = match parse_and_merge_with_sources(block) {
        Ok(instructions) => instructions,
        Err(error) => {
            error!(
                slot = slot.0,
                error = %error,
                "block instruction parse failed: {}",
                error
            );
            return Err(NodeError::from(error));
        }
    };
    let mut instructions = Vec::with_capacity(sourced.len());
    let mut instruction_tx_ids = Vec::with_capacity(sourced.len());
    for sourced in sourced {
        instruction_tx_ids.push(sourced.tx_id);
        instructions.push(sourced.instruction);
    }

    Ok(ParsedBlock {
        slot,
        parent_slot,
        blockhash,
        previous_blockhash,
        block_time: block.block_time,
        instructions,
        instruction_tx_ids,
    })
}

fn parse_chain_hash(slot: SlotNumber, label: &str, encoded: &str) -> Result<Hash, NodeError> {
    Hash::from_str(encoded).map_err(|err| {
        error!(
            slot = slot.0,
            label,
            encoded,
            error = %err,
            "chain hash parse failed"
        );
        NodeError::BlockMalformed {
            slot: slot.0,
            reason: format!("{label}: {err}"),
        }
    })
}
