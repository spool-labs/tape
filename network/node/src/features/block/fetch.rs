//! Ordered concurrent block fetching shared by bootstrap replay and live
//! ingest catch-up.

use std::str::FromStr;
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};
use rpc::Rpc;
use store::Store;
use tape_blocks::parse_and_merge_with_sources;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_retry::{RetryConfig, retry_if};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;

/// Blocks kept in flight while fetching a slot range.
pub const FETCH_PIPELINE_DEPTH: usize = 16;

/// Fetch a slot range with a bounded pipeline of in-flight requests. Results
/// arrive strictly in slot order; None marks a slot the chain skipped.
///
/// Fetch latency dominates wall time on distant RPC endpoints, so the range
/// is fetched concurrently while callers still apply blocks sequentially.
pub fn fetch_blocks_ordered<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    slots: std::ops::RangeInclusive<u64>,
) -> impl Stream<Item = (SlotNumber, Result<Option<Arc<ParsedBlock>>, NodeError>)>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    stream::iter(slots.map(SlotNumber))
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

/// Fetch and parse one block, retrying transient failures. Returns None when
/// the chain skipped the slot.
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
    let attempt_progress = context.ingest.progress();
    let fetch_context = context.clone();

    let block = retry_if(
        RetryConfig::infinite(),
        Some(&cancel),
        move || {
            let context = fetch_context.clone();
            let attempt_progress = attempt_progress.clone();
            async move {
                attempt_progress.record_attempt();
                context.rpc.get_block(slot.0).await
            }
        },
        |error| error.is_retriable() && !error.is_skipped_slot(),
    )
    .await;

    let block = match block {
        Ok(block) => block,
        Err(error) if error.is_skipped_slot() => {
            debug!(slot = slot.0, "slot skipped");
            return Ok(None);
        }
        Err(error) => {
            error!(
                slot = slot.0,
                error = %error,
                "block fetch failed: {}",
                error
            );
            return Err(NodeError::from(error));
        }
    };

    let parent_slot = SlotNumber(block.parent_slot);
    let blockhash = parse_chain_hash(slot, "blockhash", &block.blockhash)?;
    let previous_blockhash =
        parse_chain_hash(slot, "previous_blockhash", &block.previous_blockhash)?;

    let sourced = match parse_and_merge_with_sources(&block) {
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

    let parsed = Arc::new(ParsedBlock {
        slot,
        parent_slot,
        blockhash,
        previous_blockhash,
        block_time: block.block_time,
        instructions,
        instruction_tx_ids,
    });

    debug!(
        slot = parsed.slot.0,
        extracted = parsed.instructions.len(),
        "parsed block"
    );

    Ok(Some(parsed))
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
