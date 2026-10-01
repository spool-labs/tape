//! Ordered concurrent block fetching shared by bootstrap replay and live
//! ingest catch-up.

use std::str::FromStr;
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};
use tape_rpc::skip::{TRUSTED_SKIP_RUN, trusted_skip};
use tape_rpc::{Rpc, RpcError};
use tape_store_api::Store;
use tape_blocks::parse_and_merge_with_sources;
use tape_blocks::wire::Block as WireBlock;
use tape_core::encoding::ErasureCode;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel, retry_if};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;

/// Blocks kept in flight while fetching a slot range.
pub const FETCH_PIPELINE_DEPTH: usize = 16;

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
                let parsed = Arc::new(parse_block(slot, &block, context.params().profile)?);
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
        // A pruned slot answers the same way here: the endpoint has no block, and
        // the confirmed chain is what says whether one was ever produced.
        if matches!(error, RpcError::BlockNotAvailable | RpcError::BlockPruned)
            && skipped_on_confirmed_chain(&context, &cancel, slot).await?
        {
            debug!(slot = slot.0, "slot skipped on the confirmed chain");
            return Ok(None);
        }
        // The chain lists a block this endpoint has lost, and only a retry reaches it
        if matches!(error, RpcError::BlockPruned) {
            warn!(slot = slot.0, "block fetch: endpoint has no block the confirmed chain lists, retrying");
        }
        if !error.is_retriable() || backoff_or_cancel(&mut backoff, &cancel).await {
            error!(slot = slot.0, error = %error, "block fetch failed: {}", error);
            return Err(NodeError::from(error));
        }
    }
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

/// Convert a fetched block into the replay input shared by live ingest and
/// offline tooling. The network's profile sets how wide a track commitment is,
/// which the parser checks the way the chain does.
pub fn parse_block(
    slot: SlotNumber,
    block: &WireBlock,
    profile: ErasureCode,
) -> Result<ParsedBlock, NodeError> {
    let parent_slot = SlotNumber(block.parent_slot);
    let blockhash = parse_chain_hash(slot, "blockhash", &block.blockhash)?;
    let previous_blockhash =
        parse_chain_hash(slot, "previous_blockhash", &block.previous_blockhash)?;

    let sourced = match parse_and_merge_with_sources(block, usize::from(profile.n())) {
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
