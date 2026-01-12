//! Block processor worker loop.
//!
//! Continuously polls Solana blocks and processes tapedrive-related
//! transactions to keep local state synchronized with the chain.
//!
//! Uses event data from transaction logs for execution-time state,
//! enabling correct processing during historical catch-up.

use std::sync::Arc;
use std::time::Duration;

use tape_core::prelude::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::context::NodeContext;
use crate::events::NodeEvent;

use super::handlers;
use super::parser::{parse_block, ParsedInstruction};

/// Default polling interval (Solana slot time).
const DEFAULT_POLL_INTERVAL_MS: u64 = 400;

/// Maximum slots to process per iteration.
const MAX_SLOTS_PER_BATCH: u64 = 100;

/// Error type for block processor operations.
#[derive(Debug, thiserror::Error)]
pub enum BlockProcessorError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("parse error: {0}")]
    Parse(#[from] super::parser::ParseError),

    #[error("storage error: {0}")]
    Storage(#[from] tape_store::error::TapeStoreError),

    #[error("event channel closed")]
    ChannelClosed,
}

/// Run the block processor loop.
///
/// This is the main entry point for the block processor. It:
/// 1. Polls for new Solana slots
/// 2. Fetches and parses blocks (including event logs)
/// 3. Processes instructions using event data for execution-time state
/// 4. Emits events for other workers
pub async fn run(
    ctx: Arc<NodeContext>,
    event_tx: mpsc::Sender<NodeEvent>,
    cancel: CancellationToken,
) -> Result<(), BlockProcessorError> {
    info!("Block processor starting");

    let poll_interval = Duration::from_millis(
        ctx.config
            .poll_interval_ms
            .unwrap_or(DEFAULT_POLL_INTERVAL_MS),
    );

    let mut last_slot = ctx.control_plane.get_last_processed_slot();
    info!(slot = last_slot.as_u64(), "Starting from slot");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Block processor shutting down");
                break;
            }
            _ = tokio::time::sleep(poll_interval) => {
                if let Err(e) = poll_and_process(&ctx, &event_tx, &mut last_slot).await {
                    error!(error = %e, "Error processing blocks");
                    // Continue running, errors are often transient
                }
            }
        }
    }

    Ok(())
}

/// Poll for new slots and process them.
async fn poll_and_process(
    ctx: &NodeContext,
    event_tx: &mpsc::Sender<NodeEvent>,
    last_slot: &mut SlotNumber,
) -> Result<(), BlockProcessorError> {
    // Get latest slot from RPC
    let latest_slot = ctx
        .rpc
        .get_slot()
        .await
        .map_err(|e| BlockProcessorError::Rpc(e.to_string()))?;

    let latest_slot = SlotNumber(latest_slot);

    if latest_slot <= *last_slot {
        return Ok(());
    }

    // Calculate slot range to process
    let start_slot = last_slot.as_u64() + 1;
    let end_slot = latest_slot.as_u64().min(start_slot + MAX_SLOTS_PER_BATCH - 1);

    debug!(
        from = start_slot,
        to = end_slot,
        behind = latest_slot.as_u64() - end_slot,
        "Processing slot range"
    );

    // Process each slot in the range
    for slot in start_slot..=end_slot {
        match process_slot(ctx, event_tx, slot).await {
            Ok(_) => {}
            Err(BlockProcessorError::Rpc(ref e)) if e.contains("SlotSkipped") => {
                // Slot was skipped (no block produced), this is normal
                debug!(slot = slot, "Slot skipped");
            }
            Err(e) => {
                warn!(slot = slot, error = %e, "Failed to process slot");
                // Continue with next slot
            }
        }
        *last_slot = SlotNumber(slot);
    }

    // Update control plane with last processed slot
    ctx.control_plane.set_last_processed_slot(*last_slot);

    // Update metrics
    ctx.metrics.last_processed_slot.set(last_slot.as_u64() as i64);

    Ok(())
}

/// Process a single slot.
async fn process_slot(
    ctx: &NodeContext,
    event_tx: &mpsc::Sender<NodeEvent>,
    slot: u64,
) -> Result<(), BlockProcessorError> {
    // Fetch the block
    let block = ctx
        .rpc
        .get_block(slot)
        .await
        .map_err(|e| BlockProcessorError::Rpc(e.to_string()))?;

    // Parse the block for tapedrive instructions and events
    let parsed = parse_block(&block)?;

    if parsed.instructions.is_empty() {
        return Ok(());
    }

    debug!(
        slot = slot,
        instructions = parsed.instructions.len(),
        "Found tapedrive instructions"
    );

    // Process each instruction using event data
    for instruction in parsed.instructions {
        process_instruction(ctx, event_tx, instruction).await?;
    }

    // Update metrics
    ctx.metrics.blocks_processed_total.inc();

    Ok(())
}

/// Process a single parsed instruction.
///
/// Uses event data from transaction logs for execution-time state,
/// eliminating dependency on current RPC state during catch-up.
async fn process_instruction(
    ctx: &NodeContext,
    event_tx: &mpsc::Sender<NodeEvent>,
    instruction: ParsedInstruction,
) -> Result<(), BlockProcessorError> {
    match instruction {
        ParsedInstruction::AdvanceEpoch { event } => {
            // Use event data - contains execution-time epoch info
            let old_epoch = event.old_epoch;
            let new_epoch = event.new_epoch;

            info!(
                old_epoch = old_epoch.as_u64(),
                new_epoch = new_epoch.as_u64(),
                "Detected AdvanceEpoch instruction"
            );

            // Update control plane with new epoch from event
            // Note: For real-time operation, we may still want to refresh
            // system/epoch accounts periodically, but for catch-up this
            // event data is sufficient and correct.
            ctx.control_plane.set_current_epoch(new_epoch);

            // Start tracking node syncs for this epoch
            ctx.control_plane.start_epoch_sync(new_epoch);

            // Run GC for the epoch that just ended (from event data)
            let our_spools = ctx.control_plane.get_our_spools();

            match handlers::run_epoch_gc(&ctx.storage.store, old_epoch, &our_spools) {
                Ok(stats) => {
                    if stats.tracks_deleted > 0 || stats.slices_deleted > 0 {
                        info!(
                            epoch = old_epoch.as_u64(),
                            tracks = stats.tracks_deleted,
                            slices = stats.slices_deleted,
                            failed = stats.tracks_failed,
                            "Epoch GC completed"
                        );
                    }
                    ctx.metrics.gc_runs_total.inc();
                }
                Err(e) => {
                    warn!(epoch = old_epoch.as_u64(), error = %e, "Epoch GC failed");
                }
            }

            // Emit event for other workers
            event_tx
                .send(NodeEvent::EpochAdvanced { epoch: new_epoch })
                .await
                .map_err(|_| BlockProcessorError::ChannelClosed)?;

            ctx.metrics.epoch_transitions_total.inc();
            ctx.metrics.current_epoch.set(new_epoch.as_u64() as i64);
        }

        ParsedInstruction::SyncEpoch { event } => {
            // NodeSynced event contains all the data we need including NodeId
            let node_id = event.id;
            let epoch = event.epoch;

            debug!(
                node = %event.node,
                node_id = ?node_id,
                epoch = epoch.as_u64(),
                "Detected SyncEpoch instruction"
            );

            // Look up this node's spool weight from the committee
            let system = ctx.control_plane.get_system();
            let spool_count = match system.committee.index_of(&node_id) {
                Some(idx) => system.spools.weight(idx) as u64,
                None => {
                    debug!(node_id = ?node_id, "Node not found in committee");
                    0
                }
            };

            let quorum_just_reached = ctx.control_plane.record_node_sync(epoch, node_id, spool_count);

            if quorum_just_reached {
                info!(
                    epoch = epoch.as_u64(),
                    "Sync quorum reached - epoch ready for activation"
                );
                event_tx
                    .send(NodeEvent::EpochSyncReady { epoch })
                    .await
                    .map_err(|_| BlockProcessorError::ChannelClosed)?;

                // Epoch should now be Settling on-chain, trigger pool advancement
                event_tx
                    .send(NodeEvent::EpochSettling { epoch })
                    .await
                    .map_err(|_| BlockProcessorError::ChannelClosed)?;
            }

            // Emit event so other workers can track sync progress
            event_tx
                .send(NodeEvent::NodeSynced {
                    node: event.node,
                    epoch,
                    spools_hash: event.spools_hash,
                })
                .await
                .map_err(|_| BlockProcessorError::ChannelClosed)?;
        }

        ParsedInstruction::RegisterTrack {
            owner: _,
            track,
            key: _,
            root: _,
            commitment,
            size,
            event: _,
        } => {
            debug!(
                track = %track,
                size = size.as_u64(),
                "Detected RegisterTrack instruction"
            );

            // Store track info in local database
            if let Err(e) = handlers::handle_register_track(
                &ctx.storage.store,
                track.to_bytes(),
                commitment,
            ) {
                warn!(track = %track, error = %e, "Failed to store track info");
            }
        }

        ParsedInstruction::CertifyTrack { track, event } => {
            // Use epoch from event - this fixes the previous TODO!
            let epoch = event.epoch;

            debug!(
                track = %track,
                epoch = epoch.as_u64(),
                "Detected CertifyTrack instruction"
            );

            // Mark track as certified in local storage
            if let Err(e) = handlers::handle_certify_track(
                &ctx.storage.store,
                track.to_bytes(),
                epoch,
            ) {
                warn!(track = %track, error = %e, "Failed to mark track certified");
            }
        }

        ParsedInstruction::DeleteTrack {
            owner: _,
            track,
            event,
        } => {
            debug!(track = %track, "Detected DeleteTrack instruction");

            // Get epoch from control plane (already updated by AdvanceEpoch events)
            // Note: During catch-up, this will be the epoch at that point in history
            let current_epoch = ctx.control_plane.current_epoch();

            // If we have event data, we could extract more info, but for GC
            // scheduling we just need the current epoch
            if event.is_some() {
                debug!("DeleteTrack event data available");
            }

            if let Err(e) = handlers::handle_delete_track(
                &ctx.storage.store,
                track.to_bytes(),
                current_epoch,
            ) {
                warn!(track = %track, error = %e, "Failed to schedule track for GC");
            }
        }

        ParsedInstruction::InvalidateTrack { track, event } => {
            // Use epoch from event if available, otherwise from control plane
            let epoch = event
                .map(|e| e.epoch)
                .unwrap_or_else(|| ctx.control_plane.current_epoch());

            debug!(
                track = %track,
                epoch = epoch.as_u64(),
                "Detected InvalidateTrack instruction"
            );

            // Schedule track for immediate GC
            if let Err(e) = handlers::handle_invalidate_track(
                &ctx.storage.store,
                track.to_bytes(),
                epoch,
            ) {
                warn!(track = %track, error = %e, "Failed to schedule track for GC");
            }
        }

        ParsedInstruction::ReserveTape { owner, tape } => {
            debug!(tape = %tape, owner = %owner, "Detected ReserveTape instruction");
            // Informational only for now
        }

        ParsedInstruction::DestroyTape {
            owner: _,
            tape,
            event: _,
        } => {
            debug!(tape = %tape, "Detected DestroyTape instruction");

            // Use epoch from control plane (kept in sync by AdvanceEpoch events)
            let current_epoch = ctx.control_plane.current_epoch();

            if let Err(e) = handlers::handle_destroy_tape(
                &ctx.storage.store,
                tape.to_bytes(),
                current_epoch,
            ) {
                warn!(tape = %tape, error = %e, "Failed to schedule tape for GC");
            }
        }

        ParsedInstruction::RegisterNode {
            authority,
            node,
            event,
        } => {
            let epoch = event.map(|e| e.epoch);
            debug!(
                node = %node,
                authority = %authority,
                epoch = ?epoch.map(|e| e.as_u64()),
                "Detected RegisterNode instruction"
            );

            // For real-time operation, refresh system state to pick up new
            // committee membership. During catch-up, we rely on AdvanceEpoch
            // events to update the control plane state.
            //
            // TODO: Consider if we need to refresh system state here or if
            // AdvanceEpoch events are sufficient for committee tracking.
        }

        ParsedInstruction::JoinNetwork { node, event } => {
            let activation_epoch = event.map(|e| e.activation_epoch);
            debug!(
                node = %node,
                activation_epoch = ?activation_epoch.map(|e| e.as_u64()),
                "Detected JoinNetwork instruction"
            );

            // Similar to RegisterNode - for real-time operation we may want
            // to refresh system state, but during catch-up we rely on
            // AdvanceEpoch events.
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
