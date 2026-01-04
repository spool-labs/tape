//! Thread A - Live Updates
//!
//! Continuously polls Solana blocks and processes tapedrive-related
//! transactions to keep local state synchronized with the chain.

use std::sync::Arc;
use std::time::Duration;

use tape_core::prelude::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::context::NodeContext;
use crate::events::NodeEvent;
use crate::tx_parser::{parse_block, ParsedInstruction};

/// Default polling interval (Solana slot time).
const DEFAULT_POLL_INTERVAL_MS: u64 = 400;

/// Maximum slots to process per iteration.
const MAX_SLOTS_PER_BATCH: u64 = 100;

/// Error type for live update operations.
#[derive(Debug, thiserror::Error)]
pub enum LiveUpdateError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("parse error: {0}")]
    Parse(#[from] crate::tx_parser::ParseError),

    #[error("event channel closed")]
    ChannelClosed,
}

/// Run the live updates loop.
///
/// This is Thread A's main entry point. It:
/// 1. Polls for new Solana slots
/// 2. Fetches and parses blocks
/// 3. Updates the control plane cache
/// 4. Emits events for Thread B
pub async fn run(
    ctx: Arc<NodeContext>,
    event_tx: mpsc::Sender<NodeEvent>,
    cancel: CancellationToken,
) -> Result<(), LiveUpdateError> {
    info!("Live updates thread starting");

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
                info!("Live updates thread shutting down");
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
) -> Result<(), LiveUpdateError> {
    // Get latest slot from RPC
    let latest_slot = ctx
        .rpc
        .get_slot()
        .await
        .map_err(|e| LiveUpdateError::Rpc(e.to_string()))?;

    let latest_slot = SlotNumber::new(latest_slot);

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
            Err(LiveUpdateError::Rpc(ref e)) if e.contains("SlotSkipped") => {
                // Slot was skipped (no block produced), this is normal
                debug!(slot = slot, "Slot skipped");
            }
            Err(e) => {
                warn!(slot = slot, error = %e, "Failed to process slot");
                // Continue with next slot
            }
        }
        *last_slot = SlotNumber::new(slot);
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
) -> Result<(), LiveUpdateError> {
    // Fetch the block
    let block = ctx
        .rpc
        .get_block(slot)
        .await
        .map_err(|e| LiveUpdateError::Rpc(e.to_string()))?;

    // Parse the block for tapedrive instructions
    let parsed = parse_block(&block)?;

    if parsed.instructions.is_empty() {
        return Ok(());
    }

    debug!(
        slot = slot,
        instructions = parsed.instructions.len(),
        "Found tapedrive instructions"
    );

    // Process each instruction
    for instruction in parsed.instructions {
        process_instruction(ctx, event_tx, instruction).await?;
    }

    // Update metrics
    ctx.metrics.blocks_processed_total.inc();

    Ok(())
}

/// Process a single parsed instruction.
async fn process_instruction(
    ctx: &NodeContext,
    event_tx: &mpsc::Sender<NodeEvent>,
    instruction: ParsedInstruction,
) -> Result<(), LiveUpdateError> {
    match instruction {
        ParsedInstruction::AdvanceEpoch => {
            info!("Detected AdvanceEpoch instruction");

            // Fetch fresh system and epoch state
            let system = ctx
                .rpc
                .get_system()
                .await
                .map_err(|e| LiveUpdateError::Rpc(e.to_string()))?;

            let epoch = ctx
                .rpc
                .get_epoch()
                .await
                .map_err(|e| LiveUpdateError::Rpc(e.to_string()))?;

            let new_epoch = epoch.id;

            // Update control plane
            ctx.control_plane.update_system(system);
            ctx.control_plane.update_epoch(epoch);

            // Emit event for Thread B
            event_tx
                .send(NodeEvent::EpochAdvanced { epoch: new_epoch })
                .await
                .map_err(|_| LiveUpdateError::ChannelClosed)?;

            ctx.metrics.epoch_transitions_total.inc();
            ctx.metrics.current_epoch.set(new_epoch.as_u64() as i64);
        }

        ParsedInstruction::SyncEpoch {
            node,
            epoch,
            spools_hash,
        } => {
            debug!(
                node = %node,
                epoch = epoch.as_u64(),
                "Detected SyncEpoch instruction"
            );

            // Emit event so Thread B can track sync progress
            event_tx
                .send(NodeEvent::NodeSynced {
                    node,
                    epoch,
                    spools_hash,
                })
                .await
                .map_err(|_| LiveUpdateError::ChannelClosed)?;
        }

        ParsedInstruction::RegisterTrack {
            owner,
            track,
            key,
            root,
            commitment,
            size,
        } => {
            debug!(
                track = %track,
                size = size.as_u64(),
                "Detected RegisterTrack instruction"
            );

            // TODO: Store track info in local database if we're in committee
            // This helps us know what slices to expect
        }

        ParsedInstruction::CertifyTrack { track, epoch } => {
            debug!(
                track = %track,
                epoch = epoch.as_u64(),
                "Detected CertifyTrack instruction"
            );

            // TODO: Update local track state to mark as certified
        }

        ParsedInstruction::DeleteTrack { owner, track } => {
            debug!(track = %track, "Detected DeleteTrack instruction");

            // TODO: Schedule track data for GC
        }

        ParsedInstruction::InvalidateTrack { track } => {
            debug!(track = %track, "Detected InvalidateTrack instruction");

            // TODO: Mark track as invalid, schedule for cleanup
        }

        ParsedInstruction::ReserveTape { owner, tape } => {
            debug!(tape = %tape, owner = %owner, "Detected ReserveTape instruction");
            // Informational only for now
        }

        ParsedInstruction::DestroyTape { owner, tape } => {
            debug!(tape = %tape, "Detected DestroyTape instruction");

            // TODO: Schedule all tracks in tape for GC
        }

        ParsedInstruction::RegisterNode { authority, node } => {
            debug!(node = %node, authority = %authority, "Detected RegisterNode instruction");

            // Refresh system state to pick up new committee membership
            let system = ctx
                .rpc
                .get_system()
                .await
                .map_err(|e| LiveUpdateError::Rpc(e.to_string()))?;

            ctx.control_plane.update_system(system);
        }

        ParsedInstruction::JoinNetwork { node } => {
            debug!(node = %node, "Detected JoinNetwork instruction");

            // Refresh system state
            let system = ctx
                .rpc
                .get_system()
                .await
                .map_err(|e| LiveUpdateError::Rpc(e.to_string()))?;

            ctx.control_plane.update_system(system);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
