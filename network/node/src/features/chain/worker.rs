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

use crate::core::context::NodeContext;
use crate::features::epoch::FsmSignal;
use crate::features::recovery::LiveUploadDeferral;

use super::handler;
use super::parser::{parse_block, ParsedInstruction};
use crate::features::snapshot::capture;

/// Default polling interval (Solana slot time).
///
/// Note: This constant is local to the block processor. If a centralized
/// constants module is added to tape-core, consider moving this there.
const DEFAULT_POLL_INTERVAL_MS: u64 = 400;

/// Maximum slots to process per iteration.
///
/// Note: This constant is local to the block processor. If a centralized
/// constants module is added to tape-core, consider moving this there.
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
/// 4. Signals FSM loop when state changes (only when caught up)
pub async fn run(
    ctx: Arc<NodeContext>,
    signal_tx: mpsc::Sender<FsmSignal>,
    deferral: Arc<LiveUploadDeferral>,
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
                // Check for pause request (snapshot bootstrap)
                if ctx.control_plane.is_paused() {
                    info!("Block processor pausing for snapshot bootstrap");
                    ctx.control_plane.wait_for_resume().await;
                    info!("Block processor resumed");
                    continue;
                }
                if let Err(e) = poll_and_process(&ctx, &signal_tx, &deferral, &mut last_slot).await {
                    error!(error = %e, "Error processing blocks");
                }
            }
        }
    }

    Ok(())
}

/// Poll for new slots and process them.
async fn poll_and_process(
    ctx: &NodeContext,
    signal_tx: &mpsc::Sender<FsmSignal>,
    deferral: &Arc<LiveUploadDeferral>,
    last_slot: &mut SlotNumber,
) -> Result<(), BlockProcessorError> {
    let latest_slot = ctx
        .rpc
        .get_slot()
        .await
        .map_err(|e| BlockProcessorError::Rpc(e.to_string()))?;

    let latest_slot = SlotNumber(latest_slot);

    if latest_slot <= *last_slot {
        return Ok(());
    }

    let start_slot = last_slot.as_u64() + 1;
    let end_slot = latest_slot.as_u64().min(start_slot + MAX_SLOTS_PER_BATCH - 1);

    debug!(
        from = start_slot,
        to = end_slot,
        behind = latest_slot.as_u64() - end_slot,
        "Processing slot range"
    );

    let mut state_changed = false;

    for slot in start_slot..=end_slot {
        match process_slot(ctx, deferral, slot).await {
            Ok(changed) => {
                state_changed |= changed;
            }
            Err(BlockProcessorError::Rpc(ref e)) if e.contains("SlotSkipped") => {
                debug!(slot = slot, "Slot skipped");
            }
            Err(e) => {
                warn!(slot = slot, error = %e, "Failed to process slot");
            }
        }
        *last_slot = SlotNumber(slot);
    }

    ctx.control_plane.set_last_processed_slot(*last_slot);
    ctx.metrics.last_processed_slot.set(last_slot.as_u64() as i64);

    // Detect epoch lag for recovery FSM
    let processed_epoch = ctx.control_plane.current_epoch();
    let chain_epoch = ctx.control_plane.chain_epoch();
    if chain_epoch > processed_epoch {
        let lag = chain_epoch.as_u64() - processed_epoch.as_u64();
        if lag >= 2 {
            let _ = signal_tx.send(FsmSignal::DetectedLag { lag }).await;
        }
    }

    // Only signal FSM when caught up and state actually changed
    if state_changed && ctx.control_plane.is_caught_up() {
        let _ = signal_tx.send(FsmSignal::StateChanged).await;
    }

    Ok(())
}

/// Process a single slot. Returns true if FSM-relevant state changed.
async fn process_slot(ctx: &NodeContext, deferral: &Arc<LiveUploadDeferral>, slot: u64) -> Result<bool, BlockProcessorError> {
    let block = ctx
        .rpc
        .get_block(slot)
        .await
        .map_err(|e| BlockProcessorError::Rpc(e.to_string()))?;

    let parsed = parse_block(&block)?;

    if parsed.instructions.is_empty() {
        return Ok(false);
    }

    debug!(
        slot = slot,
        instructions = parsed.instructions.len(),
        "Found tapedrive instructions"
    );

    let mut state_changed = false;
    for instruction in parsed.instructions {
        state_changed |= process_instruction(ctx, deferral, slot, instruction).await?;
    }

    ctx.metrics.blocks_processed_total.inc();
    Ok(state_changed)
}

/// Process a single parsed instruction. Returns true if FSM-relevant state changed.
async fn process_instruction(
    ctx: &NodeContext,
    deferral: &Arc<LiveUploadDeferral>,
    slot: u64,
    instruction: ParsedInstruction,
) -> Result<bool, BlockProcessorError> {
    // Capture event for snapshot log before processing
    let current_epoch = ctx.control_plane.current_epoch();
    if let Some(event) = capture::to_replayable(&instruction, current_epoch) {
        use tape_store::ops::EventLogOps;
        if let Err(e) = ctx.storage.store.append_event(current_epoch, SlotNumber(slot), &event) {
            warn!(error = %e, "failed to append event to snapshot log");
        }
    }

    match instruction {
        ParsedInstruction::AdvanceEpoch { event } => {
            let old_epoch = event.old_epoch;
            let new_epoch = event.new_epoch;

            info!(
                old_epoch = old_epoch.as_u64(),
                new_epoch = new_epoch.as_u64(),
                "Detected AdvanceEpoch instruction"
            );

            ctx.control_plane.set_current_epoch(new_epoch);
            ctx.control_plane.start_epoch_sync(new_epoch);

            let owned_spools = ctx.control_plane.get_our_spools();
            if let Err(e) = handler::handle_advance_epoch(
                &ctx.storage.store,
                old_epoch,
                new_epoch,
                &owned_spools,
            ) {
                warn!(error = %e, "AdvanceEpoch handler failed");
            }

            ctx.metrics.epoch_transitions_total.inc();
            ctx.metrics.current_epoch.set(new_epoch.as_u64() as i64);
            Ok(true) // FSM-relevant state change
        }

        ParsedInstruction::SyncEpoch { event } => {
            let node_id = event.id;
            let epoch = event.epoch;

            debug!(
                node = %event.node,
                node_id = ?node_id,
                epoch = epoch.as_u64(),
                "Detected SyncEpoch instruction"
            );

            let system = ctx.control_plane.get_system();
            let spool_count = match system.committee.index_of(&node_id) {
                Some(idx) => system.spools.weight(idx) as u64,
                None => 0,
            };

            let quorum_reached = ctx.control_plane.record_node_sync(epoch, node_id, spool_count);
            if quorum_reached {
                info!(epoch = epoch.as_u64(), "Sync quorum reached");
            }
            Ok(quorum_reached) // FSM-relevant if quorum reached
        }

        ParsedInstruction::RegisterTrack {
            track,
            size,
            event,
            ..
        } => {
            debug!(track = %track, size = size.as_u64(), "Detected RegisterTrack");

            match event {
                Some(ref e) => {
                    if let Err(e) = handler::handle_register_track(
                        &ctx.storage.store,
                        track.to_bytes(),
                        e,
                    ) {
                        warn!(track = %track, error = %e, "Failed to store track info");
                    }
                }
                None => {
                    warn!(track = %track, "RegisterTrack without event, skipping");
                }
            }

            // Defer recovery for this track if a live upload is likely in progress
            if ctx.control_plane.is_caught_up() {
                deferral.begin_recovery(tape_store::types::Pubkey(track.to_bytes())).await;
            }

            Ok(false)
        }

        ParsedInstruction::CertifyTrack { track, event } => {
            let epoch = event.epoch;
            debug!(track = %track, epoch = epoch.as_u64(), "Detected CertifyTrack");
            if let Err(e) = handler::handle_certify_track(
                &ctx.storage.store,
                tape_store::types::Pubkey(track.to_bytes()),
                epoch,
            ) {
                warn!(track = %track, error = %e, "Failed to mark track certified");
            }
            Ok(false)
        }

        ParsedInstruction::DeleteTrack { track, .. } => {
            debug!(track = %track, "Detected DeleteTrack");
            let current_epoch = ctx.control_plane.current_epoch();
            let owned_spools = ctx.control_plane.get_our_spools();
            if let Err(e) = handler::handle_delete_track(
                &ctx.storage.store,
                track.to_bytes(),
                current_epoch,
                &owned_spools,
            ) {
                warn!(track = %track, error = %e, "DeleteTrack handler failed");
            }
            Ok(false)
        }

        ParsedInstruction::InvalidateTrack { track, event } => {
            let epoch = event
                .map(|e| e.epoch)
                .unwrap_or_else(|| ctx.control_plane.current_epoch());
            debug!(track = %track, epoch = epoch.as_u64(), "Detected InvalidateTrack");
            let owned_spools = ctx.control_plane.get_our_spools();
            if let Err(e) = handler::handle_invalidate_track(
                &ctx.storage.store,
                track.to_bytes(),
                epoch,
                &owned_spools,
            ) {
                warn!(track = %track, error = %e, "Failed to schedule track for GC");
            }
            Ok(false)
        }

        ParsedInstruction::ReserveTape { owner, tape, event } => {
            debug!(tape = %tape, owner = %owner, "Detected ReserveTape");

            // Extract epochs from event, or use current epoch as fallback
            let (active_epoch, expiry_epoch) = match event {
                Some(e) => (e.active_epoch, e.expiry_epoch),
                None => {
                    let current = ctx.control_plane.current_epoch();
                    (current, current)
                }
            };

            if let Err(e) = handler::handle_reserve_tape(
                &ctx.storage.store,
                tape.to_bytes(),
                owner.to_bytes(),
                active_epoch,
                expiry_epoch,
            ) {
                warn!(tape = %tape, error = %e, "ReserveTape handler failed");
            }
            Ok(false)
        }

        ParsedInstruction::DestroyTape { tape, .. } => {
            debug!(tape = %tape, "Detected DestroyTape");
            let current_epoch = ctx.control_plane.current_epoch();
            let owned_spools = ctx.control_plane.get_our_spools();
            if let Err(e) = handler::handle_destroy_tape(
                &ctx.storage.store,
                tape.to_bytes(),
                current_epoch,
                &owned_spools,
            ) {
                warn!(tape = %tape, error = %e, "DestroyTape handler failed");
            }
            Ok(false)
        }

        ParsedInstruction::RegisterNode { authority, node, event } => {
            debug!(
                node = %node,
                authority = %authority,
                epoch = ?event.map(|e| e.epoch.as_u64()),
                "Detected RegisterNode"
            );
            Ok(false)
        }

        ParsedInstruction::JoinNetwork { node, event } => {
            debug!(
                node = %node,
                activation_epoch = ?event.map(|e| e.activation_epoch.as_u64()),
                "Detected JoinNetwork"
            );
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
