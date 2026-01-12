//! Thread B - Network Sync
//!
//! Handles epoch transitions and spool synchronization:
//! - Detects new spool assignments after epoch changes
//! - Syncs data from previous spool owners
//! - Falls back to erasure recovery if sync fails
//! - Submits SyncEpoch transaction when ready

use std::sync::Arc;
use std::time::Duration;

use solana_sdk::signer::Signer;
use tape_api::instruction::{build_advance_epoch_ix, build_advance_pool_ix, build_epoch_sync_ix, build_join_network_ix};
use tape_api::program::tapedrive::{node_pda, EPOCH_DURATION};
use tape_core::prelude::*;
use tape_core::spooler::SpoolIndex;
use tape_store::ops::{RecoveryInfo, RecoveryOps, SliceOps};
use tape_store::types::Pubkey as StorePubkey;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::context::NodeContext;
use crate::events::NodeEvent;
use crate::storage::service::{Compression, SliceMeta};
use crate::sync::types::{track_id_to_pubkey, SyncSlice};
use crate::sync::{SpoolSyncHandler, SyncError};

/// Polling interval for epoch advancement monitoring.
const EPOCH_ADVANCE_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum time to monitor for epoch advancement (safety limit).
/// After this duration, the monitor task exits to avoid resource leaks.
const EPOCH_ADVANCE_MONITOR_TIMEOUT: Duration = Duration::from_secs(3600); // 1 hour

/// Error type for network sync operations.
#[derive(Debug, thiserror::Error)]
pub enum NetworkSyncError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("sync error: {0}")]
    Sync(#[from] SyncError),

    #[error("storage error: {0}")]
    Storage(String),
}

/// Run the network sync loop.
///
/// This is Thread B's main entry point. It:
/// 1. Listens for events from Thread A
/// 2. Handles epoch transitions by syncing new spools
/// 3. Submits SyncEpoch when ready
pub async fn run(
    ctx: Arc<NodeContext>,
    mut event_rx: mpsc::Receiver<NodeEvent>,
    cancel: CancellationToken,
) -> Result<(), NetworkSyncError> {
    info!("Network sync thread starting");

    let sync_handler = SpoolSyncHandler::new()
        .with_max_concurrent(ctx.config.sync_concurrency.unwrap_or(4))
        .with_batch_size(ctx.config.sync_batch_size.unwrap_or(1000));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Network sync thread shutting down");
                break;
            }
            Some(event) = event_rx.recv() => {
                if let Err(e) = handle_event(Arc::clone(&ctx), &sync_handler, event).await {
                    error!(error = %e, "Error handling event");
                }
            }
        }
    }

    Ok(())
}

/// Handle a single node event.
async fn handle_event(
    ctx: Arc<NodeContext>,
    sync_handler: &SpoolSyncHandler,
    event: NodeEvent,
) -> Result<(), NetworkSyncError> {
    match event {
        NodeEvent::EpochAdvanced { epoch } => {
            handle_epoch_advanced(Arc::clone(&ctx), sync_handler, epoch).await?;
        }

        NodeEvent::NodeSynced {
            node,
            epoch,
            spools_hash: _,
        } => {
            // Track sync progress from other nodes
            // TODO: Maintain a set of synced nodes to know when quorum is reached
            debug!(
                node = %node,
                epoch = epoch.as_u64(),
                "Node synced"
            );
        }

        NodeEvent::SpoolSyncComplete {
            spool_idx,
            slice_count,
        } => {
            info!(
                spool = spool_idx,
                slices = slice_count,
                "Spool sync complete"
            );
            ctx.metrics.spools_synced_total.inc();
        }

        NodeEvent::SpoolRecoveryNeeded { spool_idx } => {
            warn!(spool = spool_idx, "Spool needs erasure recovery");
            if let Err(e) = queue_spool_for_recovery(&ctx, spool_idx).await {
                error!(spool = spool_idx, error = %e, "Failed to queue spool for recovery");
            }
        }

        NodeEvent::EpochSyncReady { epoch } => {
            // Only submit SyncEpoch if our local sync is complete
            if ctx.control_plane.is_local_sync_complete(epoch) {
                info!(
                    epoch = epoch.as_u64(),
                    "Quorum reached and local sync complete, submitting SyncEpoch"
                );
                if let Err(e) = submit_sync_epoch(&ctx, epoch).await {
                    error!(epoch = epoch.as_u64(), error = %e, "Failed to submit SyncEpoch");
                }
            } else {
                debug!(
                    epoch = epoch.as_u64(),
                    "Quorum reached but local sync not complete, waiting..."
                );
            }
        }

        NodeEvent::EpochSettling { epoch } => {
            // Epoch has transitioned to Settling - submit AdvancePool to contribute
            // weight toward Active transition
            info!(
                epoch = epoch.as_u64(),
                "Epoch is Settling, submitting AdvancePool"
            );
            if let Err(e) = submit_advance_pool(&ctx, epoch).await {
                error!(epoch = epoch.as_u64(), error = %e, "Failed to submit AdvancePool");
            }

            // After AdvancePool, submit JoinNetwork to re-join committee_next
            // This is required each epoch since committee_next is cleared on rotation
            info!(
                epoch = epoch.as_u64(),
                "Submitting JoinNetwork to re-join committee"
            );
            if let Err(e) = submit_join_network(&ctx, epoch).await {
                error!(epoch = epoch.as_u64(), error = %e, "Failed to submit JoinNetwork");
            }

            // Start monitoring for Active state to auto-advance epoch
            let ctx_clone = Arc::clone(&ctx);
            tokio::spawn(async move {
                monitor_epoch_for_advancement(ctx_clone, epoch).await;
            });
        }

        NodeEvent::EpochActive {
            epoch,
            advance_after,
        } => {
            // Epoch is ready for advancement - check timing and submit
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            if now >= advance_after {
                info!(
                    epoch = epoch.as_u64(),
                    "Epoch ready and time elapsed, submitting AdvanceEpoch"
                );
                if let Err(e) = submit_advance_epoch(&ctx, epoch).await {
                    error!(epoch = epoch.as_u64(), error = %e, "Failed to submit AdvanceEpoch");
                }
            } else {
                let wait_secs = advance_after - now;
                info!(
                    epoch = epoch.as_u64(),
                    wait_secs = wait_secs,
                    "Epoch ready but time not elapsed, waiting"
                );
            }
        }
    }

    Ok(())
}

/// Handle an epoch advancement.
async fn handle_epoch_advanced(
    ctx: Arc<NodeContext>,
    sync_handler: &SpoolSyncHandler,
    new_epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    info!(epoch = new_epoch.as_u64(), "Handling epoch advancement");

    // Check if we're in the committee
    if !ctx.is_in_committee() {
        info!("Not in current committee, skipping epoch sync");
        return Ok(());
    }

    // Fetch current epoch state from chain to check if syncing is needed.
    // In low-quorum scenarios, the epoch may skip directly to Active.
    let epoch = ctx
        .rpc
        .get_epoch()
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to fetch epoch: {}", e)))?;

    if !epoch.state.is_syncing() {
        info!(
            epoch = new_epoch.as_u64(),
            phase = ?epoch.state,
            "Epoch not in syncing phase (low-quorum mode), handling pool maintenance"
        );

        // In low-quorum mode, we still need to:
        // 1. Advance our pool to process stake schedule and update latest_advance_epoch
        // 2. Re-join committee_next for the next epoch
        // 3. Start monitoring for epoch advancement

        // Submit AdvancePool
        if let Err(e) = submit_advance_pool(&ctx, new_epoch).await {
            warn!(epoch = new_epoch.as_u64(), error = %e, "Failed to submit AdvancePool in low-quorum mode");
        }

        // Submit JoinNetwork to re-join committee_next
        if let Err(e) = submit_join_network(&ctx, new_epoch).await {
            warn!(epoch = new_epoch.as_u64(), error = %e, "Failed to submit JoinNetwork in low-quorum mode");
        }

        // Start monitoring for epoch advancement (since EpochSettling won't fire in low-quorum mode)
        let ctx_clone = Arc::clone(&ctx);
        tokio::spawn(async move {
            monitor_epoch_for_advancement(ctx_clone, new_epoch).await;
        });

        return Ok(());
    }

    // Get current and previous spool assignments
    let system = ctx.control_plane.get_system();
    let our_node_id = ctx.control_plane.our_node_id();

    // Find our member index in current committee
    let curr_index = match system.committee.index_of(&our_node_id) {
        Some(idx) => idx,
        None => {
            warn!("Not found in current committee despite is_in_committee=true");
            return Ok(());
        }
    };

    // Get our previous committee position (if any)
    let prev_index = system.committee_prev.index_of(&our_node_id);

    // Compute spools we now own
    let curr_spools = system.spools.spools_for_member(curr_index);

    // Compute spools we previously owned
    let prev_spools: Vec<SpoolIndex> = prev_index
        .map(|idx| system.spools_prev.spools_for_member(idx))
        .unwrap_or_default();

    // New spools we need to sync
    let gained_spools: Vec<SpoolIndex> = curr_spools
        .iter()
        .filter(|s| !prev_spools.contains(s))
        .copied()
        .collect();

    // Spools we no longer own (can schedule for GC)
    let lost_spools: Vec<SpoolIndex> = prev_spools
        .iter()
        .filter(|s| !curr_spools.contains(s))
        .copied()
        .collect();

    info!(
        epoch = new_epoch.as_u64(),
        gained = gained_spools.len(),
        lost = lost_spools.len(),
        total = curr_spools.len(),
        "Computed spool changes"
    );

    // Sync gained spools from previous owners
    for spool_idx in &gained_spools {
        // Find who owned this spool before
        let prev_owner_member_idx = system.spools_prev.0[*spool_idx as usize] as usize;

        // Get their network address
        if let Some(prev_member) = system.committee_prev.member_at(prev_owner_member_idx) {
            match sync_spool_from_owner(&ctx, sync_handler, *spool_idx, prev_member.id).await {
                Ok(count) => {
                    info!(spool = spool_idx, slices = count, "Synced spool");
                    ctx.metrics.spools_synced_total.inc();
                }
                Err(e) => {
                    warn!(spool = spool_idx, error = %e, "Failed to sync spool, queuing for recovery");
                    if let Err(qe) = queue_spool_for_recovery(&ctx, *spool_idx).await {
                        error!(spool = spool_idx, error = %qe, "Failed to queue spool for recovery");
                    }
                }
            }
        } else {
            debug!(spool = spool_idx, "No previous owner, spool is new");
        }
    }

    // Schedule lost spools for GC
    for spool_idx in &lost_spools {
        // TODO: Schedule GC after epoch is fully active
        debug!(spool = spool_idx, "Scheduling spool for GC");
    }

    // Mark local sync as complete - we've synced all our new spools
    // SyncEpoch will be submitted when quorum is reached (EpochSyncReady event)
    ctx.control_plane.mark_local_sync_complete(new_epoch);

    info!(
        epoch = new_epoch.as_u64(),
        "Local sync complete, waiting for quorum"
    );

    // If previous committee is empty (first epoch or bootstrap), submit immediately
    // since there's nothing to actually sync from previous owners.
    if system.committee_prev_empty() {
        info!(
            epoch = new_epoch.as_u64(),
            "Empty previous committee, submitting SyncEpoch immediately"
        );
        submit_sync_epoch(&ctx, new_epoch).await?;
        return Ok(());
    }

    // If quorum is already reached (e.g., we're late), check and submit now
    if ctx.control_plane.is_sync_quorum_reached() {
        info!(
            epoch = new_epoch.as_u64(),
            "Quorum already reached, submitting SyncEpoch"
        );
        submit_sync_epoch(&ctx, new_epoch).await?;
    }

    Ok(())
}

/// Sync a spool from its previous owner.
async fn sync_spool_from_owner(
    ctx: &NodeContext,
    sync_handler: &SpoolSyncHandler,
    spool_idx: SpoolIndex,
    prev_owner_id: NodeId,
) -> Result<usize, NetworkSyncError> {
    // Look up the previous owner's node to get their network address
    let (_pubkey, prev_node) = ctx
        .rpc
        .get_node_by_id(prev_owner_id)
        .await
        .map_err(|e| NetworkSyncError::Rpc(e.to_string()))?;

    let addr = prev_node
        .metadata
        .network_address
        .to_socket_addr()
        .map_err(|e| NetworkSyncError::Rpc(format!("Invalid network address: {}", e)))?;

    let from_epoch = ctx.control_plane.current_epoch();

    // Sync the spool
    let storage = Arc::clone(&ctx.storage);
    let count = sync_handler
        .sync_spool(spool_idx, from_epoch, &addr.to_string(), |slice: SyncSlice| {
            // Parse track ID and store the slice
            let track = track_id_to_pubkey(&slice.track_id)
                .map_err(|e| SyncError::Storage(format!("Invalid track ID: {}", e)))?;

            // Use merkle proofs from sync response
            let meta = SliceMeta {
                len: slice.data.len() as u32,
                leaf_hash: slice.leaf_hash,
                merkle_proof: slice.merkle_proof,
                compression: Compression::None,
                received_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
            };

            storage
                .put_slice(slice.slice_index, track, slice.data, meta)
                .map_err(|e| SyncError::Storage(e.to_string()))
        })
        .await?;

    Ok(count)
}

/// Submit SyncEpoch transaction to the chain.
async fn submit_sync_epoch(ctx: &NodeContext, epoch: EpochNumber) -> Result<(), NetworkSyncError> {
    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);

    // Get our assigned spools
    let assigned_spools = ctx.control_plane.get_our_spools();

    // Build the sync instruction
    let ix = build_epoch_sync_ix(authority, authority, node_address, epoch, &assigned_spools);

    info!(
        epoch = epoch.as_u64(),
        spools = assigned_spools.len(),
        "Submitting SyncEpoch"
    );

    // Submit the transaction
    ctx.rpc
        .send_instructions(&ctx.keypair, vec![ix])
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to submit SyncEpoch: {}", e)))?;

    info!(epoch = epoch.as_u64(), "SyncEpoch submitted successfully");

    Ok(())
}

/// Submit AdvancePool transaction to contribute weight toward Active.
///
/// This claims staking rewards and adds our spool weight to the epoch state,
/// helping transition from Settling to Active when 2/3+ weight is reached.
async fn submit_advance_pool(
    ctx: &NodeContext,
    epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    // Check if we're in the committee
    if !ctx.is_in_committee() {
        debug!(
            epoch = epoch.as_u64(),
            "Not in committee, skipping AdvancePool"
        );
        return Ok(());
    }

    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);

    let ix = build_advance_pool_ix(authority, authority, node_address);

    info!(
        epoch = epoch.as_u64(),
        node = %node_address,
        "Submitting AdvancePool"
    );

    // Submit the transaction with timing
    let start = std::time::Instant::now();
    info!(epoch = epoch.as_u64(), "AdvancePool RPC call starting");

    let result = ctx.rpc.send_instructions(&ctx.keypair, vec![ix]).await;
    let elapsed = start.elapsed();

    info!(
        epoch = epoch.as_u64(),
        elapsed_ms = elapsed.as_millis() as u64,
        "AdvancePool RPC call completed"
    );

    match result {
        Ok(sig) => {
            info!(
                epoch = epoch.as_u64(),
                signature = %sig,
                elapsed_ms = elapsed.as_millis() as u64,
                "AdvancePool submitted successfully"
            );
        }
        Err(e) => {
            // Check if it's AlreadyAdvanced error (0x62) - this is OK
            let err_str = e.to_string();
            if err_str.contains("0x62") || err_str.contains("AlreadyAdvanced") {
                info!(
                    epoch = epoch.as_u64(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "Already advanced for this epoch, skipping"
                );
                return Ok(());
            }
            error!(
                epoch = epoch.as_u64(),
                elapsed_ms = elapsed.as_millis() as u64,
                error = %e,
                "AdvancePool RPC call failed"
            );
            return Err(NetworkSyncError::Rpc(format!(
                "Failed to submit AdvancePool: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Submit JoinNetwork transaction to re-join committee_next.
///
/// After each epoch rotation, committee_next is cleared. Nodes must call JoinNetwork
/// to re-establish membership for the next epoch. This must be called AFTER AdvancePool
/// (which sets latest_advance_epoch) or the instruction will fail with NodeStale.
async fn submit_join_network(
    ctx: &NodeContext,
    epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    // Check if we're in the committee (only re-join if we're currently serving)
    if !ctx.is_in_committee() {
        debug!(
            epoch = epoch.as_u64(),
            "Not in committee, skipping JoinNetwork"
        );
        return Ok(());
    }

    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);

    let ix = build_join_network_ix(authority, authority, node_address);

    info!(
        epoch = epoch.as_u64(),
        node = %node_address,
        "Submitting JoinNetwork to re-join committee_next"
    );

    match ctx.rpc.send_instructions(&ctx.keypair, vec![ix]).await {
        Ok(sig) => {
            info!(
                epoch = epoch.as_u64(),
                signature = %sig,
                "JoinNetwork submitted successfully"
            );
        }
        Err(e) => {
            let err_str = e.to_string();
            // NodeStale (0x60) - AdvancePool wasn't called first
            if err_str.contains("0x60") || err_str.contains("NodeStale") {
                error!(
                    epoch = epoch.as_u64(),
                    "JoinNetwork failed: AdvancePool must be called first"
                );
                return Err(NetworkSyncError::Rpc(
                    "JoinNetwork failed: AdvancePool must be called first".to_string(),
                ));
            }
            // 0x10 = UnexpectedState - covers multiple non-fatal scenarios:
            // - Already present in committee_next
            // - Zero stake (would have failed earlier)
            // - Committee full with lower stake than minimum
            // All are acceptable outcomes when re-joining
            if err_str.contains("0x10") {
                info!(
                    epoch = epoch.as_u64(),
                    "JoinNetwork returned UnexpectedState (likely already in committee_next), skipping"
                );
                return Ok(());
            }
            return Err(NetworkSyncError::Rpc(format!(
                "Failed to submit JoinNetwork: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Submit AdvanceEpoch transaction to transition to the next epoch.
///
/// This triggers committee rotation and starts the new epoch's Syncing phase.
/// Can only succeed when epoch is in Active state and EPOCH_DURATION has elapsed.
async fn submit_advance_epoch(
    ctx: &NodeContext,
    epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    let authority = ctx.keypair.pubkey();

    let ix = build_advance_epoch_ix(authority, authority);

    info!(epoch = epoch.as_u64(), "Submitting AdvanceEpoch");

    // Submit the transaction
    match ctx.rpc.send_instructions(&ctx.keypair, vec![ix]).await {
        Ok(_) => {
            info!(epoch = epoch.as_u64(), "AdvanceEpoch submitted successfully");
            ctx.metrics.epoch_transitions_total.inc();
        }
        Err(e) => {
            let err_str = e.to_string();
            // TooSoon (0x41) - epoch duration hasn't elapsed yet
            if err_str.contains("0x41") || err_str.contains("TooSoon") {
                debug!(
                    epoch = epoch.as_u64(),
                    "Epoch duration not elapsed yet, will retry"
                );
                return Ok(());
            }
            // BadEpochState (0x40) - epoch not in Active state
            if err_str.contains("0x40") || err_str.contains("BadEpochState") {
                debug!(
                    epoch = epoch.as_u64(),
                    "Epoch not in Active state, will retry"
                );
                return Ok(());
            }
            return Err(NetworkSyncError::Rpc(format!(
                "Failed to submit AdvanceEpoch: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Monitor epoch state and submit AdvanceEpoch when conditions are met.
///
/// Polls epoch state periodically after Settling phase is entered.
/// When Active is reached and EPOCH_DURATION has elapsed, submits AdvanceEpoch.
async fn monitor_epoch_for_advancement(ctx: Arc<NodeContext>, starting_epoch: EpochNumber) {
    info!(
        epoch = starting_epoch.as_u64(),
        "Starting epoch advancement monitor"
    );

    let start_time = std::time::Instant::now();

    loop {
        tokio::time::sleep(EPOCH_ADVANCE_POLL_INTERVAL).await;

        // Safety limit - exit if we've been monitoring too long
        if start_time.elapsed() > EPOCH_ADVANCE_MONITOR_TIMEOUT {
            warn!(
                epoch = starting_epoch.as_u64(),
                "Epoch advancement monitor timed out"
            );
            return;
        }

        // Fetch current epoch state from chain
        let epoch = match ctx.rpc.get_epoch().await {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "Failed to fetch epoch state, retrying");
                continue;
            }
        };

        // If we've moved to a new epoch, stop monitoring
        if epoch.id > starting_epoch {
            info!(
                old_epoch = starting_epoch.as_u64(),
                new_epoch = epoch.id.as_u64(),
                "Epoch already advanced, stopping monitor"
            );
            return;
        }

        // Check if epoch is in Active state
        if !epoch.state.is_active() {
            debug!(
                epoch = epoch.id.as_u64(),
                "Epoch not in Active state yet"
            );
            continue;
        }

        // Check if enough time has elapsed
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let advance_after = epoch.last_epoch + EPOCH_DURATION;

        if now < advance_after {
            let wait_secs = advance_after - now;
            debug!(
                epoch = epoch.id.as_u64(),
                wait_secs = wait_secs,
                "Active but time not elapsed"
            );
            continue;
        }

        // Both conditions met - submit AdvanceEpoch
        info!(
            epoch = epoch.id.as_u64(),
            "Active and time elapsed, submitting AdvanceEpoch"
        );

        match submit_advance_epoch(&ctx, epoch.id).await {
            Ok(_) => {
                info!(epoch = epoch.id.as_u64(), "AdvanceEpoch submitted");
                return;
            }
            Err(e) => {
                // Log error but continue polling - another node may have advanced it
                warn!(epoch = epoch.id.as_u64(), error = %e, "Failed to submit AdvanceEpoch");
            }
        }
    }
}

/// Queue all slices in a spool for erasure recovery.
///
/// This enumerates all tracks that have slices in the given spool
/// and queues each for recovery.
async fn queue_spool_for_recovery(
    ctx: &NodeContext,
    spool_idx: SpoolIndex,
) -> Result<(), NetworkSyncError> {
    // Get all slices currently stored for this spool
    let slices = ctx
        .storage
        .store
        .get_spool_slices(spool_idx)
        .map_err(|e| NetworkSyncError::Storage(e.to_string()))?;

    if slices.is_empty() {
        debug!(spool = spool_idx, "No slices to recover for spool");
        return Ok(());
    }

    info!(
        spool = spool_idx,
        slice_count = slices.len(),
        "Queuing spool slices for recovery"
    );

    let info = RecoveryInfo {
        source_node: StorePubkey::default(), // Will fetch from committee
        attempts: 0,
        last_attempt: 0,
    };

    // Queue each slice for recovery
    for (track_address, _meta) in slices {
        ctx.storage
            .store
            .queue_recovery(spool_idx, track_address, info.clone())
            .map_err(|e| NetworkSyncError::Storage(e.to_string()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
