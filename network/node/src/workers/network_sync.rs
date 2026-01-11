//! Thread B - Network Sync
//!
//! Handles epoch transitions and spool synchronization:
//! - Detects new spool assignments after epoch changes
//! - Syncs data from previous spool owners
//! - Falls back to erasure recovery if sync fails
//! - Submits SyncEpoch transaction when ready

use std::sync::Arc;

use solana_sdk::signer::Signer;
use tape_api::instruction::build_epoch_sync_ix;
use tape_api::program::tapedrive::node_pda;
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
                if let Err(e) = handle_event(&ctx, &sync_handler, event).await {
                    error!(error = %e, "Error handling event");
                }
            }
        }
    }

    Ok(())
}

/// Handle a single node event.
async fn handle_event(
    ctx: &NodeContext,
    sync_handler: &SpoolSyncHandler,
    event: NodeEvent,
) -> Result<(), NetworkSyncError> {
    match event {
        NodeEvent::EpochAdvanced { epoch } => {
            handle_epoch_advanced(ctx, sync_handler, epoch).await?;
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
            if let Err(e) = queue_spool_for_recovery(ctx, spool_idx).await {
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
                if let Err(e) = submit_sync_epoch(ctx, epoch).await {
                    error!(epoch = epoch.as_u64(), error = %e, "Failed to submit SyncEpoch");
                }
            } else {
                debug!(
                    epoch = epoch.as_u64(),
                    "Quorum reached but local sync not complete, waiting..."
                );
            }
        }
    }

    Ok(())
}

/// Handle an epoch advancement.
async fn handle_epoch_advanced(
    ctx: &NodeContext,
    sync_handler: &SpoolSyncHandler,
    new_epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    info!(epoch = new_epoch.as_u64(), "Handling epoch advancement");

    // Check if we're in the committee
    if !ctx.is_in_committee() {
        info!("Not in current committee, skipping epoch sync");
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
            match sync_spool_from_owner(ctx, sync_handler, *spool_idx, prev_member.id).await {
                Ok(count) => {
                    info!(spool = spool_idx, slices = count, "Synced spool");
                    ctx.metrics.spools_synced_total.inc();
                }
                Err(e) => {
                    warn!(spool = spool_idx, error = %e, "Failed to sync spool, queuing for recovery");
                    if let Err(qe) = queue_spool_for_recovery(ctx, *spool_idx).await {
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

    // If quorum is already reached (e.g., we're late), check and submit now
    if ctx.control_plane.is_sync_quorum_reached() {
        info!(
            epoch = new_epoch.as_u64(),
            "Quorum already reached, submitting SyncEpoch"
        );
        submit_sync_epoch(ctx, new_epoch).await?;
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
