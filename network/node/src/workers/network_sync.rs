//! Thread B - Network Sync
//!
//! Handles epoch transitions and spool synchronization:
//! - Detects new spool assignments after epoch changes
//! - Syncs data from previous spool owners
//! - Falls back to erasure recovery if sync fails
//! - Submits SyncEpoch transaction when ready

use std::sync::Arc;
use std::time::Duration;

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signer::Signer;
use tape_api::fsm::NodeAction;
use tape_api::instruction::{build_advance_epoch_ix, build_advance_pool_ix, build_epoch_sync_ix, build_join_network_ix};
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

/// Polling interval for epoch advancement monitoring.
const EPOCH_ADVANCE_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Compute units required for AdvanceEpoch instruction.
/// AdvanceEpoch performs committee rotation and spool reallocation which
/// requires significant computation, especially with many nodes.
const ADVANCE_EPOCH_COMPUTE_UNITS: u32 = 1_400_000;

/// Compute units required for AdvancePool instruction.
/// AdvancePool calculates rewards based on committee size and spool assignment,
/// which can exceed the default 200k CU limit with larger committees.
const ADVANCE_POOL_COMPUTE_UNITS: u32 = 400_000;

// Note: We no longer use a fixed timeout for epoch advancement monitoring.
// Instead, we use exponential backoff when the system is stuck waiting for
// nodes to join committee_next. This allows recovery without arbitrary limits.

/// Get the current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

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
            // Skip stale epochs
            if ctx.control_plane.is_stale_epoch(epoch) {
                debug!(epoch = epoch.as_u64(), "Skipping EpochSyncReady: stale epoch");
                return Ok(());
            }

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
            // Skip stale epochs
            if ctx.control_plane.is_stale_epoch(epoch) {
                debug!(epoch = epoch.as_u64(), "Skipping EpochSettling: stale epoch");
                return Ok(());
            }

            // Use FSM to determine expected action and log it
            let now = current_timestamp();
            let (action, _) = ctx.control_plane.determine_action(now);
            log_fsm_action(&action, epoch);

            // Skip if not in committee
            if matches!(action, NodeAction::NotInCommittee) {
                debug!(epoch = epoch.as_u64(), "Not in committee, skipping settling");
                return Ok(());
            }

            // Epoch has transitioned to Settling - submit AdvancePool to contribute
            // weight toward Active transition
            info!(epoch = epoch.as_u64(), "Epoch is Settling, submitting AdvancePool");
            if let Err(e) = submit_advance_pool(&ctx, epoch).await {
                error!(epoch = epoch.as_u64(), error = %e, "Failed to submit AdvancePool");
            }

            // Submit JoinNetwork to re-join committee_next
            // This is required each epoch since committee_next is cleared on rotation
            info!(epoch = epoch.as_u64(), "Submitting JoinNetwork to re-join committee");
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
            // Skip stale epochs
            if ctx.control_plane.is_stale_epoch(epoch) {
                debug!(epoch = epoch.as_u64(), "Skipping EpochActive: stale epoch");
                return Ok(());
            }

            // Use FSM to determine what action to take
            let now = current_timestamp();
            let (action, _) = ctx.control_plane.determine_action(now);
            log_fsm_action(&action, epoch);

            match action {
                NodeAction::AdvanceEpoch => {
                    info!(epoch = epoch.as_u64(), "FSM says AdvanceEpoch - submitting");
                    if let Err(e) = submit_advance_epoch(&ctx, epoch).await {
                        error!(epoch = epoch.as_u64(), error = %e, "Failed to submit AdvanceEpoch");
                    }
                }
                NodeAction::WaitForEpochDuration { seconds_remaining } => {
                    info!(
                        epoch = epoch.as_u64(),
                        seconds = seconds_remaining,
                        "Epoch active but duration not elapsed, waiting"
                    );
                }
                NodeAction::WaitForCommitteeThreshold {
                    current_size,
                    required_size,
                } => {
                    info!(
                        epoch = epoch.as_u64(),
                        current = current_size,
                        required = required_size,
                        "Waiting for committee_next threshold"
                    );
                }
                NodeAction::EpochBlocked { committee_next_size } => {
                    warn!(
                        epoch = epoch.as_u64(),
                        committee_next = committee_next_size,
                        "Epoch blocked: committee_next below threshold"
                    );
                }
                _ => {
                    // Fall back to legacy timing check if FSM returns unexpected state
                    if now >= advance_after {
                        info!(
                            epoch = epoch.as_u64(),
                            "Epoch ready and time elapsed, submitting AdvanceEpoch"
                        );
                        if let Err(e) = submit_advance_epoch(&ctx, epoch).await {
                            error!(epoch = epoch.as_u64(), error = %e, "Failed to submit AdvanceEpoch");
                        }
                    }
                }
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

    // Fetch current epoch state from chain
    let epoch = ctx
        .rpc
        .get_epoch()
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to fetch epoch: {}", e)))?;

    // Update chain epoch for catch-up detection
    ctx.control_plane.set_chain_epoch(epoch.id);

    // Skip stale epochs - we're catching up on historical events
    if ctx.control_plane.is_stale_epoch(new_epoch) {
        info!(
            event_epoch = new_epoch.as_u64(),
            chain_epoch = epoch.id.as_u64(),
            "Stale epoch: skipping (catching up on historical events)"
        );
        return Ok(());
    }

    // Real-time mode: fetch fresh system state since the committee just rotated
    info!(epoch = new_epoch.as_u64(), "Real-time mode: refreshing system state");
    let system = ctx
        .rpc
        .get_system()
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to fetch system: {}", e)))?;
    ctx.control_plane.update_system(system);
    ctx.control_plane.update_epoch(epoch);

    // Use FSM to determine what action to take
    let now = current_timestamp();
    let (action, _catching_up) = ctx.control_plane.determine_action(now);

    log_fsm_action(&action, new_epoch);

    match action {
        NodeAction::NotInCommittee => {
            info!("Not in current committee, skipping epoch sync");
            Ok(())
        }
        NodeAction::SyncEpoch => {
            // Normal syncing phase - sync spools and submit SyncEpoch
            sync_spools_and_submit(&ctx, sync_handler, new_epoch).await
        }
        NodeAction::WaitForSyncQuorum { current_weight } => {
            // Already synced this epoch
            info!(
                epoch = new_epoch.as_u64(),
                weight = current_weight,
                "Already synced for this epoch, waiting for quorum"
            );
            Ok(())
        }
        NodeAction::AdvancePool | NodeAction::JoinNetwork => {
            // Bootstrap/Active case - FSM says we need maintenance
            handle_epoch_maintenance(Arc::clone(&ctx), new_epoch).await
        }
        NodeAction::WaitForEpochDuration { .. }
        | NodeAction::WaitForSettleQuorum { .. }
        | NodeAction::WaitForCommitteeThreshold { .. }
        | NodeAction::AdvanceEpoch => {
            // Active phase states - still need to do maintenance and start monitor
            handle_epoch_maintenance(Arc::clone(&ctx), new_epoch).await
        }
        NodeAction::EpochBlocked { committee_next_size } => {
            warn!(
                epoch = new_epoch.as_u64(),
                committee_next = committee_next_size,
                "Epoch blocked due to insufficient committee_next"
            );
            // Still do maintenance and start monitor
            handle_epoch_maintenance(Arc::clone(&ctx), new_epoch).await
        }
        NodeAction::UnknownPhase { phase } => {
            error!(epoch = new_epoch.as_u64(), phase, "Unknown epoch phase");
            Ok(())
        }
    }
}

/// Handle epoch maintenance tasks (AdvancePool, JoinNetwork, start monitor).
///
/// Called during bootstrap or when epoch is already in Active state.
async fn handle_epoch_maintenance(
    ctx: Arc<NodeContext>,
    epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    info!(
        epoch = epoch.as_u64(),
        "Performing epoch maintenance (AdvancePool, JoinNetwork)"
    );

    // Submit AdvancePool
    if let Err(e) = submit_advance_pool(&ctx, epoch).await {
        warn!(epoch = epoch.as_u64(), error = %e, "Failed to submit AdvancePool");
    }

    // Submit JoinNetwork to re-join committee_next
    if let Err(e) = submit_join_network(&ctx, epoch).await {
        warn!(epoch = epoch.as_u64(), error = %e, "Failed to submit JoinNetwork");
    }

    // Start monitoring for epoch advancement
    tokio::spawn(async move {
        monitor_epoch_for_advancement(ctx, epoch).await;
    });

    Ok(())
}

/// Sync spools from previous owners and submit SyncEpoch.
async fn sync_spools_and_submit(
    ctx: &NodeContext,
    sync_handler: &SpoolSyncHandler,
    new_epoch: EpochNumber,
) -> Result<(), NetworkSyncError> {
    // Get current and previous spool assignments
    let system = ctx.control_plane.get_system();
    let our_node_id = ctx.control_plane.our_node_id();

    // Find our member index in current committee
    let curr_index = match system.committee.index_of(&our_node_id) {
        Some(idx) => idx,
        None => {
            warn!("Not found in current committee despite FSM saying SyncEpoch");
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
    ctx.control_plane.mark_local_sync_complete(new_epoch);

    info!(
        epoch = new_epoch.as_u64(),
        "Local sync complete, submitting SyncEpoch"
    );

    // Submit SyncEpoch immediately after local sync completes.
    // The on-chain logic aggregates weight from all submissions.
    // Waiting for quorum before submitting would cause a deadlock
    // since quorum requires submissions from other nodes.
    submit_sync_epoch(ctx, new_epoch).await?;

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

    // Set compute budget for AdvancePool (needs more than default 200k CUs)
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_POOL_COMPUTE_UNITS);
    let ix = build_advance_pool_ix(authority, authority, node_address);

    info!(
        epoch = epoch.as_u64(),
        node = %node_address,
        "Submitting AdvancePool"
    );

    // Submit the transaction with timing
    let start = std::time::Instant::now();
    info!(epoch = epoch.as_u64(), "AdvancePool RPC call starting");

    let result = ctx.rpc.send_instructions(&ctx.keypair, vec![cu_ix, ix]).await;
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

    // AdvanceEpoch requires more compute units due to committee rotation and spool reallocation
    let compute_budget_ix =
        ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_EPOCH_COMPUTE_UNITS);
    let ix = build_advance_epoch_ix(authority, authority);

    info!(epoch = epoch.as_u64(), "Submitting AdvanceEpoch");

    // Submit the transaction with compute budget
    match ctx
        .rpc
        .send_instructions(&ctx.keypair, vec![compute_budget_ix, ix])
        .await
    {
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
/// Uses FSM to determine when conditions are met for epoch advancement.
/// Uses exponential backoff when system is stuck waiting for nodes.
async fn monitor_epoch_for_advancement(ctx: Arc<NodeContext>, starting_epoch: EpochNumber) {
    info!(
        epoch = starting_epoch.as_u64(),
        "Starting epoch advancement monitor"
    );

    // Use exponential backoff when stuck, reset on progress
    let mut backoff = EPOCH_ADVANCE_POLL_INTERVAL;
    const MAX_BACKOFF: Duration = Duration::from_secs(300); // 5 minutes max

    loop {
        tokio::time::sleep(backoff).await;

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

        // Fetch system state for committee_next check
        let system = match ctx.rpc.get_system().await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to fetch system state, retrying");
                continue;
            }
        };

        // Update control plane with fresh state for FSM
        ctx.control_plane.update_system(system);
        ctx.control_plane.update_epoch(epoch.clone());

        // Use FSM to determine what action to take
        let now = current_timestamp();
        let (action, _) = ctx.control_plane.determine_action(now);

        match action {
            NodeAction::AdvanceEpoch => {
                info!(
                    epoch = epoch.id.as_u64(),
                    "FSM says AdvanceEpoch - all conditions met"
                );
                match submit_advance_epoch(&ctx, epoch.id).await {
                    Ok(_) => {
                        info!(epoch = epoch.id.as_u64(), "AdvanceEpoch submitted");
                        return;
                    }
                    Err(e) => {
                        // Log error but continue polling - another node may have advanced it
                        warn!(epoch = epoch.id.as_u64(), error = %e, "Failed to submit AdvanceEpoch");
                        backoff = EPOCH_ADVANCE_POLL_INTERVAL;
                    }
                }
            }
            NodeAction::WaitForEpochDuration { seconds_remaining } => {
                debug!(
                    epoch = epoch.id.as_u64(),
                    seconds = seconds_remaining,
                    "Waiting for epoch duration to elapse"
                );
                backoff = EPOCH_ADVANCE_POLL_INTERVAL;
            }
            NodeAction::WaitForSettleQuorum { current_weight } => {
                debug!(
                    epoch = epoch.id.as_u64(),
                    weight = current_weight,
                    "Waiting for settle quorum"
                );
                backoff = EPOCH_ADVANCE_POLL_INTERVAL;
            }
            NodeAction::WaitForCommitteeThreshold {
                current_size,
                required_size,
            } => {
                debug!(
                    epoch = epoch.id.as_u64(),
                    current = current_size,
                    required = required_size,
                    backoff_secs = backoff.as_secs(),
                    "Waiting for committee_next threshold"
                );
                // Use exponential backoff when waiting for more nodes
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            NodeAction::EpochBlocked { committee_next_size } => {
                debug!(
                    epoch = epoch.id.as_u64(),
                    committee_next = committee_next_size,
                    backoff_secs = backoff.as_secs(),
                    "Epoch blocked: waiting for more nodes to join"
                );
                // Use exponential backoff when stuck
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            _ => {
                // Other states (SyncEpoch, AdvancePool, JoinNetwork, etc.)
                // shouldn't happen in this monitor, but reset backoff
                debug!(
                    epoch = epoch.id.as_u64(),
                    action = ?action,
                    "Unexpected FSM state in advancement monitor"
                );
                backoff = EPOCH_ADVANCE_POLL_INTERVAL;
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

/// Log FSM action for debugging and observability.
fn log_fsm_action(action: &NodeAction, epoch: EpochNumber) {
    match action {
        NodeAction::SyncEpoch => {
            info!(epoch = epoch.as_u64(), "FSM action: SyncEpoch");
        }
        NodeAction::AdvancePool => {
            info!(epoch = epoch.as_u64(), "FSM action: AdvancePool");
        }
        NodeAction::JoinNetwork => {
            info!(epoch = epoch.as_u64(), "FSM action: JoinNetwork");
        }
        NodeAction::AdvanceEpoch => {
            info!(epoch = epoch.as_u64(), "FSM action: AdvanceEpoch");
        }
        NodeAction::WaitForSyncQuorum { current_weight } => {
            debug!(
                epoch = epoch.as_u64(),
                weight = current_weight,
                "FSM state: waiting for sync quorum"
            );
        }
        NodeAction::WaitForSettleQuorum { current_weight } => {
            debug!(
                epoch = epoch.as_u64(),
                weight = current_weight,
                "FSM state: waiting for settle quorum"
            );
        }
        NodeAction::WaitForEpochDuration { seconds_remaining } => {
            debug!(
                epoch = epoch.as_u64(),
                seconds = seconds_remaining,
                "FSM state: waiting for epoch duration"
            );
        }
        NodeAction::WaitForCommitteeThreshold {
            current_size,
            required_size,
        } => {
            debug!(
                epoch = epoch.as_u64(),
                current = current_size,
                required = required_size,
                "FSM state: waiting for committee threshold"
            );
        }
        NodeAction::NotInCommittee => {
            debug!(epoch = epoch.as_u64(), "FSM state: not in committee");
        }
        NodeAction::EpochBlocked { committee_next_size } => {
            warn!(
                epoch = epoch.as_u64(),
                committee_next = committee_next_size,
                "FSM state: epoch blocked"
            );
        }
        NodeAction::UnknownPhase { phase } => {
            error!(epoch = epoch.as_u64(), phase, "FSM state: unknown phase");
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
