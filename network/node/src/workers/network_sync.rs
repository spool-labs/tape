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
use crate::storage::service::{Compression, SliceMeta};
use crate::sync::types::{track_id_to_pubkey, SyncSlice};
use crate::sync::{SpoolSyncHandler, SyncError};

/// Outcome of executing an FSM action.
///
/// This enum replaces the scattered boolean/unit returns from submit functions,
/// providing a consistent way to communicate results back to the FSM loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerOutcome {
    /// Action completed successfully (or already done by another node).
    /// The FSM loop should move on to the next action.
    Completed,
    /// Action not ready yet (timing, threshold, etc.).
    /// The FSM loop will retry on the next iteration.
    RetryLater,
}

/// Signals from block processor to wake FSM loop.
///
/// These signals allow the FSM loop to react immediately to on-chain state
/// changes rather than waiting for the next polling interval.
#[derive(Debug, Clone)]
pub enum FsmSignal {
    /// On-chain state changed, re-evaluate FSM immediately.
    StateChanged,
}

/// Polling interval for epoch advancement monitoring.
const EPOCH_ADVANCE_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Compute units required for AdvanceEpoch instruction.
/// AdvanceEpoch performs committee rotation and spool reallocation which
/// requires significant computation, especially with many nodes.
const ADVANCE_EPOCH_COMPUTE_UNITS: u32 = 1_400_000;

/// Compute units required for AdvancePool instruction.
/// AdvancePool calculates rewards based on committee size and spool assignment,
/// which can exceed the default 200k CU limit with larger committees.
const ADVANCE_POOL_COMPUTE_UNITS: u32 = 400_000;

/// Categorize RPC/program errors into handler outcomes.
///
/// This centralizes error handling logic that was previously scattered across
/// all submit functions. Each error code maps to either:
/// - `Completed`: The action is done (by us or another node), stop trying
/// - `RetryLater`: The action isn't ready yet, try again next loop
/// - `Err`: Fatal error, report and stop
fn categorize_tx_error(err: &str, action_name: &str) -> Result<HandlerOutcome, NetworkSyncError> {
    // Already completed (by us or another node)
    if err.contains("0x40") ||  // BadEpochState - epoch not in expected phase
       err.contains("0x62") ||  // AlreadyAdvanced - already did AdvancePool
       err.contains("0x10") ||  // UnexpectedState - already in committee
       err.contains("0x4a") ||  // AlreadySynced - already synced this epoch
       err.contains("AlreadyInCommittee") ||
       err.contains("BadEpochState") ||
       err.contains("AlreadyAdvanced") ||
       err.contains("AlreadySynced") {
        info!("{} already complete (or not needed)", action_name);
        return Ok(HandlerOutcome::Completed);
    }

    // Timing/threshold issues - retry later
    if err.contains("0x41") ||  // TooSoon - epoch duration not elapsed
       err.contains("0x55") ||  // InsufficientCommittee - committee_next too small
       err.contains("TooSoon") ||
       err.contains("InsufficientCommittee") {
        debug!("{} not ready yet, will retry", action_name);
        return Ok(HandlerOutcome::RetryLater);
    }

    // Fatal errors - node state is invalid
    if err.contains("0x60") || err.contains("NodeStale") {
        return Err(NetworkSyncError::Rpc(
            format!("{} failed: AdvancePool required first (NodeStale)", action_name)
        ));
    }

    // Unknown error - treat as fatal to avoid silent failures
    Err(NetworkSyncError::Rpc(format!("{} failed: {}", action_name, err)))
}

/// Get the current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Try to advance the epoch.
///
/// Submits AdvanceEpoch transaction if conditions are met.
/// Uses centralized error categorization for consistent handling.
async fn try_advance_epoch(ctx: &NodeContext) -> Result<HandlerOutcome, NetworkSyncError> {
    let authority = ctx.keypair.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_EPOCH_COMPUTE_UNITS);
    let ix = build_advance_epoch_ix(authority, authority);

    match ctx.rpc.send_instructions(&ctx.keypair, vec![cu_ix, ix]).await {
        Ok(_) => {
            info!("AdvanceEpoch submitted successfully");
            ctx.metrics.epoch_transitions_total.inc();
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "AdvanceEpoch"),
    }
}

/// Try to advance the staking pool.
///
/// Submits AdvancePool transaction to claim rewards and contribute
/// to settle quorum. Must be done before JoinNetwork.
async fn try_advance_pool(ctx: &NodeContext) -> Result<HandlerOutcome, NetworkSyncError> {
    if !ctx.is_in_committee() {
        debug!("Not in committee, skipping AdvancePool");
        return Ok(HandlerOutcome::Completed);
    }

    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_POOL_COMPUTE_UNITS);
    let ix = build_advance_pool_ix(authority, authority, node_address);

    match ctx.rpc.send_instructions(&ctx.keypair, vec![cu_ix, ix]).await {
        Ok(_) => {
            info!("AdvancePool submitted successfully");
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "AdvancePool"),
    }
}

/// Try to join the network (re-join committee_next).
///
/// After each epoch rotation, nodes must call JoinNetwork to
/// re-establish membership for the next epoch.
async fn try_join_network(ctx: &NodeContext) -> Result<HandlerOutcome, NetworkSyncError> {
    if !ctx.is_in_committee() {
        debug!("Not in committee, skipping JoinNetwork");
        return Ok(HandlerOutcome::Completed);
    }

    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);
    let ix = build_join_network_ix(authority, authority, node_address);

    match ctx.rpc.send_instructions(&ctx.keypair, vec![ix]).await {
        Ok(_) => {
            info!("JoinNetwork submitted successfully");
            // Refresh node state so FSM sees updated committee membership
            if let Ok(node) = ctx.rpc.get_node(&authority).await {
                ctx.control_plane.update_node(node);
            }
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "JoinNetwork"),
    }
}

/// Try to sync the epoch.
///
/// Ensures spool data is synced from previous owners, then submits
/// SyncEpoch transaction to contribute to sync quorum.
async fn try_sync_epoch(ctx: &NodeContext, sync_handler: &SpoolSyncHandler) -> Result<HandlerOutcome, NetworkSyncError> {
    let epoch = ctx.control_plane.current_epoch();

    // Step 1: Ensure spool data is synced (only once per epoch)
    if !ctx.control_plane.is_local_sync_complete(epoch) {
        // Sync new spools from previous owners
        if let Err(e) = sync_new_spools(ctx, sync_handler).await {
            warn!(epoch = epoch.as_u64(), error = %e, "Spool sync had errors, continuing anyway");
            // Don't fail - we can still attest and recover later
        }
        ctx.control_plane.mark_local_sync_complete(epoch);
    }

    // Step 2: Submit SyncEpoch transaction
    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);
    let assigned_spools = ctx.control_plane.get_our_spools();
    let ix = build_epoch_sync_ix(authority, authority, node_address, epoch, &assigned_spools);

    info!(
        epoch = epoch.as_u64(),
        spools = assigned_spools.len(),
        "Submitting SyncEpoch"
    );

    match ctx.rpc.send_instructions(&ctx.keypair, vec![ix]).await {
        Ok(_) => {
            info!(epoch = epoch.as_u64(), "SyncEpoch submitted successfully");
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "SyncEpoch"),
    }
}

/// Sync newly assigned spools from previous owners.
async fn sync_new_spools(ctx: &NodeContext, sync_handler: &SpoolSyncHandler) -> Result<(), NetworkSyncError> {
    let system = ctx.control_plane.get_system();
    let our_node_id = ctx.control_plane.our_node_id();

    // Find our member index in current committee
    let curr_index = match system.committee.index_of(&our_node_id) {
        Some(idx) => idx,
        None => {
            warn!("Not found in current committee");
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

    info!(
        gained = gained_spools.len(),
        total = curr_spools.len(),
        "Syncing new spool assignments"
    );

    // Sync gained spools from previous owners
    for spool_idx in &gained_spools {
        let prev_owner_member_idx = system.spools_prev.0[*spool_idx as usize] as usize;

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

    Ok(())
}

/// Run the FSM-driven network sync loop.
///
/// This is the main entry point for network synchronization. It:
/// 1. Waits for catch-up to complete (historical block replay)
/// 2. Runs the FSM loop to execute actions based on on-chain state
///
/// The loop polls every 3 seconds OR immediately when signaled by the
/// block processor that state has changed.
pub async fn run(
    ctx: Arc<NodeContext>,
    mut signal_rx: mpsc::Receiver<FsmSignal>,
    cancel: CancellationToken,
) -> Result<(), NetworkSyncError> {
    info!("FSM loop starting, waiting for catch-up to complete");

    let sync_handler = SpoolSyncHandler::new()
        .with_max_concurrent(ctx.config.sync_concurrency.unwrap_or(4))
        .with_batch_size(ctx.config.sync_batch_size.unwrap_or(1000));

    // Wait for catch-up to complete before making FSM decisions
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("FSM loop shutting down during catch-up");
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                if ctx.control_plane.is_caught_up() {
                    break;
                }
                let local = ctx.control_plane.current_epoch();
                let chain = ctx.control_plane.chain_epoch();
                debug!(local = local.as_u64(), chain = chain.as_u64(), "Waiting for catch-up");
            }
        }
    }

    info!("Catch-up complete, entering main FSM loop");
    let mut interval = tokio::time::interval(EPOCH_ADVANCE_POLL_INTERVAL);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("FSM loop shutting down");
                break;
            }

            // Block processor signals state change (only sent when caught up)
            Some(FsmSignal::StateChanged) = signal_rx.recv() => {
                debug!("Received StateChanged signal, re-evaluating FSM");
                // Fall through to execute
            }

            // Regular polling interval
            _ = interval.tick() => {
                // Fall through to execute
            }
        }

        // Refresh state from chain
        if let Err(e) = refresh_state(&ctx).await {
            warn!(error = %e, "Failed to refresh state, retrying next iteration");
            continue;
        }

        // Ask FSM what to do
        let now = current_timestamp();
        let (action, catching_up) = ctx.control_plane.determine_action(now);

        // Safety check - shouldn't happen but belt-and-suspenders
        if catching_up {
            warn!("Unexpectedly catching up in main loop, skipping action");
            continue;
        }

        // Execute action
        execute_action(&ctx, &sync_handler, &action).await;
    }

    Ok(())
}

/// Refresh on-chain state into control plane.
async fn refresh_state(ctx: &NodeContext) -> Result<(), NetworkSyncError> {
    let epoch = ctx
        .rpc
        .get_epoch()
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to fetch epoch: {}", e)))?;

    let system = ctx
        .rpc
        .get_system()
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to fetch system: {}", e)))?;

    let node = ctx
        .rpc
        .get_node(&ctx.keypair.pubkey())
        .await
        .map_err(|e| NetworkSyncError::Rpc(format!("Failed to fetch node: {}", e)))?;

    ctx.control_plane.set_chain_epoch(epoch.id);
    ctx.control_plane.update_system(system);
    ctx.control_plane.update_epoch(epoch);
    ctx.control_plane.update_node(node);

    Ok(())
}

/// Execute an FSM action.
async fn execute_action(ctx: &NodeContext, sync_handler: &SpoolSyncHandler, action: &NodeAction) {
    let result = match action {
        NodeAction::AdvanceEpoch => {
            info!("FSM: AdvanceEpoch");
            try_advance_epoch(ctx).await
        }
        NodeAction::SyncEpoch => {
            info!("FSM: SyncEpoch");
            try_sync_epoch(ctx, sync_handler).await
        }
        NodeAction::AdvancePool => {
            info!("FSM: AdvancePool");
            try_advance_pool(ctx).await
        }
        NodeAction::JoinNetwork => {
            info!("FSM: JoinNetwork");
            try_join_network(ctx).await
        }

        // Wait states - nothing to execute, just log
        NodeAction::WaitForEpochDuration { seconds_remaining } => {
            debug!(seconds = seconds_remaining, "Waiting for epoch duration");
            return;
        }
        NodeAction::WaitForSyncQuorum { current_weight } => {
            debug!(weight = current_weight, "Waiting for sync quorum");
            return;
        }
        NodeAction::WaitForSettleQuorum { current_weight } => {
            debug!(weight = current_weight, "Waiting for settle quorum");
            return;
        }
        NodeAction::WaitForCommitteeThreshold { current_size, required_size } => {
            debug!(current = current_size, required = required_size, "Waiting for committee threshold");
            return;
        }
        NodeAction::EpochBlocked { committee_next_size } => {
            debug!(size = committee_next_size, "Epoch blocked - waiting for more nodes");
            return;
        }
        NodeAction::NotInCommittee => {
            debug!("Not in committee, no action needed");
            return;
        }
        NodeAction::UnknownPhase { phase } => {
            warn!(phase = phase, "Unknown epoch phase");
            return;
        }
    };

    // Log outcome
    match result {
        Ok(HandlerOutcome::Completed) => {
            info!(action = ?action, "Action completed");
        }
        Ok(HandlerOutcome::RetryLater) => {
            debug!(action = ?action, "Action needs retry, will try again next iteration");
        }
        Err(e) => {
            error!(action = ?action, error = %e, "Action failed");
        }
    }
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
