//! FSM loop and epoch synchronization handler.
//!
//! Handles epoch transitions and spool synchronization:
//! - Detects new spool assignments after epoch changes
//! - Syncs data from previous spool owners
//! - Falls back to erasure recovery if sync fails
//! - Submits SyncEpoch transaction when ready
//!
//! NOTE: Spool sync and recovery operations are currently stubs pending
//! storage layer redesign.

use std::sync::Arc;
use std::time::Duration;

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signer::Signer;
use tape_api::errors::TapeError;
use tape_api::fsm::NodeAction;
use tape_api::instruction::{
    build_advance_epoch_ix, build_advance_pool_ix, build_epoch_sync_ix, build_join_network_ix,
};
use tape_api::program::tapedrive::node_pda;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::core::context::NodeContext;
use crate::features::spool_sync::SpoolSyncHandler;

/// Outcome of executing an FSM action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerOutcome {
    /// Action completed successfully (or already done by another node).
    Completed,
    /// Action not ready yet (timing, threshold, etc.).
    RetryLater,
}

/// Signals from block processor to wake FSM loop.
#[derive(Debug, Clone)]
pub enum FsmSignal {
    /// On-chain state changed, re-evaluate FSM immediately.
    StateChanged,
}

/// Polling interval for epoch advancement monitoring.
pub const EPOCH_ADVANCE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Compute units required for AdvanceEpoch instruction.
pub const ADVANCE_EPOCH_COMPUTE_UNITS: u32 = 1_400_000;

/// Compute units required for AdvancePool instruction.
pub const ADVANCE_POOL_COMPUTE_UNITS: u32 = 400_000;

/// Error type for network sync operations.
#[derive(Debug, thiserror::Error)]
pub enum NetworkSyncError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("sync error: {0}")]
    Sync(String),

    #[error("storage error: {0}")]
    Storage(String),
}

/// Categorize RPC/program errors into handler outcomes.
fn categorize_tx_error(err: &str, action_name: &str) -> Result<HandlerOutcome, NetworkSyncError> {
    if let Some(tape_err) = TapeError::from_error_string(err) {
        return match tape_err {
            e if e.is_already_done() => {
                info!("{} already complete: {}", action_name, e);
                Ok(HandlerOutcome::Completed)
            }
            TapeError::NotStaked => {
                info!("{} not applicable: not staked", action_name);
                Ok(HandlerOutcome::Completed)
            }
            e if e.is_retriable() => {
                debug!("{} not ready yet ({}), will retry", action_name, e);
                Ok(HandlerOutcome::RetryLater)
            }
            TapeError::NodeStale => Err(NetworkSyncError::Rpc(format!(
                "{} failed: AdvancePool required first (NodeStale)",
                action_name
            ))),
            _ => Err(NetworkSyncError::Rpc(format!(
                "{} failed: {} ({})",
                action_name,
                tape_err.user_message(),
                tape_err
            ))),
        };
    }

    Err(NetworkSyncError::Rpc(format!(
        "{} failed: {}",
        action_name, err
    )))
}

/// Get the current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Try to advance the epoch.
async fn try_advance_epoch(ctx: &NodeContext) -> Result<HandlerOutcome, NetworkSyncError> {
    let authority = ctx.keypair.pubkey();
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_EPOCH_COMPUTE_UNITS);
    let ix = build_advance_epoch_ix(authority, authority);

    match ctx
        .rpc
        .send_instructions(&ctx.keypair, vec![cu_ix, ix])
        .await
    {
        Ok(_) => {
            info!("AdvanceEpoch submitted successfully");
            ctx.metrics.epoch_transitions_total.inc();
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "AdvanceEpoch"),
    }
}

/// Try to advance the staking pool.
async fn try_advance_pool(ctx: &NodeContext) -> Result<HandlerOutcome, NetworkSyncError> {
    if !ctx.is_in_committee() {
        debug!("Not in committee, skipping AdvancePool");
        return Ok(HandlerOutcome::Completed);
    }

    let authority = ctx.keypair.pubkey();
    let (node_address, _) = node_pda(authority);
    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_POOL_COMPUTE_UNITS);
    let ix = build_advance_pool_ix(authority, authority, node_address);

    let result = match ctx
        .rpc
        .send_instructions(&ctx.keypair, vec![cu_ix, ix])
        .await
    {
        Ok(_) => {
            info!("AdvancePool submitted successfully");
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "AdvancePool"),
    };

    if let Ok(node) = ctx.rpc.get_node(&authority).await {
        ctx.control_plane.update_node(node);
    }

    result
}

/// Try to join the network (re-join committee_next).
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
            if let Ok(node) = ctx.rpc.get_node(&authority).await {
                ctx.control_plane.update_node(node);
            }
            Ok(HandlerOutcome::Completed)
        }
        Err(e) => categorize_tx_error(&e.to_string(), "JoinNetwork"),
    }
}

/// Try to sync the epoch.
async fn try_sync_epoch(
    ctx: &NodeContext,
    _sync_handler: &SpoolSyncHandler,
) -> Result<HandlerOutcome, NetworkSyncError> {
    let epoch = ctx.control_plane.current_epoch();

    // Step 1: Ensure spool data is synced (stub - just mark complete)
    if !ctx.control_plane.is_local_sync_complete(epoch) {
        debug!(epoch = epoch.as_u64(), "Spool sync (stub) - marking complete");
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

/// Run the FSM-driven network sync loop.
pub async fn run(
    ctx: Arc<NodeContext>,
    mut signal_rx: mpsc::Receiver<FsmSignal>,
    cancel: CancellationToken,
) -> Result<(), NetworkSyncError> {
    info!("FSM loop starting, waiting for catch-up to complete");

    let sync_handler = SpoolSyncHandler::new()
        .with_max_concurrent(ctx.config.sync_concurrency.unwrap_or(4))
        .with_batch_size(ctx.config.sync_batch_size.unwrap_or(1000))
        .with_insecure(ctx.config.insecure);

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
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("FSM loop shutting down");
                break;
            }

            Some(FsmSignal::StateChanged) = signal_rx.recv() => {
                debug!("Received StateChanged signal, re-evaluating FSM");
            }

            _ = interval.tick() => {
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

        if catching_up {
            warn!("Unexpectedly catching up in main loop, skipping action");
            continue;
        }

        if action.requires_transaction() {
            let node_id = ctx.control_plane.our_node_id();
            let in_committee = ctx.control_plane.is_in_committee();
            debug!(
                node_id = node_id.as_u64(),
                in_committee = in_committee,
                action = ?action,
                "FSM determined action"
            );
        }

        execute_action(&ctx, &sync_handler, &action).await;
    }

    Ok(())
}

/// Refresh on-chain state into control plane.
pub async fn refresh_state(ctx: &NodeContext) -> Result<(), NetworkSyncError> {
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
pub async fn execute_action(
    ctx: &NodeContext,
    sync_handler: &SpoolSyncHandler,
    action: &NodeAction,
) {
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
        NodeAction::WaitForCommitteeThreshold {
            current_size,
            required_size,
        } => {
            debug!(
                current = current_size,
                required = required_size,
                "Waiting for committee threshold"
            );
            return;
        }
        NodeAction::EpochBlocked { committee_next_size } => {
            debug!(
                size = committee_next_size,
                "Epoch blocked - waiting for more nodes"
            );
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

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
