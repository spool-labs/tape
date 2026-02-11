//! FSM loop and epoch synchronization handler.
//!
//! Handles epoch transitions and spool synchronization:
//! - Detects new spool assignments after epoch changes
//! - Evaluates NodeStatus FSM and dispatches recovery tasks
//! - Submits SyncEpoch transaction when ready

use std::collections::HashSet;
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
use tape_core::spooler::SpoolIndex;
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::core::context::NodeContext;
use tape_store::ops::SpoolOps;
use tape_store::types::SpoolStatus;

use crate::features::lifecycle::{NodeEvent, evaluate_transition, start_node_recovery, run_metadata_sync};
use crate::features::recovery::{LiveUploadDeferral, TrackSyncHandler, start_spool_recovery};
use crate::features::sync::SpoolSyncHandler;

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
    /// Block processor detected the node is behind by `lag` epochs.
    DetectedLag { lag: u64 },
}

/// Polling interval for epoch advancement monitoring.
pub const EPOCH_ADVANCE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Minimum backoff for SyncEpoch tx retries.
const SYNC_EPOCH_MIN_BACKOFF: Duration = Duration::from_secs(1);

/// Maximum backoff for SyncEpoch tx retries.
const SYNC_EPOCH_MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// Tracks exponential backoff for SyncEpoch transaction failures.
struct SyncEpochRetry {
    consecutive_failures: u32,
    last_attempt: Option<tokio::time::Instant>,
}

impl SyncEpochRetry {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            last_attempt: None,
        }
    }

    /// Returns true if enough time has elapsed since the last failure.
    fn should_attempt(&self) -> bool {
        match self.last_attempt {
            None => true,
            Some(last) => {
                let backoff =
                    SYNC_EPOCH_MIN_BACKOFF * 2u32.saturating_pow(self.consecutive_failures);
                let backoff = backoff.min(SYNC_EPOCH_MAX_BACKOFF);
                last.elapsed() >= backoff
            }
        }
    }

    fn record_failure(&mut self) {
        self.last_attempt = Some(tokio::time::Instant::now());
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.last_attempt = None;
    }
}

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
///
/// Returns `RetryLater` until local spool sync is complete, then submits
/// the SyncEpoch transaction on-chain.
async fn try_sync_epoch(
    ctx: &NodeContext,
    _sync_handler: &SpoolSyncHandler,
) -> Result<HandlerOutcome, NetworkSyncError> {
    let epoch = ctx.control_plane.current_epoch();

    if ctx.control_plane.is_stale_epoch(epoch) {
        debug!(epoch = epoch.as_u64(), "epoch already advanced, skipping SyncEpoch");
        return Ok(HandlerOutcome::Completed);
    }

    if !ctx.control_plane.is_local_sync_complete(epoch) {
        debug!(epoch = epoch.as_u64(), "local sync not yet complete, waiting");
        return Ok(HandlerOutcome::RetryLater);
    }

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

/// Apply a NodeStatus transition: update control plane + persist to store.
fn apply_node_status_transition(
    ctx: &NodeContext,
    new_status: NodeStatus,
) {
    ctx.control_plane.set_node_status(new_status.clone());
    if let Err(e) = ctx.storage.store.set_node_status(new_status) {
        warn!(error = %e, "failed to persist node status");
    }
}

/// Handle a NodeStatus transition by dispatching the appropriate action.
fn dispatch_node_status(
    ctx: &Arc<NodeContext>,
    new_status: &NodeStatus,
    epoch: tape_core::types::EpochNumber,
    track_sync: &Arc<TrackSyncHandler>,
    deferral: &Arc<LiveUploadDeferral>,
    sync_handler: &SpoolSyncHandler,
    cancel: &CancellationToken,
) {
    match new_status {
        NodeStatus::RecoveryInProgress { epoch: recovery_epoch } => {
            let our_spools = ctx.control_plane.get_our_spools();
            let ctx = Arc::clone(ctx);
            let track_sync = Arc::clone(track_sync);
            let deferral = Arc::clone(deferral);
            let cancel = cancel.clone();
            let recovery_epoch = *recovery_epoch;

            tokio::spawn(async move {
                start_node_recovery(ctx, recovery_epoch, our_spools, track_sync, deferral, cancel)
                    .await;
            });
        }
        NodeStatus::RecoverMetadata => {
            let ctx = Arc::clone(ctx);
            let cancel = cancel.clone();
            let sync_handler = sync_handler.clone();
            tokio::spawn(async move {
                run_metadata_sync(ctx, sync_handler, cancel).await;
            });
        }
        NodeStatus::RecoveryReplay | NodeStatus::PartialReplay { .. } => {
            let ctx = Arc::clone(ctx);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                match crate::features::snapshot::bootstrap::bootstrap_from_snapshots(
                    ctx, cancel
                ).await {
                    Ok(()) => info!("Snapshot bootstrap from lag detection complete"),
                    Err(e) => warn!(error = %e, "Snapshot bootstrap failed, block processor will catch up"),
                }
            });
        }
        NodeStatus::Active => {
            // Directly active — mark local sync complete
            ctx.control_plane.mark_local_sync_complete(epoch);
        }
        _ => {}
    }
}

/// Resume recovery on startup based on persisted NodeStatus.
fn resume_from_persisted_status(
    ctx: &Arc<NodeContext>,
    track_sync: &Arc<TrackSyncHandler>,
    deferral: &Arc<LiveUploadDeferral>,
    sync_handler: &SpoolSyncHandler,
    cancel: &CancellationToken,
) {
    let status = ctx.control_plane.get_node_status();
    match &status {
        NodeStatus::RecoveryInProgress { epoch } => {
            info!(epoch = epoch.as_u64(), "resuming node recovery from persisted state");
            let our_spools = ctx.control_plane.get_our_spools();
            let ctx = Arc::clone(ctx);
            let track_sync = Arc::clone(track_sync);
            let deferral = Arc::clone(deferral);
            let cancel = cancel.clone();
            let epoch = *epoch;

            tokio::spawn(async move {
                start_node_recovery(ctx, epoch, our_spools, track_sync, deferral, cancel).await;
            });
        }
        NodeStatus::RecoverMetadata => {
            info!("resuming metadata sync from persisted state");
            let ctx = Arc::clone(ctx);
            let cancel = cancel.clone();
            let sync_handler = sync_handler.clone();
            tokio::spawn(async move {
                run_metadata_sync(ctx, sync_handler, cancel).await;
            });
        }
        NodeStatus::Active => {
            let our_spools = ctx.control_plane.get_our_spools();
            let has_recovering = our_spools.iter().any(|&s| {
                ctx.storage.store.get_spool_status(s).ok().flatten()
                    != Some(SpoolStatus::Active)
            });
            if has_recovering {
                info!("resuming spool recovery on restart");
                let ctx = Arc::clone(ctx);
                let cancel = cancel.clone();
                let sync_handler = sync_handler.clone();
                tokio::spawn(async move {
                    start_spool_recovery(ctx, sync_handler, cancel).await;
                });
            } else {
                ctx.control_plane.mark_local_sync_complete(ctx.control_plane.current_epoch());
            }
        }
        NodeStatus::RecoveryReplay | NodeStatus::PartialReplay { .. } => {
            info!("resuming from replay state — triggering snapshot bootstrap");
            let ctx = Arc::clone(ctx);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                match crate::features::snapshot::bootstrap::bootstrap_from_snapshots(
                    ctx, cancel
                ).await {
                    Ok(()) => info!("Snapshot bootstrap from resume complete"),
                    Err(e) => warn!(error = %e, "Snapshot bootstrap failed on resume"),
                }
            });
        }
        _ => {}
    }
}

/// Run the FSM-driven network sync loop.
pub async fn run(
    ctx: Arc<NodeContext>,
    mut signal_rx: mpsc::Receiver<FsmSignal>,
    track_sync: Arc<TrackSyncHandler>,
    deferral: Arc<LiveUploadDeferral>,
    cancel: CancellationToken,
) -> Result<(), NetworkSyncError> {
    info!("FSM loop starting, waiting for catch-up to complete");

    let sync_handler = SpoolSyncHandler::new()
        .with_max_concurrent(ctx.config.sync_concurrency.unwrap_or(10))
        .with_batch_size(ctx.config.sync_batch_size.unwrap_or(1000) as u32)
        .with_insecure(ctx.config.insecure);

    // If lagging, attempt snapshot bootstrap before grinding through blocks
    if ctx.control_plane.is_catching_up() {
        let node_status = ctx.control_plane.get_node_status();
        if matches!(node_status, NodeStatus::RecoveryReplay | NodeStatus::PartialReplay { .. })
            || ctx.control_plane.is_catching_up()
        {
            info!("Node is lagging, attempting snapshot bootstrap");
            match crate::features::snapshot::bootstrap::bootstrap_from_snapshots(
                Arc::clone(&ctx), cancel.clone()
            ).await {
                Ok(()) => info!("Snapshot bootstrap complete"),
                Err(e) => warn!(error = %e, "Snapshot bootstrap unavailable, falling back to block replay"),
            }
        }
    }

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

    // Resume any in-progress recovery from persisted state
    resume_from_persisted_status(&ctx, &track_sync, &deferral, &sync_handler, &cancel);

    let mut interval = tokio::time::interval(EPOCH_ADVANCE_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sync_retry = SyncEpochRetry::new();
    let mut last_evaluated_epoch = ctx.control_plane.current_epoch();
    let mut prev_spools = ctx.control_plane.get_our_spools();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("FSM loop shutting down");
                break;
            }

            Some(signal) = signal_rx.recv() => {
                match signal {
                    FsmSignal::StateChanged => {
                        debug!("Received StateChanged signal, re-evaluating FSM");
                    }
                    FsmSignal::DetectedLag { lag } => {
                        let current_status = ctx.control_plane.get_node_status();
                        let event = NodeEvent::DetectedLag { lag };
                        if let Some(new_status) = evaluate_transition(&current_status, &event) {
                            info!(
                                lag,
                                from = ?current_status,
                                to = ?new_status,
                                "NodeStatus transition from lag detection"
                            );
                            apply_node_status_transition(&ctx, new_status);
                        }
                    }
                }
            }

            _ = interval.tick() => {
            }
        }

        // Refresh state from chain
        if let Err(e) = refresh_state(&ctx).await {
            warn!(error = %e, "Failed to refresh state, retrying next iteration");
            continue;
        }

        // Detect epoch changes after refreshing state
        let current_epoch = ctx.control_plane.current_epoch();
        if current_epoch != last_evaluated_epoch {
            info!(
                prev = last_evaluated_epoch.as_u64(),
                current = current_epoch.as_u64(),
                "Epoch change detected in FSM loop"
            );

            let current_spools = ctx.control_plane.get_our_spools();
            let in_committee = ctx.control_plane.is_in_committee();
            let prev_set: HashSet<SpoolIndex> = prev_spools.iter().copied().collect();
            let new_spools: Vec<SpoolIndex> = current_spools
                .iter()
                .filter(|s| !prev_set.contains(s))
                .copied()
                .collect();

            // Seed status for carried-over spools
            for &spool in &current_spools {
                if prev_set.contains(&spool) {
                    if ctx.storage.store.get_spool_status(spool).ok().flatten()
                        != Some(SpoolStatus::Active)
                    {
                        let _ = ctx.storage.store.set_spool_status(spool, SpoolStatus::Active);
                    }
                }
            }

            sync_retry.reset();

            let current_status = ctx.control_plane.get_node_status();
            let event = NodeEvent::EpochChanged {
                processed_epoch: current_epoch,
                latest_epoch: ctx.control_plane.chain_epoch(),
                in_committee,
                new_spools: new_spools.clone(),
            };

            if let Some(new_status) = evaluate_transition(&current_status, &event) {
                info!(
                    from = ?current_status,
                    to = ?new_status,
                    "NodeStatus transition from epoch change"
                );
                apply_node_status_transition(&ctx, new_status.clone());
                dispatch_node_status(
                    &ctx, &new_status, current_epoch, &track_sync, &deferral, &sync_handler, &cancel,
                );
            } else if in_committee && new_spools.is_empty() {
                if matches!(current_status, NodeStatus::Active) {
                    ctx.control_plane.mark_local_sync_complete(current_epoch);
                }
            }

            // Spawn snapshot build + certify for the completed epoch
            if in_committee {
                let snap_ctx = Arc::clone(&ctx);
                let completed_epoch = last_evaluated_epoch;
                tokio::spawn(async move {
                    if let Err(e) = crate::features::snapshot::builder::build_and_certify(
                        snap_ctx,
                        completed_epoch,
                    ).await {
                        warn!(
                            epoch = completed_epoch.as_u64(),
                            error = %e,
                            "Snapshot build/certify failed"
                        );
                    }
                });
            }

            last_evaluated_epoch = current_epoch;
            prev_spools = current_spools;
        }

        // Ask FSM what to do
        let now = current_timestamp();
        let (action, catching_up) = ctx.control_plane.determine_action(now);

        if catching_up {
            warn!("Unexpectedly catching up in main loop, skipping action");
            continue;
        }

        // Apply backoff for SyncEpoch failures
        if matches!(action, NodeAction::SyncEpoch) && !sync_retry.should_attempt() {
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

        let result = execute_action(&ctx, &sync_handler, &action).await;

        // Track SyncEpoch retry state
        if matches!(action, NodeAction::SyncEpoch) {
            match result {
                Some(Ok(HandlerOutcome::Completed)) => sync_retry.reset(),
                Some(Err(_)) => sync_retry.record_failure(),
                _ => {}
            }
        }
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

/// Execute an FSM action. Returns the result for actions that submit
/// transactions, or `None` for wait/no-op actions.
pub async fn execute_action(
    ctx: &NodeContext,
    sync_handler: &SpoolSyncHandler,
    action: &NodeAction,
) -> Option<Result<HandlerOutcome, NetworkSyncError>> {
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
            return None;
        }
        NodeAction::WaitForSyncQuorum { current_weight } => {
            debug!(weight = current_weight, "Waiting for sync quorum");
            return None;
        }
        NodeAction::WaitForSettleQuorum { current_weight } => {
            debug!(weight = current_weight, "Waiting for settle quorum");
            return None;
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
            return None;
        }
        NodeAction::EpochBlocked { committee_next_size } => {
            debug!(
                size = committee_next_size,
                "Epoch blocked - waiting for more nodes"
            );
            return None;
        }
        NodeAction::NotInCommittee => {
            debug!("Not in committee, no action needed");
            return None;
        }
        NodeAction::UnknownPhase { phase } => {
            warn!(phase = phase, "Unknown epoch phase");
            return None;
        }
    };

    match &result {
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

    Some(result)
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
