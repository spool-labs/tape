//! Orchestrator - coordinates all node threads.
//!
//! Spawns and manages:
//! - Thread A: Live updates (block processing)
//! - Thread B: Network sync (epoch transitions + recovery FSM)
//!
//! Recovery is event-driven from NodeStatus transitions in the FSM loop,
//! not from a polling loop.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Instrument};

use crate::core::context::NodeContext;
use crate::features::api::ServerHandle;
use crate::features::chain as block;
use crate::features::epoch::{self as network_sync, FsmSignal};
use crate::features::recovery::{LiveUploadDeferral, TrackSyncHandler};

/// Signal channel capacity (small - only FSM wake-up signals).
const SIGNAL_CHANNEL_CAPACITY: usize = 32;

/// Error type for orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("block processor failed: {0}")]
    BlockProcessor(String),

    #[error("thread B (network sync) failed: {0}")]
    NetworkSync(String),

    #[error("server error: {0}")]
    Server(String),
}

/// Run the node orchestrator.
///
/// This spawns all worker threads and coordinates shutdown.
pub async fn run(
    ctx: Arc<NodeContext>,
    server_handle: ServerHandle,
) -> Result<(), OrchestratorError> {
    let node_id = ctx.control_plane.our_node_id();
    run_inner(ctx, server_handle)
        .instrument(tracing::info_span!("", node = node_id.as_u64()))
        .await
}

async fn run_inner(
    ctx: Arc<NodeContext>,
    server_handle: ServerHandle,
) -> Result<(), OrchestratorError> {
    info!("Orchestrator starting");

    // Create signal channel: block processor -> FSM loop
    let (signal_tx, signal_rx) = mpsc::channel::<FsmSignal>(SIGNAL_CHANNEL_CAPACITY);

    // Create shared recovery resources
    let recovery_config = &ctx.config.recovery;
    let track_sync = Arc::new(TrackSyncHandler::with_limits(
        recovery_config.max_concurrent_track_syncs,
        recovery_config.max_concurrent_slice_syncs,
    ));
    let deferral = Arc::new(LiveUploadDeferral::new(recovery_config.max_total_defer));

    let cancel = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();

    // Spawn deferral cleanup task (evicts expired entries)
    tasks.spawn({
        let deferral = Arc::clone(&deferral);
        async move {
            deferral.run_cleanup().await;
            Ok(())
        }
    });

    // Capture span for spawned tasks (they don't inherit parent span automatically)
    let span = tracing::Span::current();

    // Block processor: parses blocks, signals FSM when state changes
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let deferral = Arc::clone(&deferral);
        let cancel = cancel.clone();
        let span = span.clone();
        async move {
            block::run(ctx, signal_tx, deferral, cancel)
                .instrument(span)
                .await
                .map_err(|e| OrchestratorError::BlockProcessor(e.to_string()))
        }
    });

    // FSM loop: executes actions based on on-chain state + recovery FSM
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let cancel = cancel.clone();
        let track_sync = Arc::clone(&track_sync);
        let deferral = Arc::clone(&deferral);
        let span = span.clone();
        async move {
            network_sync::run(ctx, signal_rx, track_sync, deferral, cancel)
                .instrument(span)
                .await
                .map_err(|e| OrchestratorError::NetworkSync(e.to_string()))
        }
    });

    info!("All worker threads spawned");

    // Wait for shutdown signal or task failure
    let result = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal (Ctrl+C)");
            Ok(())
        }
        result = tasks.join_next() => {
            match result {
                Some(Ok(Ok(()))) => {
                    // A task completed successfully (shouldn't happen normally)
                    info!("A worker task completed unexpectedly");
                    Ok(())
                }
                Some(Ok(Err(e))) => {
                    error!(error = %e, "A worker task failed");
                    Err(e)
                }
                Some(Err(join_error)) => {
                    error!(error = %join_error, "A worker task panicked");
                    Err(OrchestratorError::BlockProcessor(join_error.to_string()))
                }
                None => {
                    // All tasks completed (shouldn't happen)
                    info!("All worker tasks completed");
                    Ok(())
                }
            }
        }
    };

    // Initiate graceful shutdown
    info!("Initiating graceful shutdown");
    cancel.cancel();
    deferral.cancel_token().cancel();

    // Cancel all in-progress recovery tasks
    track_sync.cancel_all().await;

    // Shutdown HTTP server
    server_handle.shutdown().await;

    // Wait for all tasks to complete with timeout
    let shutdown_timeout = std::time::Duration::from_secs(30);
    let shutdown_deadline = tokio::time::Instant::now() + shutdown_timeout;

    while let Ok(Some(task_result)) =
        tokio::time::timeout_at(shutdown_deadline, tasks.join_next()).await
    {
        match task_result {
            Ok(Ok(())) => {
                info!("Worker task shut down cleanly");
            }
            Ok(Err(e)) => {
                error!(error = %e, "Worker task error during shutdown");
            }
            Err(join_error) => {
                error!(error = %join_error, "Worker task panic during shutdown");
            }
        }
    }

    // Shutdown storage
    if let Err(e) = ctx.storage.shutdown().await {
        error!(error = %e, "Storage shutdown error");
    }

    info!("Orchestrator shutdown complete");
    result
}

#[cfg(test)]
mod tests {
    // Tests would require mocking various components
}
