//! Orchestrator - coordinates all node threads.
//!
//! Spawns and manages:
//! - Thread A: Live updates (block processing)
//! - Thread B: Network sync (epoch transitions)
//! - Thread C: Challenges (storage proofs)

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::context::NodeContext;
use crate::events::NodeEvent;
use crate::server::ServerHandle;
use crate::{challenges, live_updates, network_sync};

/// Event channel capacity.
const EVENT_CHANNEL_CAPACITY: usize = 10_000;

/// Error type for orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("thread A (live updates) failed: {0}")]
    LiveUpdates(String),

    #[error("thread B (network sync) failed: {0}")]
    NetworkSync(String),

    #[error("thread C (challenges) failed: {0}")]
    Challenges(String),

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
    info!("Orchestrator starting");

    // Create event channel for inter-thread communication
    let (event_tx, event_rx) = mpsc::channel::<NodeEvent>(EVENT_CHANNEL_CAPACITY);

    // Create cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    // Spawn worker threads
    let mut tasks = tokio::task::JoinSet::new();

    // Thread A: Live Updates
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let event_tx = event_tx.clone();
        let cancel = cancel.clone();
        async move {
            live_updates::run(ctx, event_tx, cancel)
                .await
                .map_err(|e| OrchestratorError::LiveUpdates(e.to_string()))
        }
    });

    // Thread B: Network Sync
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let cancel = cancel.clone();
        async move {
            network_sync::run(ctx, event_rx, cancel)
                .await
                .map_err(|e| OrchestratorError::NetworkSync(e.to_string()))
        }
    });

    // Thread C: Challenges (stub)
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let cancel = cancel.clone();
        async move {
            challenges::run(ctx, cancel)
                .await
                .map_err(|e| OrchestratorError::Challenges(e.to_string()))
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
                    Err(OrchestratorError::LiveUpdates(join_error.to_string()))
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
