//! Orchestrator - coordinates all node threads.
//!
//! Spawns and manages:
//! - Thread A: Live updates (block processing)
//! - Thread B: Network sync (epoch transitions)
//! - Thread C: Challenges (storage proofs)
//! - Thread D: Recovery (erasure coding recovery)

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::core::context::NodeContext;
use crate::features::api::ServerHandle;
use crate::features::block_processing as block;
use crate::features::challenges;
use crate::features::epoch_sync::{self as network_sync, FsmSignal};
use crate::features::recovery;

/// Signal channel capacity (small - only FSM wake-up signals).
const SIGNAL_CHANNEL_CAPACITY: usize = 32;

/// Error type for orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("block processor failed: {0}")]
    BlockProcessor(String),

    #[error("thread B (network sync) failed: {0}")]
    NetworkSync(String),

    #[error("thread C (challenges) failed: {0}")]
    Challenges(String),

    #[error("thread D (recovery) failed: {0}")]
    Recovery(String),

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

    // Create signal channel: block processor -> FSM loop
    let (signal_tx, signal_rx) = mpsc::channel::<FsmSignal>(SIGNAL_CHANNEL_CAPACITY);

    let cancel = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();

    // Block processor: parses blocks, signals FSM when state changes
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let cancel = cancel.clone();
        async move {
            block::run(ctx, signal_tx, cancel)
                .await
                .map_err(|e| OrchestratorError::BlockProcessor(e.to_string()))
        }
    });

    // FSM loop: executes actions based on on-chain state
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let cancel = cancel.clone();
        async move {
            network_sync::run(ctx, signal_rx, cancel)
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

    // Thread D: Recovery
    tasks.spawn({
        let ctx = Arc::clone(&ctx);
        let cancel = cancel.clone();
        async move {
            recovery::run(ctx, cancel)
                .await
                .map_err(|e| OrchestratorError::Recovery(e.to_string()))
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
