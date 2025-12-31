//! Tapedrive storage node binary.
//!
//! This is the main entry point for running a storage node.
//! It initializes all components and coordinates graceful shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use tape_metrics::MetricsRegistry;
use tape_node::{EpochDriver, NodeConfig, NodeMetrics, Server, StorageService};

/// Tapedrive storage node.
#[derive(Parser, Debug)]
#[command(name = "tape-node")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the configuration file (YAML format).
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,

    /// Override bind address from config.
    #[arg(long, value_name = "ADDRESS")]
    bind: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with environment filter
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Load configuration
    info!(config_path = %cli.config.display(), "Loading configuration");
    let mut config = NodeConfig::from_yaml_file(&cli.config)
        .with_context(|| format!("Failed to load config from {}", cli.config.display()))?;

    // Apply CLI overrides
    if let Some(bind_addr) = cli.bind {
        info!(bind = %bind_addr, "Overriding bind address from CLI");
        config = config.with_bind_address(bind_addr);
    }

    // Run the node
    run_node(config).await
}

/// Run the storage node with the given configuration.
async fn run_node(config: NodeConfig) -> Result<()> {
    info!(
        name = %config.name,
        bind = %config.bind_address,
        storage_path = %config.storage_path.display(),
        "Starting Tapedrive storage node"
    );

    // Initialize metrics registry
    let registry = MetricsRegistry::init();
    let metrics = Arc::new(NodeMetrics::new(registry.prometheus_registry()));

    // Initialize storage service
    let storage = Arc::new(
        StorageService::open(&config.storage_path)
            .context("Failed to create storage service")?
            .with_metrics(Arc::clone(&metrics)),
    );

    // Create shutdown token
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    // Set up signal handlers for graceful shutdown
    tokio::spawn(async move {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("Received Ctrl+C, initiating shutdown");
            }
            _ = terminate => {
                info!("Received SIGTERM, initiating shutdown");
            }
        }

        shutdown_clone.cancel();
    });

    // Initialize epoch driver
    let epoch_driver = EpochDriver::new(0, config.name.clone());

    // Spawn epoch driver background task
    let epoch_shutdown = shutdown.clone();
    let epoch_handle = tokio::spawn(async move {
        // Placeholder callbacks for epoch driver
        // In production, these would interact with Solana RPC and storage
        let fetch_epoch = || {
            Box::pin(async {
                // Placeholder: return current epoch from Solana
                Ok(0u64)
            }) as futures::future::BoxFuture<'static, Result<u64, tape_node::epoch_driver::EpochError>>
        };

        let compute_changes = |_from: u64, _to: u64| {
            // Placeholder: compute spool changes between epochs
            Ok((vec![], vec![]))
        };

        let on_sync_done = |_epoch: u64| {
            Box::pin(async {
                // Placeholder: signal sync completion
                Ok(())
            }) as futures::future::BoxFuture<'static, Result<(), tape_node::epoch_driver::EpochError>>
        };

        epoch_driver
            .run(epoch_shutdown, fetch_epoch, compute_changes, on_sync_done)
            .await;

        info!("Epoch driver stopped");
    });

    // Create and run the server
    let server = Server::new(config, Arc::clone(&metrics), Arc::clone(&storage));

    // Run server with graceful shutdown
    let server_shutdown = shutdown.clone();
    let server_handle = tokio::spawn(async move {
        tokio::select! {
            result = server.run() => {
                if let Err(e) = result {
                    error!(error = %e, "Server error");
                }
            }
            _ = server_shutdown.cancelled() => {
                info!("Server shutdown requested");
            }
        }
    });

    info!("Node is running. Press Ctrl+C to shutdown.");

    // Wait for shutdown signal
    shutdown.cancelled().await;

    info!("Shutting down...");

    // Wait for tasks to complete (with timeout)
    let shutdown_timeout = tokio::time::Duration::from_secs(30);

    tokio::select! {
        _ = async {
            let _ = tokio::join!(epoch_handle, server_handle);
        } => {
            info!("All tasks completed");
        }
        _ = tokio::time::sleep(shutdown_timeout) => {
            error!("Shutdown timeout exceeded, forcing exit");
        }
    }

    // Shutdown storage
    storage
        .shutdown()
        .await
        .context("Failed to shutdown storage")?;

    info!("Node shutdown complete");

    Ok(())
}
