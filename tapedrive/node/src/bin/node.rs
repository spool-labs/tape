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

use solana_sdk::signature::read_keypair_file;
use tape_client::TapeClient;
use tape_metrics::MetricsRegistry;
use tape_node::{EpochManager, NodeConfig, NodeMetrics, Server, StorageService};
use tape_rpc::RpcConfig;

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

    // Load authority keypair for Solana transactions
    let authority_keypair = Arc::new(
        read_keypair_file(&config.protocol_keypair)
            .map_err(|e| anyhow::anyhow!("Failed to load keypair from {}: {}", config.protocol_keypair.display(), e))?,
    );

    // Initialize tape client for chain interactions
    let rpc_config = RpcConfig {
        endpoints: vec![config.solana_rpc_url.clone()],
        ..Default::default()
    };
    let client = Arc::new(
        TapeClient::new(rpc_config)
            .context("Failed to create tape client")?,
    );

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

    // Initialize epoch manager
    let epoch_manager = Arc::new(EpochManager::new(
        Arc::clone(&client),
        Arc::clone(&authority_keypair),
        Arc::clone(&storage),
    ));

    // Spawn epoch manager background task
    let epoch_shutdown = shutdown.clone();
    let epoch_manager_handle = Arc::clone(&epoch_manager);
    let epoch_handle = tokio::spawn(async move {
        epoch_manager_handle.run(epoch_shutdown).await;
        info!("Epoch manager stopped");
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
