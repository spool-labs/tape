//! Tapedrive storage node binary.
//!
//! This is the main entry point for running a storage node.
//! It initializes all components via NodeContext and runs the orchestrator.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

use tape_node::{NodeConfig, NodeContext, Server};

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
        storage_path = %config.storage_path,
        "Starting Tapedrive storage node"
    );

    // Build context from config (handles all initialization)
    let ctx = NodeContext::from_config(config.clone())
        .await
        .context("Failed to initialize node context")?;

    // Validate node state
    let node = ctx.control_plane.get_node();
    let epoch = ctx.control_plane.get_epoch();

    info!(
        node_id = node.id.as_u64(),
        epoch = epoch.id.as_u64(),
        spools = ctx.our_spools().len(),
        in_committee = ctx.is_in_committee(),
        "Node initialized"
    );

    if node.latest_epoch < epoch.id {
        tracing::warn!(
            node_epoch = node.latest_epoch.as_u64(),
            current_epoch = epoch.id.as_u64(),
            "Node is behind current epoch, will sync"
        );
    }

    // Start HTTP server
    let server = Server::new(
        config,
        ctx.metrics.clone(),
        ctx.storage.clone(),
    );
    let server_handle = server
        .start()
        .await
        .context("Failed to start HTTP server")?;

    // Run orchestrator (blocks until shutdown)
    tape_node::orchestrator::run(ctx, server_handle)
        .await
        .context("Orchestrator error")?;

    info!("Node shutdown complete");

    Ok(())
}
