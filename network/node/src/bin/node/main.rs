use anyhow::{Context, Result};
use clap::Parser;
use rpc_client::RpcClient;
use rpc_solana::RpcConfig;
use tape_node::core::config::NodeConfig;
use tape_node::core::NodeContextBuilder;
use tape_node::pipeline::spawn_runtime;
use tape_store::TapeStore;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "tape-node", about = "Tapedrive storage node")]
struct Cli {
    /// Path to node configuration YAML file
    #[arg(short, long, default_value = "~/.tape/config.yaml")]
    config: String,

    /// RPC endpoint URL (overrides config)
    #[arg(long)]
    rpc_url: Option<String>,
}

fn expand_path(path: &str) -> std::path::PathBuf {
    shellexpand::tilde(path).to_string().into()
}

#[tokio::main]
async fn main() -> Result<()> {
    // Init tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Load config
    let config_path = expand_path(&cli.config);
    let config = NodeConfig::from_yaml_file(&config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    // Load Solana keypair
    let keypair_path = &config.node_keypair;
    let keypair = solana_sdk::signature::read_keypair_file(keypair_path)
        .map_err(|e| anyhow::anyhow!("failed to read keypair from {keypair_path}: {e}"))?;
    tracing::info!(name = %config.name, "starting node");

    // Open RocksDB
    let db_path = expand_path(&config.storage_path);
    tracing::info!(path = %db_path.display(), "opening database");
    let store = TapeStore::open_primary(&db_path)
        .with_context(|| format!("failed to open database at {}", db_path.display()))?;

    // Initialize store metrics (registers tape_store_* families with prometheus)
    store::init_metrics();

    // Create RPC client
    let rpc_url = cli
        .rpc_url
        .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string());
    let rpc_config = RpcConfig {
        endpoints: vec![rpc_url.clone()],
        ..RpcConfig::default()
    };
    let rpc = RpcClient::new(rpc_config)
        .with_context(|| format!("failed to create RPC client for {rpc_url}"))?;

    tracing::info!(%rpc_url, "connected to RPC");

    // Build context (includes startup node-id resolution from on-chain node account)
    let context = NodeContextBuilder::new(
        config,
        keypair,
        store,
        rpc,
    )
    .build()
    .await
    .context("build node context")?;
    let node_id = context.node_id();

    // Cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    // Signal handler
    let shutdown_cancel = cancel.clone();
    let shutdown_span =
        tracing::info_span!("tape_node_runtime", component = "shutdown", node_id = node_id);
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

        #[cfg(unix)]
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }

        #[cfg(not(unix))]
        ctrl_c.await.ok();

        tracing::info!("shutdown signal received");
        shutdown_cancel.cancel();
    }.instrument(shutdown_span));

    // Spawn the runtime
    let handles = spawn_runtime(context, cancel).await;

    // Await all handles
    let _ = tokio::try_join!(
        handles.ingestor,
        handles.fsm,
        handles.reconciler,
        handles.supervisor,
        handles.http,
    );

    tracing::info!(node_id = node_id, "node stopped");
    Ok(())
}
