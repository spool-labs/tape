use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use peer_http::HttpApi;
use rpc_client::RpcClient;
use rpc_solana::RpcConfig;
use tape_node::core::{
    NodeConfig, NodeContextBuilder, default_config_path, load_bls_keypair_from_config,
    load_node_keypair, open_primary_store,
};
use tape_node::runtime::spawn_runtime;
use peer_manager::PeerManager;
use tape_protocol::{ProtocolState, new_shared_state};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "tape-node", about = "Tapedrive storage node")]
struct Cli {
    /// Path to node configuration YAML file
    #[arg(short, long, default_value_t = default_config_path().to_string_lossy().into_owned())]
    config: String,

    /// RPC endpoint URL (overrides config)
    #[arg(long)]
    rpc_url: Option<String>,
}

async fn watch_handle(
    name: &'static str,
    handle: tokio::task::JoinHandle<()>,
) -> (&'static str, std::result::Result<(), tokio::task::JoinError>) {
    (name, handle.await)
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
    let config = NodeConfig::from_yaml_file(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config))?;

    // Load Solana keypair
    let keypair = load_node_keypair(&config).context("load node keypair")?;
    let bls_keypair = load_bls_keypair_from_config(&config).context("load BLS keypair")?;
    tracing::info!(name = %config.name, "starting node");

    // Open RocksDB
    tracing::info!(path = %config.storage_path, "opening database");
    let store = open_primary_store(&config).context("open primary store")?;

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

    // Build peer manager and API
    let shared_state = new_shared_state(ProtocolState::default());
    let peer_manager = Arc::new(PeerManager::new(shared_state.clone()));
    let api = Arc::new(HttpApi::new(Default::default(), peer_manager.clone()));

    // Build context (includes startup node-id resolution from on-chain node account)
    let context = NodeContextBuilder::new(
        config,
        keypair,
        bls_keypair,
        store,
        rpc,
        shared_state,
        peer_manager,
        api,
    )
    .build()
    .await
    .context("build node context")?;
    let node_id = context.node_id();

    // Cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    // Signal handler
    let shutdown_cancel = cancel.clone();
    let shutdown_span = tracing::info_span!("", node_id = node_id.0);
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
    let handles = spawn_runtime(context, cancel.clone()).await;

    // Await all runtime handles. If any task fails, trigger cancellation and
    // continue awaiting all remaining handles so none are dropped/detached.
    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(watch_handle("ingestor", handles.ingestor));
    join_set.spawn(watch_handle("fsm", handles.fsm));
    join_set.spawn(watch_handle("scheduler", handles.scheduler));
    join_set.spawn(watch_handle("task_runner", handles.task_runner));
    join_set.spawn(watch_handle("http", handles.http));

    let mut first_failure = None;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((name, Ok(()))) => {
                tracing::info!(task = name, "runtime task exited");
            }
            Ok((name, Err(error))) => {
                tracing::error!(task = name, %error, "runtime task failed");
                if first_failure.is_none() {
                    first_failure = Some(format!("{name}: {error}"));
                    cancel.cancel();
                }
            }
            Err(error) => {
                tracing::error!(%error, "task watcher failed");
                if first_failure.is_none() {
                    first_failure = Some(format!("task watcher failed: {error}"));
                    cancel.cancel();
                }
            }
        }
    }

    if let Some(error) = first_failure {
        return Err(anyhow::anyhow!("runtime shutdown after failure: {error}"));
    }

    tracing::info!(node_id = node_id.0, "node stopped");
    Ok(())
}
