use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tape_e2e_prodnet::config::ProdnetConfig;
use tape_e2e_prodnet::orchestrator::Orchestrator;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "prodnet", about = "Tapedrive production-like testnet orchestrator")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    #[arg(long, default_value = "target/debug/tape-node2")]
    node_binary: PathBuf,

    #[arg(long, default_value = "target/prodnet")]
    data_dir: PathBuf,

    #[arg(long, default_value_t = 4000)]
    base_port: u16,

    #[arg(long, default_value_t = 3)]
    nodes: usize,

    #[arg(long, default_value_t = 50_000_000_000)]
    sol_airdrop: u64,

    #[arg(long, default_value_t = 1_000_000)]
    stake_amount: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .compact()
        .init();

    let cli = Cli::parse();

    let config = ProdnetConfig {
        rpc_url: cli.rpc_url,
        node_binary: cli.node_binary,
        data_dir: cli.data_dir,
        base_port: cli.base_port,
        node_count: cli.nodes,
        sol_airdrop: cli.sol_airdrop,
        stake_amount: cli.stake_amount,
    };

    let node_count = config.node_count;
    let mut orch = match Orchestrator::new(config) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("orchestrator init failed: {e:#}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = orch.init().await {
        eprintln!("chain init failed: {e:#}");
        return ExitCode::FAILURE;
    }

    if let Err(e) = orch.add_nodes(node_count).await {
        eprintln!("add nodes failed: {e:#}");
        let _ = orch.shutdown().await;
        return ExitCode::FAILURE;
    }

    tracing::info!("prodnet running, press ctrl-c to stop");
    tokio::signal::ctrl_c().await.ok();

    if let Err(e) = orch.shutdown().await {
        eprintln!("shutdown failed: {e:#}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
