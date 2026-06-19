use std::process::ExitCode;

use clap::Parser;
use tape_node::config::node::{default_config_path, NodeConfig};
use tape_node::core::limits::check_fd_limit;
use tape_node::runtime::{build_runtime, init_tracing};
use tracing::info;

#[derive(Parser)]
#[command(name = "tape-gateway", about = "Tapedrive read gateway runtime")]
struct Cli {
    #[arg(short, long, default_value_t = default_config_path().to_string_lossy().into_owned())]
    config: String,

    #[arg(long)]
    rpc_url: Option<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    check_fd_limit();

    let mut config = match NodeConfig::from_yaml_file(&cli.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Some(rpc_url) = cli.rpc_url {
        config.solana.rpc = rpc_url;
    }

    if let Err(error) = init_tracing(&config.logging) {
        eprintln!("tracing initialization failed: {error}");
        return ExitCode::FAILURE;
    }

    info!(
        node_name = %config.node.name,
        rpc = %config.solana.rpc,
        version = env!("CARGO_PKG_VERSION"),
        "starting gateway"
    );

    let runtime = match build_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("runtime build failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(tape_gateway::runtime::run_application(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("application failed: {error}");
            ExitCode::FAILURE
        }
    }
}
