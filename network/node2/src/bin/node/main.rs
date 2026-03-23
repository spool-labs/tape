use std::process::ExitCode;

use clap::Parser;
use tape_node2::config::node::{NodeConfig, default_config_path};
use tape_node2::core::limits::check_fd_limit;
use tape_node2::runtime::{build_runtime, init_tracing, run_application};
use tracing::info;

#[derive(Parser)]
#[command(name = "tape-node2", about = "Tapedrive storage node runtime v2")]
struct Cli {
    #[arg(short, long, default_value_t = default_config_path().to_string_lossy().into_owned())]
    config: String,

    #[arg(long)]
    rpc_url: Option<String>,
}

fn main() -> ExitCode {
    check_fd_limit();

    let cli = Cli::parse();

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

    if let Some(host) = &config.network.host {
        info!(
            node_name = %config.node.name,
            listen = %config.http.listen,
            host = %host,
            port = config.network.port,
            rpc = %config.solana.rpc,
            "starting node"
        );
    } else {
        info!(
            node_name = %config.node.name,
            listen = %config.http.listen,
            rpc = %config.solana.rpc,
            "starting node"
        );
    }

    let runtime = match build_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("runtime build failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run_application(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("application failed: {error}");
            ExitCode::FAILURE
        }
    }
}
