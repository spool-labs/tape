use std::process::ExitCode;

use clap::Parser;
use tape_node2::config::{AppConfig, NodeConfig, default_config_path};
use tape_node2::core::limits::check_fd_limit;
use tape_node2::runtime::{build_runtime, init_tracing, run_application};

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

    if let Err(error) = init_tracing() {
        eprintln!("tracing initialization failed: {error}");
        return ExitCode::FAILURE;
    }

    let cli = Cli::parse();

    let mut node_config = match NodeConfig::from_yaml_file(&cli.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Some(rpc_url) = cli.rpc_url {
        node_config.rpc_url = rpc_url;
    }

    let config = match AppConfig::production(node_config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    let runtime = match build_runtime(&config.runtime) {
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
