mod keygen;

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tape_node::VERSION;
use tape_node::config::node::{NodeConfig, default_config_path};
use tape_node::core::limits::check_fd_limit;
use tape_node::runtime::{build_runtime, init_tracing, run_application};
use tracing::info;

#[derive(Parser)]
#[command(name = "tape-node", about = "Tapedrive storage node runtime v2")]
struct Cli {
    #[arg(short, long, default_value_t = default_config_path().to_string_lossy().into_owned(), global = true)]
    config: String,

    #[arg(long, global = true)]
    rpc_url: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a fresh per-node key bundle (identity, BLS, TLS) and a
    /// starter node.yaml. Used by operators and by tape-network.
    Keygen(keygen::KeygenArgs),
    /// Print the boot marker (Cargo version + git sha).
    Version,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Keygen(args)) => match keygen::run(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("keygen failed: {error}");
                ExitCode::FAILURE
            }
        },
        Some(Command::Version) => {
            println!("{VERSION}");
            ExitCode::SUCCESS
        }
        None => run_node(&cli.config, cli.rpc_url),
    }
}

fn run_node(config_path: &str, rpc_url: Option<String>) -> ExitCode {
    check_fd_limit();

    let mut config = match NodeConfig::from_yaml_file(config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Some(rpc_url) = rpc_url {
        config.solana.rpc = rpc_url;
    }

    if let Err(error) = init_tracing(&config.logging) {
        eprintln!("tracing initialization failed: {error}");
        return ExitCode::FAILURE;
    }

    // Query part carries the RPC api key; never log it.
    let rpc_display = config.solana.rpc.split('?').next().unwrap_or("");

    if let Some(host) = &config.network.host {
        info!(
            node_name = %config.node.name,
            listen = %config.http.listen,
            host = %host,
            port = config.network.port,
            rpc = rpc_display,
            boot_marker = VERSION,
            "starting node"
        );
    } else {
        info!(
            node_name = %config.node.name,
            listen = %config.http.listen,
            rpc = rpc_display,
            boot_marker = VERSION,
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
