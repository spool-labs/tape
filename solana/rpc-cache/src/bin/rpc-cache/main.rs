use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;
use rpc_cache::config::Config;
use rpc_cache::runtime::run_application;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rpc-cache", about = "Caching proxy for Solana RPC", version)]
struct Cli {
    /// Config file path. See docs/rpc-cache.md for schema.
    #[arg(short = 'c', long = "config", default_value = "rpc-cache.yaml")]
    config: PathBuf,

    /// Verbose logs (sets RUST_LOG=debug if unset).
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let default_filter = if cli.verbose { "debug" } else { "info" };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .try_init();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("runtime build failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = Config::from_file(&cli.config)
        .with_context(|| format!("loading {}", cli.config.display()))?;

    tracing::info!(
        listen = %config.listen,
        upstream = %config.upstream,
        slot_store_max_bytes = config.slot_store_max_bytes,
        max_entries = config.max_entries,
        "rpc-cache starting"
    );

    run_application(config).await
}
