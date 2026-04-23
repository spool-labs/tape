use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::ConnectInfo;
use clap::Parser;
use rpc_cache::cache::{CacheStore, Policy};
use rpc_cache::config::Config;
use rpc_cache::server::{AppState, router};
use rpc_cache::upstream::Upstream;
use tokio::net::TcpListener;
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

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let default_filter = if cli.verbose { "debug" } else { "info" };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .try_init();

    match run(&cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: &Cli) -> anyhow::Result<()> {
    let config = Config::from_file(&cli.config)
        .with_context(|| format!("loading {}", cli.config.display()))?;

    let policy = Policy::new(config.ttls.clone());
    let cache = CacheStore::new(config.max_entries);
    let upstream = Upstream::new(config.upstream.clone(), config.min_429_delay);
    let state = Arc::new(AppState {
        policy,
        cache,
        upstream,
        log_submits: config.log_submits,
        api_key: config.api_key.clone(),
    });

    let listener = TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!(
        listen = %config.listen,
        upstream = %config.upstream,
        min_429_delay_ms = config.min_429_delay.as_millis() as u64,
        max_entries = config.max_entries,
        "rpc-cache starting"
    );

    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("axum serve")?;

    // axum::serve returns (() not io::Result) so the above will stay
    // alive until the process is killed. Make the type checker happy.
    #[allow(unreachable_code)]
    Ok(())
}

// Keep the ConnectInfo import used (clippy-silence for future).
#[allow(dead_code)]
fn _use_connect_info(_: ConnectInfo<SocketAddr>) {}
