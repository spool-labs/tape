use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::future::IntoFuture;

use arc_swap::ArcSwap;
use clap::Parser;
use tape_e2e_prodnet::api::{self, AppState};
use tape_e2e_prodnet::config::ProdnetConfig;
use tape_e2e_prodnet::observer::Observer;
use tape_e2e_prodnet::orchestrator::Orchestrator;
use tape_e2e_prodnet::poller;
use tape_e2e_prodnet::tui::{self, Command as TuiCommand};
use tape_e2e_prodnet::view::ProdnetView;
use tokio::sync::Mutex;
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

    #[arg(long, default_value_t = 9000)]
    api_port: u16,

    #[arg(long, default_value_t = 0)]
    init_nodes: usize,

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
        .with_writer(io::sink)
        .compact()
        .init();

    let cli = Cli::parse();
    let api_port = cli.api_port;

    let config = ProdnetConfig {
        rpc_url: cli.rpc_url.clone(),
        node_binary: cli.node_binary,
        data_dir: cli.data_dir,
        base_port: cli.base_port,
        sol_airdrop: cli.sol_airdrop,
        stake_amount: cli.stake_amount,
    };

    let observer = match Observer::new(&cli.rpc_url) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("observer init failed: {e:#}");
            return ExitCode::FAILURE;
        }
    };

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

    if cli.init_nodes > 0 {
        if let Err(e) = orch.add_nodes(cli.init_nodes).await {
            eprintln!("add nodes failed: {e:#}");
            let _ = orch.shutdown().await;
            return ExitCode::FAILURE;
        }
    }

    let snapshot = Arc::new(ArcSwap::from_pointee(ProdnetView::default()));
    let observer = Arc::new(observer);
    let orchestrator = Arc::new(Mutex::new(orch));
    let shutdown = Arc::new(AtomicBool::new(false));

    let state = AppState {
        orchestrator: orchestrator.clone(),
        snapshot: snapshot.clone(),
    };

    let app = api::router(state.clone());
    let addr = SocketAddr::from(([0, 0, 0, 0], api_port));

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind failed: {e:#}");
            return ExitCode::FAILURE;
        }
    };

    let poller_task = tokio::spawn(poller::run(
        observer,
        orchestrator.clone(),
        snapshot.clone(),
    ));
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<TuiCommand>();
    let tui_shutdown = shutdown.clone();
    let tui_thread = thread::Builder::new()
        .name("prodnet-tui".into())
        .spawn(move || {
            if let Err(error) = tui::run_tui(snapshot, cmd_tx, tui_shutdown) {
                eprintln!("tui error: {error:#}");
            }
        });

    let tui_thread = match tui_thread {
        Ok(handle) => handle,
        Err(error) => {
            eprintln!("spawn TUI failed: {error:#}");
            return ExitCode::FAILURE;
        }
    };

    let mut server = Box::pin(axum::serve(listener, app).into_future());
    let mut fatal_error: Option<String> = None;
    let exit_code = loop {
        tokio::select! {
            result = &mut server => {
                if let Err(error) = result {
                    fatal_error = Some(format!("server error: {error:#}"));
                    break ExitCode::FAILURE;
                }
                break ExitCode::SUCCESS;
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    TuiCommand::AddNode => {
                        let mut orch = orchestrator.lock().await;
                        if let Err(error) = orch.add_node().await {
                            tracing::error!(error = %error, "add node failed");
                        }
                    }
                    TuiCommand::RemoveNode => {
                        let mut orch = orchestrator.lock().await;
                        match orch.remove_last_node().await {
                            Ok(Some(_)) | Ok(None) => {}
                            Err(error) => tracing::error!(error = %error, "remove node failed"),
                        }
                    }
                    TuiCommand::Quit => break ExitCode::SUCCESS,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break ExitCode::SUCCESS;
            }
        }
    };

    shutdown.store(true, Ordering::Relaxed);
    poller_task.abort();
    let _ = tui_thread.join();

    if let Some(error) = fatal_error {
        eprintln!("{error}");
    }

    let mut orch = state.orchestrator.lock().await;
    if let Err(e) = orch.shutdown().await {
        eprintln!("shutdown failed: {e:#}");
        return ExitCode::FAILURE;
    }

    exit_code
}
