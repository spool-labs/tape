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
use tape_e2e_localnet::action::ActionLog;
use tape_e2e_localnet::api::{self, AppState};
use tape_e2e_localnet::config::LocalnetConfig;
use tape_e2e_localnet::observer::Observer;
use tape_e2e_localnet::orchestrator::Orchestrator;
use tape_e2e_localnet::poller;
use tape_e2e_localnet::tui::{self, Command as TuiCommand};
use tape_e2e_localnet::upload::UploadManager;
use tape_e2e_localnet::view::LocalnetView;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;
use tape_node::core::limits::check_fd_limit;

#[derive(Parser)]
#[command(name = "localnet", about = "Tapedrive local-validator e2e orchestrator")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    #[arg(long, default_value = "target/debug/tape-node")]
    node_binary: PathBuf,

    #[arg(long, default_value = "target/localnet")]
    data_dir: PathBuf,

    #[arg(long, default_value_t = 4000)]
    base_port: u16,

    #[arg(long, default_value_t = 9000)]
    api_port: u16,

    #[arg(long, default_value_t = 0)]
    init_nodes: usize,

    #[arg(long, default_value_t = 50_000_000_000)]
    sol_airdrop: u64,

    #[arg(long, default_value_t = 100_000)]
    stake_amount: u64,

    #[arg(long, default_value_t = 1)]
    spool_groups: u64,
}

fn main() -> ExitCode {
    check_fd_limit();

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
    {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("build tokio runtime failed: {error:#}");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async_main())
}

async fn async_main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(io::sink)
        .compact()
        .init();

    let cli = Cli::parse();
    let api_port = cli.api_port;
    let admin_keypair_path = cli.data_dir.join("admin.json");

    let config = LocalnetConfig {
        rpc_url: cli.rpc_url.clone(),
        node_binary: cli.node_binary,
        data_dir: cli.data_dir,
        base_port: cli.base_port,
        sol_airdrop: cli.sol_airdrop,
        stake_amount: cli.stake_amount,
        spool_groups: cli.spool_groups,
        ..LocalnetConfig::default()
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

    let snapshot = Arc::new(ArcSwap::from_pointee(LocalnetView::default()));
    let action = Arc::new(ArcSwap::from_pointee(ActionLog::default()));
    let observer = Arc::new(observer);
    let orchestrator = Arc::new(Mutex::new(orch));
    let upload_manager = Arc::new(UploadManager::new(
        cli.rpc_url.clone(),
        admin_keypair_path,
    ));
    let shutdown = Arc::new(AtomicBool::new(false));

    let state = AppState {
        orchestrator: orchestrator.clone(),
        upload_manager: upload_manager.clone(),
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
        upload_manager.clone(),
        snapshot.clone(),
    ));
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<TuiCommand>();
    let tui_shutdown = shutdown.clone();
    let tui_action = action.clone();
    let tui_thread = thread::Builder::new()
        .name("localnet-tui".into())
        .spawn(move || {
            if let Err(error) = tui::run_tui(snapshot, tui_action, cmd_tx, tui_shutdown) {
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
    // The operator crank for stalled nodes: their join and pool advance only
    // have to land once per epoch, whenever each window is open.
    let mut crank = tokio::time::interval(std::time::Duration::from_secs(10));
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
                // Every arm reports, because these keys act on the last node,
                // which is the row a short terminal drops first.
                let log = match cmd {
                    TuiCommand::AddNode => {
                        let mut orch = orchestrator.lock().await;
                        match orch.add_node().await {
                            Ok(id) => ActionLog::done(format!("added node {id}"), id),
                            Err(error) => ActionLog::failed(format!("add failed: {error:#}")),
                        }
                    }
                    TuiCommand::RemoveNode => {
                        let mut orch = orchestrator.lock().await;
                        match orch.remove_last_node().await {
                            Ok(Some(id)) => {
                                ActionLog::done(format!("removed node {id}, its seat is gone"), id)
                            }
                            Ok(None) => ActionLog::idle("remove: no node is running".into()),
                            Err(error) => ActionLog::failed(format!("remove failed: {error}")),
                        }
                    }
                    TuiCommand::StallNode => {
                        let mut orch = orchestrator.lock().await;
                        match orch.stall_last_node().await {
                            Ok(Some(id)) => {
                                ActionLog::done(format!("stalled node {id}, crank holds its seat"), id)
                            }
                            Ok(None) => ActionLog::idle("stall: no node is running".into()),
                            Err(error) => ActionLog::failed(format!("stall failed: {error:#}")),
                        }
                    }
                    TuiCommand::FlapNode => {
                        let mut orch = orchestrator.lock().await;
                        match orch.toggle_flap_last_node() {
                            Ok(Some((id, true))) => {
                                ActionLog::done(format!("flapping node {id}, pauses on the crank"), id)
                            }
                            Ok(Some((id, false))) => {
                                ActionLog::done(format!("flap off for node {id}"), id)
                            }
                            Ok(None) => ActionLog::idle("flap: no node is running".into()),
                            Err(error) => ActionLog::failed(format!("flap failed: {error:#}")),
                        }
                    }
                    TuiCommand::LoseSlices => {
                        let mut orch = orchestrator.lock().await;
                        match orch.lose_slices_last_node().await {
                            Ok(Some((id, dropped))) => ActionLog::done(
                                format!("node {id} silently lost {dropped} slices, restarted"),
                                id,
                            ),
                            Ok(None) => ActionLog::idle("lose slices: no node is running".into()),
                            Err(error) => ActionLog::failed(format!("slice loss failed: {error:#}")),
                        }
                    }
                    TuiCommand::UploadBlob => match upload_manager.start_random_upload() {
                        Ok(upload) => ActionLog::idle(format!(
                            "uploading {} bytes to {}",
                            upload.size_bytes, upload.tape_address
                        )),
                        Err(error) => ActionLog::failed(format!("upload failed: {error:#}")),
                    },
                    TuiCommand::DeleteUpload => match upload_manager.delete_latest() {
                        Ok(Some(tape)) => ActionLog::idle(format!("deleting the track on {tape}")),
                        Ok(None) => ActionLog::idle("delete: no completed upload to delete".into()),
                        Err(error) => ActionLog::failed(format!("delete failed: {error:#}")),
                    },
                    TuiCommand::Quit => break ExitCode::SUCCESS,
                };
                action.store(Arc::new(log));
            }
            _ = crank.tick() => {
                let (chain, targets) = {
                    let mut orch = orchestrator.lock().await;
                    orch.crank_flapping();
                    (orch.chain_handle(), orch.stalled_targets())
                };
                for (id, pubkey, keypair) in targets {
                    if let Err(error) = chain.advance_pool(pubkey).await {
                        tracing::debug!(id, error = %error, "stall crank: advance pool");
                    }
                    if let Err(error) = chain.join_committee(&keypair).await {
                        tracing::debug!(id, error = %error, "stall crank: join committee");
                    }
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
