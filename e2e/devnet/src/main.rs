use std::io;
use std::sync::Arc;
use std::thread;

use arc_swap::ArcSwap;
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, LeaveAlternateScreen};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod app;
mod log_layer;
mod poller;
mod simnet;
mod sparkline;
mod stake_fuzzer;
mod tui;
mod verify;

use crate::app::{Command, PollSnapshot};
use crate::log_layer::LogHistogram;

fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let mut stdout = io::stdout();
        let _ = disable_raw_mode();
        let _ = execute!(stdout, LeaveAlternateScreen);
        default_hook(panic_info);
    }));
}

fn main() {
    tape_node::core::limits::check_fd_limit();
    install_panic_hook();

    let histogram = LogHistogram::new();

    tracing_subscriber::registry()
        .with(histogram.clone())
        .with(tracing_subscriber::EnvFilter::new(
            "devnet=info,tape_e2e_devnet=info,tape_e2e_simnet=info,tape_node=info",
        ))
        .init();

    let snapshot: Arc<ArcSwap<PollSnapshot>> =
        Arc::new(ArcSwap::from_pointee(PollSnapshot::default()));

    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<Command>();

    let snap_clone = Arc::clone(&snapshot);
    let hist_clone = histogram.clone();

    let simnet_thread = thread::Builder::new()
        .name("simnet".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            simnet::run(cmd_rx, snap_clone, hist_clone);
        })
        .expect("spawn simnet thread");

    if let Err(e) = tui::run_tui(snapshot, cmd_tx) {
        eprintln!("tui error: {e:#}");
    }

    let _ = simnet_thread.join();
}
