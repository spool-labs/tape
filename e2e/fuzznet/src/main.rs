use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossterm::{execute, terminal::{disable_raw_mode, LeaveAlternateScreen}};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod log_histogram;
mod runner;
mod stats;
mod tui;

use crate::log_histogram::{LogHistogram, RingBuffer};
use crate::runner::{run_fuzz, FuzzConfig};
use crate::stats::{FuzzPhase, FuzzStats};

const NODE_COUNT: usize = 25;
const TARGET_EPOCHS: u64 = 100;
const MIN_ALIVE: usize = 20;
const POLL_INTERVAL_SECS: u64 = 2;
const UPLOAD_INTERVAL_SECS: u64 = 5;
const DOWNLOAD_INTERVAL_SECS: u64 = 7;
const CHURN_INTERVAL_SECS: u64 = 25;
const BLOB_SIZE_MIN: usize = 10_240;
const BLOB_SIZE_MAX: usize = 1_024_000;
const RING_BUFFER_SIZE: usize = 2000;
const EPOCH_TIMEOUT_SECS: u64 = 120;
const TUI_TICK_MS: u64 = 250;
const FUZZ_SEED: u64 = 42;

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
    install_panic_hook();

    let code = thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime")
                .block_on(run())
                .unwrap_or(1)
        })
        .expect("spawn fuzznet thread")
        .join()
        .expect("fuzznet thread panicked");

    std::process::exit(code);
}

async fn run() -> Result<i32> {
    let histogram = LogHistogram::new();
    let ring = RingBuffer::new(RING_BUFFER_SIZE);

    tracing_subscriber::registry()
        .with(histogram.clone())
        .with(ring.clone())
        .with(tracing_subscriber::EnvFilter::new(
            "tape_e2e_simnet=info,tape_node=info",
        ))
        .init();

    let stats = Arc::new(Mutex::new(FuzzStats::new(
        FUZZ_SEED,
        TARGET_EPOCHS,
        NODE_COUNT,
    )));

    let abort = Arc::new(AtomicBool::new(false));
    let tui_handle = {
        let tui_stats = Arc::clone(&stats);
        let tui_abort = Arc::clone(&abort);
        tokio::spawn(async move { tui::run_tui(tui_stats, tui_abort, TUI_TICK_MS).await })
    };

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .build()?;

    let config = FuzzConfig {
        node_count: NODE_COUNT,
        target_epochs: TARGET_EPOCHS,
        min_alive: MIN_ALIVE,
        poll_interval: Duration::from_secs(POLL_INTERVAL_SECS),
        upload_interval: Duration::from_secs(UPLOAD_INTERVAL_SECS),
        download_interval: Duration::from_secs(DOWNLOAD_INTERVAL_SECS),
        churn_interval: Duration::from_secs(CHURN_INTERVAL_SECS),
        blob_size_min: BLOB_SIZE_MIN,
        blob_size_max: BLOB_SIZE_MAX,
        epoch_timeout: Duration::from_secs(EPOCH_TIMEOUT_SECS),
    };

    let fuzz_error = match run_fuzz(
        &mut harness,
        FUZZ_SEED,
        &stats,
        &histogram,
        &abort,
        config,
    )
    .await
    {
        Ok(_) => None,
        Err(error) => {
            let mut state = stats.lock().expect("stats lock poisoned");
            state.phase = FuzzPhase::Done { passed: false };
            harness.stop_all().await.ok();
            Some(format!("{error:#}"))
        }
    };

    abort.store(true, Ordering::Release);
    let _ = tui_handle.await;

    fs::create_dir_all("target/sim-e2e")?;
    if let Err(error) = ring.dump_to_file(Path::new("target/sim-e2e/fuzznet-postmortem.log")) {
        eprintln!("failed to write postmortem log: {error:#}");
    }

    if let Some(ref error) = fuzz_error {
        eprintln!("fuzznet error: {error}");
        eprintln!("postmortem log: target/sim-e2e/fuzznet-postmortem.log");
    }

    let final_stats = stats.lock().expect("stats lock poisoned").clone();
    print_summary(&final_stats);

    let passed = matches!(final_stats.phase, FuzzPhase::Done { passed: true });
    Ok(if passed { 0 } else { 1 })
}

fn print_summary(state: &FuzzStats) {
    let uploads_total = state.upload_registry.len();
    let uploaded_bytes = state.uploaded_bytes_total() as f64 / (1024.0 * 1024.0);
    let (download_checked, download_passed) = state.downloaded_count();
    let durations = state.epoch_durations_secs();

    let mut min = 0.0f64;
    let mut max = 0.0f64;
    let mut mean = 0.0f64;
    let mut p50 = 0.0f64;
    let mut p95 = 0.0f64;

    if !durations.is_empty() {
        let mut sorted = durations.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        min = sorted[0];
        max = sorted[sorted.len() - 1];
        mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
        p50 = percentile(&sorted, 50);
        p95 = percentile(&sorted, 95);
    }

    let mut spool_active = 0.0;
    let mut spool_sync = 0.0;
    let mut spool_recover = 0.0;
    let mut spool_locked = 0.0;
    if !state.epochs.is_empty() {
        let total = state.epochs.len() as f64;
        spool_active = state
            .epochs
            .iter()
            .map(|epoch| epoch.spools_active as f64)
            .sum::<f64>()
            / total;
        spool_sync = state
            .epochs
            .iter()
            .map(|epoch| epoch.spools_sync as f64)
            .sum::<f64>()
            / total;
        spool_recover = state
            .epochs
            .iter()
            .map(|epoch| epoch.spools_recover as f64)
            .sum::<f64>()
            / total;
        spool_locked = state
            .epochs
            .iter()
            .map(|epoch| epoch.spools_locked as f64)
            .sum::<f64>()
            / total;
    }

    println!("Fuzz run complete");
    println!("  seed:        {}", state.seed);
    println!("  wall time:   {}", format_duration(state.start_time.elapsed()));
    println!("  epochs:      {}", state.epochs.len());
    println!("  uploads:     {} ({uploaded_bytes:.1} MB)", uploads_total);
    println!(
        "  downloads:   {} / {} passed",
        download_passed, download_checked
    );

    println!();
    println!("Epoch duration (seconds):");
    println!(
        "  min={:.1}  max={:.1}  mean={:.1}  p50={:.1}  p95={:.1}",
        min, max, mean, p50, p95
    );

    println!();
    println!("Spool status (mean per epoch):");
    println!(
        "  active={:.0}  sync={:.0}  recover={:.0}  locked={:.0}",
        spool_active, spool_sync, spool_recover, spool_locked
    );

    let mut totals: HashMap<(String, String), u64> = HashMap::new();
    let mut warnings = 0usize;
    for epoch in &state.epochs {
        warnings += epoch.warnings.len();
        for ((level, target), count) in &epoch.log_counts {
            *totals
                .entry((level.to_string(), target.clone()))
                .or_default() += *count;
        }
    }

    let mut top: Vec<_> = totals.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1));
    top.truncate(6);

    println!();
    println!("Top log sources:");
    for ((level, target), count) in top {
        println!("  {:5} {:<36} {count:>6}", level, target);
    }

    println!();
    println!("Warnings observed: {warnings}");
}

fn percentile(sorted: &[f64], pct: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) * pct) / 100]
}

fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    let mins = (secs / 60) % 60;
    let hours = secs / 3600;
    let secs = secs % 60;
    format!("{hours}h {mins}m {secs}s")
}
