//! CLI utilities.

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use solana_sdk::signature::Keypair;
use tape_sdk::load_solana_keypair;

use crate::config::expand_path;
use crate::Context;

/// Load the fee payer keypair from CLI context.
///
/// This loads the keypair from the path specified via --keypair flag
/// or the keys.default config setting.
pub fn get_keypair(ctx: &Context) -> Result<Keypair> {
    let path = ctx.keypair.as_ref()
        .ok_or_else(|| anyhow::anyhow!("No keypair configured. Use --keypair or set keys.default in config."))?;

    load_keypair_from_path(&path.to_string_lossy())
}

/// Load a keypair from an arbitrary file path.
///
/// Handles path expansion (e.g., ~ for home directory).
pub fn load_keypair_from_path(path: &str) -> Result<Keypair> {
    let expanded = expand_path(path);
    load_solana_keypair(&expanded)
        .map_err(|e| anyhow::anyhow!("Failed to load keypair from {}: {}", path, e))
}

/// Create a spinner for long-running operations.
pub fn spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.blue} {msg}")
            .unwrap(),
    );
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

/// Create a progress bar.
#[allow(dead_code)]
pub fn progress_bar(len: u64, msg: &str) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_message(msg.to_string());
    pb
}
