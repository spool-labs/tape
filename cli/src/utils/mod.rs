//! CLI utilities.

use std::path::PathBuf;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use solana_sdk::signature::Keypair;
use tape_sdk::load_solana_keypair;

use crate::config::expand_path;
use crate::Context;

/// Authority key types for resolution.
#[derive(Debug, Clone, Copy)]
pub enum AuthorityType {
    Tape,
    Stake,
    Exchange,
}

impl AuthorityType {
    /// Get the subdirectory name for this authority type.
    pub fn subdir(&self) -> &'static str {
        match self {
            AuthorityType::Tape => "tapes",
            AuthorityType::Stake => "stakes",
            AuthorityType::Exchange => "exchanges",
        }
    }
}

/// Get the keys directory for a specific authority type.
pub fn authority_keys_dir(auth_type: AuthorityType) -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("keys").join(auth_type.subdir()))
        .unwrap_or_else(|| PathBuf::from(format!(".tape/keys/{}", auth_type.subdir())))
}

/// Resolve an authority string to a keypair.
///
/// If the string looks like a path (contains `/` or ends with `.json`),
/// loads the keypair from that path.
///
/// Otherwise, treats it as a pubkey and looks up the keypair in
/// `~/.tape/keys/{type}/{pubkey}.json`.
pub fn resolve_authority(authority: &str, auth_type: AuthorityType) -> Result<Keypair> {
    let is_path = authority.contains('/') || authority.ends_with(".json");

    if is_path {
        load_keypair_from_path(authority)
    } else {
        // Treat as pubkey, look up in keys directory
        let keys_dir = authority_keys_dir(auth_type);
        let keypair_path = keys_dir.join(format!("{}.json", authority));

        if !keypair_path.exists() {
            anyhow::bail!(
                "Authority keypair not found: {}\nLooked in: {}\nUse `tape keys list {}` to see available keypairs.",
                authority,
                keypair_path.display(),
                auth_type.subdir()
            );
        }

        load_keypair_from_path(&keypair_path.to_string_lossy())
    }
}

/// List all authority keypairs of a given type.
#[allow(dead_code)]
pub fn list_authority_keypairs(auth_type: AuthorityType) -> Result<Vec<(String, PathBuf)>> {
    let dir = authority_keys_dir(auth_type);

    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut keypairs = Vec::new();

    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map_or(false, |e| e == "json") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                keypairs.push((stem.to_string(), path));
            }
        }
    }

    keypairs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(keypairs)
}

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
