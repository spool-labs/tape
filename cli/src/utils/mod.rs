//! CLI utilities.

use std::path::PathBuf;

// =============================================================================
// Compute Budget Constants
// =============================================================================

/// Compute unit limit for AdvanceEpoch instruction.
/// D'Hondt spool allocation with many nodes is expensive.
pub const ADVANCE_EPOCH_COMPUTE_UNITS: u32 = 1_400_000;

/// Compute unit limit for CertifyTrack instruction.
/// BLS signature verification is expensive.
pub const CERTIFY_TRACK_COMPUTE_UNITS: u32 = 1_400_000;

use anyhow::{Context as _, Result};
use indicatif::{ProgressBar, ProgressStyle};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::program::tapedrive::{tape_pda, stake_pda};
use tape_api::program::exchange::exchange_pda;
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

/// Resolve an account address to its authority keypair.
///
/// If the string looks like a path (contains `/` or ends with `.json`),
/// loads the keypair from that path.
///
/// Otherwise, treats it as an on-chain account address and looks up the
/// authority keypair in `~/.tape/keys/{type}/{account_address}.json`.
pub fn resolve_authority(account_address: &str, auth_type: AuthorityType) -> Result<Keypair> {
    let is_path = account_address.contains('/') || account_address.ends_with(".json");

    if is_path {
        load_keypair_from_path(account_address)
    } else {
        // Treat as account address, look up authority keypair in keys directory
        let keys_dir = authority_keys_dir(auth_type);
        let keypair_path = keys_dir.join(format!("{}.json", account_address));

        if !keypair_path.exists() {
            anyhow::bail!(
                "Keypair not found for account: {}\nLooked in: {}\nUse `tape {} list` to see available accounts.",
                account_address,
                keypair_path.display(),
                auth_type.subdir().trim_end_matches('s') // "tapes" -> "tape"
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

// ============================================================================
// Keypair save functions - save authority keypair by on-chain account address
// ============================================================================

/// Save a tape authority keypair, indexed by the tape's on-chain address.
///
/// Returns the tape address and the path where the keypair was saved.
pub fn save_tape_keypair(keypair: &Keypair) -> Result<(Pubkey, PathBuf)> {
    let authority = keypair.pubkey();
    let (tape_address, _) = tape_pda(authority);

    let dir = authority_keys_dir(AuthorityType::Tape);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create tapes keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", tape_address));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write tape keypair to {}", path.display()))?;

    Ok((tape_address, path))
}

/// Save a stake authority keypair, indexed by the stake's on-chain address.
///
/// Returns the stake address and the path where the keypair was saved.
pub fn save_stake_keypair(keypair: &Keypair) -> Result<(Pubkey, PathBuf)> {
    let authority = keypair.pubkey();
    let (stake_address, _) = stake_pda(authority);

    let dir = authority_keys_dir(AuthorityType::Stake);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create stakes keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", stake_address));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write stake keypair to {}", path.display()))?;

    Ok((stake_address, path))
}

/// Save an exchange authority keypair, indexed by the exchange's on-chain address.
///
/// Returns the exchange address and the path where the keypair was saved.
pub fn save_exchange_keypair(keypair: &Keypair) -> Result<(Pubkey, PathBuf)> {
    let authority = keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);

    let dir = authority_keys_dir(AuthorityType::Exchange);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create exchanges keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", exchange_address));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write exchange keypair to {}", path.display()))?;

    Ok((exchange_address, path))
}
