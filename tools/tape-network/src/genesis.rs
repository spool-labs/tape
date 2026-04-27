//! On-chain genesis steps. Delegates to `tape_admin` so on-chain logic has a
//! single home.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::info;

use crate::settings::Settings;

/// Run `solana program deploy` for the four tapedrive-authored programs,
/// using the treasury keypair from settings as payer.
///
/// Returns the deployed program ids in tapedrive/token/staking/exchange order.
pub fn deploy_programs(settings: &Settings, deploy_dir: Option<PathBuf>) -> Result<Vec<(String, String)>> {
    let ctx = tape_admin::Context::new(
        settings.solana.upstream_url(),
        &settings.solana.treasury_keypair,
    )
    .map_err(|e| anyhow::anyhow!("tape-admin context: {e}"))?;

    let deploy_dir = deploy_dir.unwrap_or_else(tape_admin::programs::default_deploy_dir);
    info!(dir = %deploy_dir.display(), "deploying programs");

    let deployed = tape_admin::programs::deploy(&ctx, tape_admin::programs::Program::All, &deploy_dir)
        .map_err(|e| anyhow::anyhow!("deploy: {e}"))
        .context("running tape-admin programs deploy")?;

    Ok(deployed
        .into_iter()
        .map(|(name, pk)| (name, pk.to_string()))
        .collect())
}
