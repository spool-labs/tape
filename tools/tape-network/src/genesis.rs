//! On-chain genesis steps. Delegates to `tape_admin` so on-chain logic has a
//! single home.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::info;

use crate::settings::Settings;

/// Initialize the TAPE mint and the tapedrive `System` / `Epoch` / `Archive`
/// PDAs. Idempotent — already-initialized accounts are skipped.
///
/// Must run after `deploy_programs` and before `cache deploy` / `bootstrap`,
/// because the cache and node services read the System PDA + mint at startup.
pub async fn init(settings: &Settings, rpc_url: &str) -> Result<()> {
    let ctx = tape_admin::Context::new(rpc_url, &settings.solana.treasury_keypair)
        .map_err(|e| anyhow::anyhow!("tape-admin context: {e}"))?;

    info!("initializing TAPE mint");
    match tape_admin::mint::init(&ctx).await {
        Ok(()) => info!("TAPE mint initialized"),
        Err(e) => info!(reason = %e, "mint init skipped (likely already initialized)"),
    }

    info!("initializing chain (system + expand + epoch + archive)");
    tape_admin::chain::init_all(&ctx)
        .await
        .map_err(|e| anyhow::anyhow!("chain init: {e}"))?;

    Ok(())
}

/// Run `solana program deploy` for the four tapedrive-authored programs,
/// using the treasury keypair from settings as payer.
///
/// Returns the deployed program ids in tapedrive/token/staking/exchange order.
pub fn deploy_programs(
    settings: &Settings,
    rpc_url: &str,
    deploy_dir: Option<PathBuf>,
) -> Result<Vec<(String, String)>> {
    let ctx = tape_admin::Context::new(rpc_url, &settings.solana.treasury_keypair)
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
