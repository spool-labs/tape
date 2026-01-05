//! System administration commands.

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::instruction::{
    build_advance_epoch_ix, build_create_system_ix, build_expand_system_ix, build_initialize_ix,
    build_initialize_mint_ix,
};
use tape_client::{RpcConfig, TapeClient};

use crate::config::expand_path;
use crate::Context;

#[derive(Subcommand, Debug)]
pub enum AdminCommand {
    /// Initialize the system (CreateSystem + ExpandSystem + Initialize).
    Init,

    /// Expand system account.
    ExpandSystem,

    /// Initialize TAPE token mint.
    InitMint,

    /// Advance to next epoch (permissionless).
    AdvanceEpoch,
}

/// Load keypair from the configured path.
fn load_keypair(ctx: &Context) -> Result<Keypair> {
    let path = ctx
        .keypair
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No keypair configured. Use --keypair or set keys.default in config."))?;

    let path = expand_path(&path.to_string_lossy());

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read keypair: {}", path.display()))?;

    let bytes: Vec<u8> = serde_json::from_str(&contents)
        .with_context(|| "Failed to parse keypair file (expected JSON array of bytes)")?;

    Keypair::from_bytes(&bytes).map_err(|e| anyhow::anyhow!("Invalid keypair data: {}", e))
}

/// Create a TapeClient from context.
fn create_client(ctx: &Context) -> Result<TapeClient<rpc_solana::SolanaRpc>> {
    let config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    TapeClient::new(config).map_err(|e| anyhow::anyhow!("Failed to create RPC client: {}", e))
}

pub async fn execute(ctx: &Context, cmd: AdminCommand) -> Result<()> {
    ctx.debug(&format!("Using cluster: {}", ctx.cluster));

    match cmd {
        AdminCommand::Init => init_system(ctx).await,
        AdminCommand::ExpandSystem => expand_system(ctx).await,
        AdminCommand::InitMint => init_mint(ctx).await,
        AdminCommand::AdvanceEpoch => advance_epoch(ctx).await,
    }
}

/// Initialize the full system: CreateSystem + ExpandSystem + Initialize.
async fn init_system(ctx: &Context) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Initializing Tapedrive system...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: CreateSystem, ExpandSystem, Initialize");
        return Ok(());
    }

    // Step 1: CreateSystem
    ctx.print("Step 1/3: Creating system account...");
    let ix = build_create_system_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("CreateSystem failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    // Step 2: ExpandSystem
    ctx.print("Step 2/3: Expanding system account...");
    let ix = build_expand_system_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("ExpandSystem failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    // Step 3: Initialize
    ctx.print("Step 3/3: Initializing system state...");
    let ix = build_initialize_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Initialize failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    ctx.print("System initialized successfully!");
    Ok(())
}

/// Expand system account (for incremental allocation).
async fn expand_system(ctx: &Context) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Expanding system account...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: ExpandSystem");
        return Ok(());
    }

    let ix = build_expand_system_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("ExpandSystem failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("System account expanded successfully!");
    Ok(())
}

/// Initialize the TAPE token mint.
async fn init_mint(ctx: &Context) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Initializing TAPE token mint...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: InitializeMint");
        return Ok(());
    }

    let ix = build_initialize_mint_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("InitializeMint failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("TAPE token mint initialized successfully!");
    Ok(())
}

/// Advance to the next epoch (permissionless operation).
async fn advance_epoch(ctx: &Context) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Advancing epoch...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: AdvanceEpoch");
        return Ok(());
    }

    let ix = build_advance_epoch_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("AdvanceEpoch failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Epoch advanced successfully!");
    Ok(())
}
