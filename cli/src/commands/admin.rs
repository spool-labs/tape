//! System administration commands.

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::instruction::{
    build_advance_epoch_ix, build_create_system_ix, build_expand_system_ix, build_initialize_ix,
    build_initialize_mint_ix,
};
use rpc_client::{RpcConfig, RpcClient};

use crate::config::expand_path;
use crate::Context;

#[derive(Subcommand, Debug)]
pub enum AdminCommand {
    /// Initialize the full system (InitMint + CreateSystem + ExpandSystem + Initialize).
    Init,

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

/// Create a RpcClient from context.
fn create_client(ctx: &Context) -> Result<RpcClient<rpc_solana::SolanaRpc>> {
    let config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    RpcClient::new(config).map_err(|e| anyhow::anyhow!("Failed to create RPC client: {}", e))
}

pub async fn execute(ctx: &Context, cmd: AdminCommand) -> Result<()> {
    ctx.debug(&format!("Using cluster: {}", ctx.cluster));

    match cmd {
        AdminCommand::Init => init_system(ctx).await,
        AdminCommand::AdvanceEpoch => advance_epoch(ctx).await,
    }
}

/// Initialize the full system: InitMint + CreateSystem + ExpandSystem (repeated) + Initialize.
async fn init_system(ctx: &Context) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Initializing Tapedrive system...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: InitMint, CreateSystem, ExpandSystem (multiple), Initialize");
        return Ok(());
    }

    // Step 1: Initialize TAPE token mint
    ctx.print("Step 1: Initializing TAPE token mint...");
    let ix = build_initialize_mint_ix(keypair.pubkey(), keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("InitializeMint failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    // Step 2: CreateSystem
    ctx.print("Step 2: Creating system account...");
    let ix = build_create_system_ix(keypair.pubkey(), keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("CreateSystem failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    // Step 3: ExpandSystem (repeat until fully expanded)
    // System account is ~45KB, MAX_PERMITTED_DATA_INCREASE is 10KB per tx
    // Need multiple expand calls until the account reaches full size
    ctx.print("Step 3: Expanding system account...");
    let mut expansion_count = 0;
    for _ in 0..10 {
        let ix = build_expand_system_ix(keypair.pubkey(), keypair.pubkey());
        match client.send_instructions(&keypair, vec![ix]).await {
            Ok(sig) => {
                expansion_count += 1;
                ctx.print(&format!("  Expansion {}: {}", expansion_count, sig));
            }
            Err(e) => {
                // AccountAlreadyInitialized means we've reached full size
                let err_str = format!("{:?}", e);
                if err_str.contains("AccountAlreadyInitialized")
                    || err_str.contains("already initialized")
                    || err_str.contains("uninitialized account")
                {
                    break;
                }
                return Err(anyhow::anyhow!("ExpandSystem failed: {}", e));
            }
        }
    }
    ctx.print(&format!("  System account expanded ({} iterations)", expansion_count));

    // Step 4: Initialize
    ctx.print("Step 4: Initializing system state...");
    let ix = build_initialize_ix(keypair.pubkey(), keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Initialize failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    ctx.print("System initialized successfully!");
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

    let ix = build_advance_epoch_ix(keypair.pubkey(), keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("AdvanceEpoch failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Epoch advanced successfully!");
    Ok(())
}
