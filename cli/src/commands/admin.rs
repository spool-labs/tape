//! System administration commands.

use anyhow::Result;
use clap::Subcommand;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signer;

use tape_api::instruction::{
    build_advance_epoch_ix, build_create_system_ix, build_expand_system_ix, build_initialize_ix,
    build_initialize_mint_ix, build_reserve_snapshot_tape_ix,
};
use rpc_client::{RpcConfig, RpcClient};

use crate::utils::ADVANCE_EPOCH_COMPUTE_UNITS;

use crate::Context;

#[derive(Subcommand, Debug)]
pub enum AdminCommand {
    /// Initialize the full system (InitMint + CreateSystem + ExpandSystem + Initialize + ReserveSnapshotTape).
    Init,

    /// Advance to next epoch (permissionless).
    AdvanceEpoch,
}

use crate::utils::get_keypair;

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
    let keypair = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Initializing Tapedrive system...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: InitMint, CreateSystem, ExpandSystem (multiple), Initialize, ReserveSnapshotTape");
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

    // Step 5: Reserve snapshot tape (system-owned tape for epoch snapshots)
    ctx.print("Step 5: Reserving snapshot tape...");
    let ix = build_reserve_snapshot_tape_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("ReserveSnapshotTape failed: {}", e))?;
    ctx.print(&format!("  Transaction: {}", sig));

    ctx.print("System initialized successfully!");
    Ok(())
}

/// Advance to the next epoch (permissionless operation).
async fn advance_epoch(ctx: &Context) -> Result<()> {
    let keypair = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    ctx.print("Advancing epoch...");
    ctx.print(&format!("Signer: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: AdvanceEpoch");
        return Ok(());
    }

    let compute_budget_ix =
        ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_EPOCH_COMPUTE_UNITS);
    let advance_ix = build_advance_epoch_ix(keypair.pubkey(), keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![compute_budget_ix, advance_ix])
        .await
        .map_err(|e| anyhow::anyhow!("AdvanceEpoch failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Epoch advanced successfully!");
    Ok(())
}
