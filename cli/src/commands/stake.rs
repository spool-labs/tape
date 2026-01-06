//! Staking operations commands.

use std::str::FromStr;

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use comfy_table::{presets::UTF8_FULL, Table};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::helpers::{build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_stake_with_pool_ix, build_request_stake_unlock_ix, build_unstake_from_pool_ix,
    build_split_pool_stake_ix, build_merge_pool_stake_ix,
};
use tape_api::program::tapedrive::stake_pda;
use rpc_client::{RpcConfig, RpcClient};
use tape_core::types::coin::TAPE;

use crate::output::OutputFormat;
use crate::utils::{get_keypair, resolve_authority, authority_keys_dir, save_stake_keypair, load_keypair_from_path, AuthorityType};
use crate::Context;

/// Stake subcommand arguments.
#[derive(Args, Debug)]
pub struct StakeArgs {
    #[command(subcommand)]
    pub command: StakeCommand,
}

#[derive(Subcommand, Debug)]
pub enum StakeCommand {
    /// Stake tokens to a pool (node). Creates new stake with new keypair.
    Deposit {
        /// Node pool pubkey.
        pool: String,

        /// Amount in TAPE (e.g., "100.5" or "1000").
        amount: String,
    },

    /// Request stake unlock (starts cooldown).
    Unlock {
        /// Stake account address.
        stake: String,
    },

    /// Withdraw stake after cooldown.
    Withdraw {
        /// Stake account address.
        stake: String,
    },

    /// Split stake to a new account.
    Split {
        /// Source stake account address.
        stake: String,

        /// Amount to split in TAPE.
        #[arg(long)]
        amount: String,
    },

    /// Merge stakes from another account.
    Merge {
        /// Destination stake account address (will receive merged stake).
        destination: String,

        /// Source stake account address (will be merged and closed).
        source: String,
    },

    /// List all saved stakes.
    List,
}

/// Create a RpcClient from context.
fn create_client(ctx: &Context) -> Result<RpcClient<rpc_solana::SolanaRpc>> {
    let config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    RpcClient::new(config).map_err(|e| anyhow::anyhow!("Failed to create RPC client: {}", e))
}

/// Parse a pubkey string.
fn parse_pubkey(s: &str) -> Result<Pubkey> {
    Pubkey::from_str(s).map_err(|e| anyhow::anyhow!("Invalid pubkey '{}': {}", s, e))
}

/// Parse a TAPE amount string.
fn parse_tape_amount(s: &str) -> Result<TAPE> {
    TAPE::parse(s).map_err(|_| anyhow::anyhow!("Invalid TAPE amount '{}'. Use format like '100.5' or '1000'", s))
}

pub async fn execute(ctx: &Context, args: StakeArgs) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match args.command {
        StakeCommand::Deposit { pool, amount } => {
            deposit(ctx, &pool, &amount).await
        }
        StakeCommand::Unlock { stake } => {
            unlock(ctx, &stake).await
        }
        StakeCommand::Withdraw { stake } => {
            withdraw(ctx, &stake).await
        }
        StakeCommand::Split { stake, amount } => {
            split(ctx, &stake, &amount).await
        }
        StakeCommand::Merge { destination, source } => {
            merge(ctx, &destination, &source).await
        }
        StakeCommand::List => {
            list(ctx).await
        }
    }
}

/// Deposit (stake) tokens to a pool.
async fn deposit(
    ctx: &Context,
    pool_str: &str,
    amount_str: &str,
) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let amount = parse_tape_amount(amount_str)?;

    // Generate a new unique keypair for this stake
    let authority_keypair = Keypair::new();
    let authority = authority_keypair.pubkey();
    let (stake_address, _) = stake_pda(authority);

    ctx.print(&format!("Staking {} to pool {}...", amount, pool));
    ctx.print(&format!("Fee payer: {}", fee_payer.pubkey()));
    ctx.print(&format!("Stake: {} (new)", stake_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: StakeWithPool");
        ctx.print(&format!("[DRY RUN] Would create stake: {}", stake_address));
        return Ok(());
    }

    // Build instructions: create ATA and transfer TAPE tokens
    let mut instructions = Vec::new();
    let ata_ixs = build_authority_with_tokens_ix(
        fee_payer.pubkey(),
        authority,
        amount,
    );
    instructions.extend(ata_ixs);

    // Stake instruction (fee_payer pays, authority signs and owns stake)
    instructions.push(build_stake_with_pool_ix(fee_payer.pubkey(), authority, pool, amount));

    // Close the ATA to reclaim rent
    instructions.push(build_close_ata_ix(authority, fee_payer.pubkey()));

    // Send with authority as additional signer
    let sig = client
        .send_instructions_with_signers(&fee_payer, instructions, &[&authority_keypair])
        .await
        .map_err(|e| anyhow::anyhow!("StakeWithPool failed: {}", e))?;

    // Save the new keypair (indexed by stake address)
    let (_, keypair_path) = save_stake_keypair(&authority_keypair)?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake deposited successfully!");
    ctx.print(&format!("Stake: {}", stake_address));
    ctx.print(&format!("Keypair saved: {}", keypair_path.display()));

    Ok(())
}

/// Request stake unlock (starts cooldown period).
async fn unlock(ctx: &Context, stake_address_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    // Resolve authority keypair from stake address
    let authority_keypair = resolve_authority(stake_address_str, AuthorityType::Stake)?;
    let authority = authority_keypair.pubkey();
    let (stake_address, _) = stake_pda(authority);

    // Fetch stake to get the pool
    let stake = client.get_stake(&authority).await
        .map_err(|e| anyhow::anyhow!("Failed to fetch stake: {}", e))?;
    let pool = stake.pool;

    ctx.print(&format!("Requesting unlock for stake {}...", stake_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: RequestStakeUnlock");
        return Ok(());
    }

    let ix = build_request_stake_unlock_ix(fee_payer.pubkey(), authority, pool);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("RequestStakeUnlock failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("RequestStakeUnlock failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Unlock requested! Cooldown period has started.");
    Ok(())
}

/// Withdraw stake after cooldown.
async fn withdraw(ctx: &Context, stake_address_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    // Resolve authority keypair from stake address
    let authority_keypair = resolve_authority(stake_address_str, AuthorityType::Stake)?;
    let authority = authority_keypair.pubkey();
    let (stake_address, _) = stake_pda(authority);

    // Fetch stake to get the pool
    let stake = client.get_stake(&authority).await
        .map_err(|e| anyhow::anyhow!("Failed to fetch stake: {}", e))?;
    let pool = stake.pool;

    ctx.print(&format!("Withdrawing stake {}...", stake_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: UnstakeFromPool");
        return Ok(());
    }

    let ix = build_unstake_from_pool_ix(fee_payer.pubkey(), authority, pool);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("UnstakeFromPool failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("UnstakeFromPool failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake withdrawn successfully!");
    Ok(())
}

/// Split stake to a new account.
async fn split(
    ctx: &Context,
    stake_address_str: &str,
    amount_str: &str,
) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Resolve source authority keypair from stake address
    let source_keypair = resolve_authority(stake_address_str, AuthorityType::Stake)?;
    let source_authority = source_keypair.pubkey();
    let (source_stake_address, _) = stake_pda(source_authority);

    // Fetch stake to get the pool
    let stake = client.get_stake(&source_authority).await
        .map_err(|e| anyhow::anyhow!("Failed to fetch stake: {}", e))?;
    let pool = stake.pool;

    // Generate new keypair for recipient
    let recipient_keypair = Keypair::new();
    let recipient_authority = recipient_keypair.pubkey();
    let (new_stake_address, _) = stake_pda(recipient_authority);

    ctx.print(&format!("Splitting {} from stake {}...", amount, source_stake_address));
    ctx.print(&format!("New stake: {} (new)", new_stake_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SplitPoolStake");
        ctx.print(&format!("[DRY RUN] Would create stake: {}", new_stake_address));
        return Ok(());
    }

    let ix = build_split_pool_stake_ix(fee_payer.pubkey(), source_authority, pool, recipient_authority, amount);

    // Both source and recipient need to sign
    let sig = if fee_payer.pubkey() == source_authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&recipient_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("SplitPoolStake failed: {}", e))?
    } else {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&source_keypair, &recipient_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("SplitPoolStake failed: {}", e))?
    };

    // Save the new recipient keypair
    let (_, keypair_path) = save_stake_keypair(&recipient_keypair)?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake split successfully!");
    ctx.print(&format!("Source: {}", source_stake_address));
    ctx.print(&format!("New stake: {}", new_stake_address));
    ctx.print(&format!("Keypair saved: {}", keypair_path.display()));
    Ok(())
}

/// Merge stakes.
async fn merge(
    ctx: &Context,
    dest_stake_str: &str,
    source_stake_str: &str,
) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    // Resolve both keypairs from stake addresses
    let source_keypair = resolve_authority(source_stake_str, AuthorityType::Stake)?;
    let dest_keypair = resolve_authority(dest_stake_str, AuthorityType::Stake)?;

    let source_authority = source_keypair.pubkey();
    let dest_authority = dest_keypair.pubkey();
    let (source_stake_address, _) = stake_pda(source_authority);
    let (dest_stake_address, _) = stake_pda(dest_authority);

    // Fetch source stake to get the pool
    let stake = client.get_stake(&source_authority).await
        .map_err(|e| anyhow::anyhow!("Failed to fetch source stake: {}", e))?;
    let pool = stake.pool;

    ctx.print(&format!("Merging stake {} into {}...", source_stake_address, dest_stake_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: MergePoolStake");
        return Ok(());
    }

    let ix = build_merge_pool_stake_ix(fee_payer.pubkey(), source_authority, pool, dest_authority);

    // Both source and dest need to sign
    let fee_payer_is_source = fee_payer.pubkey() == source_authority;
    let fee_payer_is_dest = fee_payer.pubkey() == dest_authority;

    let sig = if fee_payer_is_source && fee_payer_is_dest {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}", e))?
    } else if fee_payer_is_source {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&dest_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}", e))?
    } else if fee_payer_is_dest {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&source_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}", e))?
    } else {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&source_keypair, &dest_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stakes merged successfully!");
    ctx.print(&format!("Source (closed): {}", source_stake_address));
    ctx.print(&format!("Destination: {}", dest_stake_address));
    Ok(())
}

/// List all saved stakes.
async fn list(ctx: &Context) -> Result<()> {
    let client = create_client(ctx)?;

    // Collect stakes to display: (stake_address, stake_data)
    let mut stakes: Vec<(Pubkey, tape_api::state::Stake)> = Vec::new();
    let mut not_found: Vec<Pubkey> = Vec::new();

    // List all saved stake keypairs (filenames are stake addresses)
    let stakes_dir = authority_keys_dir(AuthorityType::Stake);

    if !stakes_dir.exists() {
        match ctx.output {
            OutputFormat::Json => println!("[]"),
            _ => {
                println!("No stakes found.");
                println!("Use `tape stake deposit` to stake tokens.");
            }
        }
        return Ok(());
    }

    let entries: Vec<_> = std::fs::read_dir(&stakes_dir)
        .with_context(|| format!("Failed to read stakes directory: {}", stakes_dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .collect();

    if entries.is_empty() {
        match ctx.output {
            OutputFormat::Json => println!("[]"),
            _ => {
                println!("No stakes found.");
                println!("Use `tape stake deposit` to stake tokens.");
            }
        }
        return Ok(());
    }

    for entry in entries {
        let path = entry.path();
        let filename = entry.file_name();
        let stake_address_str = filename.to_string_lossy();
        let stake_address_str = stake_address_str.trim_end_matches(".json");

        // Parse stake address from filename
        let stake_address: Pubkey = match stake_address_str.parse() {
            Ok(pk) => pk,
            Err(_) => continue,
        };

        // Load keypair to get authority
        let keypair = match load_keypair_from_path(&path.to_string_lossy()) {
            Ok(kp) => kp,
            Err(_) => continue,
        };
        let authority = keypair.pubkey();

        // Fetch stake using authority
        match client.get_stake(&authority).await {
            Ok(stake) => stakes.push((stake_address, stake)),
            Err(e) => {
                if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                    not_found.push(stake_address);
                }
            }
        }
    }

    // Output based on format
    match ctx.output {
        OutputFormat::Json => {
            let json_stakes: Vec<_> = stakes.iter().map(|(stake_address, stake)| {
                serde_json::json!({
                    "address": stake_address.to_string(),
                    "pool": stake.pool.to_string(),
                    "amount": stake.inner.amount.as_u64(),
                    "activation_epoch": stake.inner.activation_epoch.as_u64(),
                    "status": if stake.inner.is_withdrawing() { "unlocking" } else { "active" },
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&json_stakes)?);
        }
        _ => {
            if stakes.is_empty() && not_found.is_empty() {
                println!("No stakes found.");
                println!("Use `tape stake deposit` to stake tokens.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Stake", "Pool", "Amount", "Status"]);

            for (stake_address, stake) in &stakes {
                let status = if stake.inner.is_withdrawing() { "unlocking" } else { "active" };
                table.add_row(vec![
                    &stake_address.to_string(),
                    &stake.pool.to_string(),
                    &format!("{}", stake.inner.amount),
                    status,
                ]);
            }

            for stake_address in &not_found {
                table.add_row(vec![
                    &stake_address.to_string(),
                    "(not found on-chain)",
                    "",
                    "",
                ]);
            }

            println!("{}", table);
            println!("\nTotal: {} stake(s)", stakes.len());
        }
    }

    Ok(())
}
