//! Staking operations commands.

use std::path::PathBuf;
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
use crate::utils::{get_keypair, resolve_authority, authority_keys_dir, AuthorityType};
use crate::Context;

/// Save a keypair to the stakes keys directory.
fn save_stake_keypair(keypair: &Keypair) -> Result<PathBuf> {
    let dir = authority_keys_dir(AuthorityType::Stake);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create stakes keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", keypair.pubkey()));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write stake keypair to {}", path.display()))?;

    Ok(path)
}

/// Stake subcommand arguments with global authority flag.
#[derive(Args, Debug)]
pub struct StakeArgs {
    /// Staker authority keypair: path to file OR pubkey (resolves to ~/.tape/keys/stakes/{pubkey}.json).
    /// If not specified, uses --keypair as the authority.
    #[arg(long, short = 'a', global = true)]
    pub authority: Option<String>,

    #[command(subcommand)]
    pub command: StakeCommand,
}

#[derive(Subcommand, Debug)]
pub enum StakeCommand {
    /// Stake tokens to a pool (node).
    /// If no authority is specified, generates a new keypair.
    Deposit {
        /// Node pool pubkey.
        pool: String,

        /// Amount in TAPE (e.g., "100.5" or "1000").
        amount: String,
    },

    /// Request stake unlock (starts cooldown).
    Unlock {
        /// Node pool pubkey.
        pool: String,
    },

    /// Withdraw stake after cooldown.
    Withdraw {
        /// Node pool pubkey.
        pool: String,
    },

    /// Split stake to another account.
    Split {
        /// Node pool pubkey.
        pool: String,

        /// Recipient pubkey.
        recipient: String,

        /// Amount to split in TAPE.
        amount: String,
    },

    /// Merge stakes from another account.
    Merge {
        /// Node pool pubkey.
        pool: String,

        /// Source staker pubkey (whose stake will be merged into yours).
        source: String,
    },

    /// List user's stakes.
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
            deposit(ctx, args.authority, &pool, &amount).await
        }
        StakeCommand::Unlock { pool } => {
            unlock(ctx, args.authority, &pool).await
        }
        StakeCommand::Withdraw { pool } => {
            withdraw(ctx, args.authority, &pool).await
        }
        StakeCommand::Split { pool, recipient, amount } => {
            split(ctx, args.authority, &pool, &recipient, &amount).await
        }
        StakeCommand::Merge { pool, source } => {
            merge(ctx, args.authority, &pool, &source).await
        }
        StakeCommand::List => {
            list(ctx, args.authority).await
        }
    }
}

/// Deposit (stake) tokens to a pool.
async fn deposit(
    ctx: &Context,
    authority_arg: Option<String>,
    pool_str: &str,
    amount_str: &str,
) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let amount = parse_tape_amount(amount_str)?;

    // Determine authority: resolve from arg, or generate new one
    let (authority_keypair, is_new_keypair) = match authority_arg {
        Some(auth) => {
            let kp = resolve_authority(&auth, AuthorityType::Stake)?;
            (kp, false)
        }
        None => {
            // Generate a new unique keypair for this stake
            (Keypair::new(), true)
        }
    };

    let authority = authority_keypair.pubkey();
    let (stake_address, _) = stake_pda(authority, pool);

    ctx.print(&format!("Staking {} to pool {}...", amount, pool));
    ctx.print(&format!("Fee payer: {}", fee_payer.pubkey()));
    ctx.print(&format!("Staker: {}{}", authority, if is_new_keypair { " (new)" } else { "" }));
    ctx.print(&format!("Stake PDA: {}", stake_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: StakeWithPool");
        if is_new_keypair {
            ctx.print(&format!("[DRY RUN] Would generate new staker: {}", authority));
        }
        return Ok(());
    }

    // Build instructions
    let mut instructions = Vec::new();

    // If using a new keypair, create ATA and transfer TAPE tokens
    if is_new_keypair {
        let ata_ixs = build_authority_with_tokens_ix(
            fee_payer.pubkey(),
            authority,
            amount,
        );
        instructions.extend(ata_ixs);
    }

    // Stake instruction (fee_payer pays, authority signs and owns stake)
    instructions.push(build_stake_with_pool_ix(fee_payer.pubkey(), authority, pool, amount));

    // Close the ATA to reclaim rent (if new keypair)
    if is_new_keypair {
        instructions.push(build_close_ata_ix(authority, fee_payer.pubkey()));
    }

    // Send with both signers if using new keypair or different authority
    let sig = if is_new_keypair || fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, instructions, &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("StakeWithPool failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, instructions)
            .await
            .map_err(|e| anyhow::anyhow!("StakeWithPool failed: {}", e))?
    };

    // Save the new keypair
    let keypair_path = if is_new_keypair {
        let path = save_stake_keypair(&authority_keypair)?;
        Some(path)
    } else {
        None
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake deposited successfully!");

    if let Some(path) = keypair_path {
        ctx.print(&format!("Keypair saved: {}", path.display()));
    }

    Ok(())
}

/// Request stake unlock (starts cooldown period).
async fn unlock(ctx: &Context, authority_arg: Option<String>, pool_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Stake)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();

    ctx.print(&format!("Requesting unlock from pool {}...", pool));
    ctx.print(&format!("Staker: {}", authority));

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
async fn withdraw(ctx: &Context, authority_arg: Option<String>, pool_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Stake)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();

    ctx.print(&format!("Withdrawing stake from pool {}...", pool));
    ctx.print(&format!("Staker: {}", authority));

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

/// Split stake to another recipient.
async fn split(
    ctx: &Context,
    authority_arg: Option<String>,
    pool_str: &str,
    recipient_str: &str,
    amount_str: &str,
) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let recipient = parse_pubkey(recipient_str)?;
    let amount = parse_tape_amount(amount_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Stake)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();

    ctx.print(&format!("Splitting {} to {} from pool {}...", amount, recipient, pool));
    ctx.print(&format!("Source staker: {}", authority));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SplitPoolStake");
        return Ok(());
    }

    // Note: SplitPoolStake requires the recipient to sign
    if recipient != authority {
        anyhow::bail!(
            "Split requires recipient ({}) to sign. Multi-party signing not yet supported in CLI. \
             For self-split, use your own pubkey as recipient.",
            recipient
        );
    }

    let ix = build_split_pool_stake_ix(fee_payer.pubkey(), authority, pool, recipient, amount);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("SplitPoolStake failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("SplitPoolStake failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake split successfully!");
    Ok(())
}

/// Merge stake from source into signer's stake.
async fn merge(
    ctx: &Context,
    authority_arg: Option<String>,
    pool_str: &str,
    source_str: &str,
) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let source = parse_pubkey(source_str)?;

    // Resolve authority keypair (this is the recipient/destination)
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Stake)?,
        None => get_keypair(ctx)?,
    };

    let recipient = authority_keypair.pubkey();

    ctx.print(&format!("Merging stake from {} into your stake for pool {}...", source, pool));
    ctx.print(&format!("Recipient (you): {}", recipient));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: MergePoolStake");
        return Ok(());
    }

    // Note: MergePoolStake requires both source and dest authority signatures
    if source != recipient {
        anyhow::bail!(
            "Merge requires source ({}) to sign. Multi-party signing not yet supported in CLI.",
            source
        );
    }

    let ix = build_merge_pool_stake_ix(fee_payer.pubkey(), source, pool, recipient);

    let sig = if fee_payer.pubkey() != recipient {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake merged successfully!");
    Ok(())
}

/// List user's stakes.
async fn list(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    let client = create_client(ctx)?;

    // Collect stakes to display
    let mut stakes: Vec<(Pubkey, Pubkey, tape_api::state::Stake)> = Vec::new(); // (staker, pool, stake)

    // Get all nodes to check stakes against
    let nodes = match client.get_all_nodes().await {
        Ok(n) => n,
        Err(e) => {
            ctx.debug(&format!("Failed to enumerate nodes: {}", e));
            match ctx.output {
                OutputFormat::Json => println!("[]"),
                _ => {
                    println!("Could not enumerate nodes to find stakes.");
                    println!("Try querying a specific stake: tape account stake <staker> <node>");
                }
            }
            return Ok(());
        }
    };

    // If authority is provided, query just that staker
    if let Some(auth) = authority_arg {
        let staker_pubkey: Pubkey = auth.parse()
            .with_context(|| format!("Invalid staker pubkey: {}", auth))?;

        for (node_pubkey, _node) in nodes.iter() {
            if let Ok(stake) = client.get_stake(&staker_pubkey, node_pubkey).await {
                stakes.push((staker_pubkey, *node_pubkey, stake));
            }
        }
    } else {
        // No authority provided - list all saved stake keypairs
        let stakes_dir = authority_keys_dir(AuthorityType::Stake);

        if stakes_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&stakes_dir)
                .with_context(|| format!("Failed to read stakes directory: {}", stakes_dir.display()))?
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
                .collect();

            for entry in entries {
                let filename = entry.file_name();
                let pubkey_str = filename.to_string_lossy();
                let pubkey_str = pubkey_str.trim_end_matches(".json");

                let staker_pubkey: Pubkey = match pubkey_str.parse() {
                    Ok(pk) => pk,
                    Err(_) => continue,
                };

                for (node_pubkey, _node) in nodes.iter() {
                    if let Ok(stake) = client.get_stake(&staker_pubkey, node_pubkey).await {
                        stakes.push((staker_pubkey, *node_pubkey, stake));
                    }
                }
            }
        }
    }

    // Output based on format
    match ctx.output {
        OutputFormat::Json => {
            let json_stakes: Vec<_> = stakes.iter().map(|(staker, pool, stake)| {
                serde_json::json!({
                    "staker": staker.to_string(),
                    "pool": pool.to_string(),
                    "amount": stake.inner.amount.as_u64(),
                    "activation_epoch": stake.inner.activation_epoch.as_u64(),
                    "status": if stake.inner.is_withdrawing() { "unlocking" } else { "active" },
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&json_stakes)?);
        }
        _ => {
            if stakes.is_empty() {
                println!("No stakes found.");
                println!("Use `tape stake deposit` to stake tokens.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Staker", "Pool (Node)", "Amount", "Status"]);

            for (staker, pool, stake) in &stakes {
                let status = if stake.inner.is_withdrawing() { "unlocking" } else { "active" };
                table.add_row(vec![
                    &staker.to_string(),
                    &pool.to_string(),
                    &format!("{}", stake.inner.amount),
                    status,
                ]);
            }

            println!("{}", table);
            println!("\nTotal: {} stake(s)", stakes.len());
        }
    }

    Ok(())
}
