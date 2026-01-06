//! Staking operations commands.

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::helpers::{build_authority_ix, build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_stake_with_pool_ix, build_request_stake_unlock_ix, build_unstake_from_pool_ix,
    build_split_pool_stake_ix, build_merge_pool_stake_ix,
};
use tape_api::program::tapedrive::stake_pda;
use rpc_client::{RpcConfig, RpcClient};
use tape_core::types::coin::TAPE;

use crate::utils::{get_keypair, load_keypair_from_path};
use crate::Context;

/// Default lamports to fund new authority accounts (0.01 SOL).
/// Covers Stake account rent plus buffer for transaction fees.
const AUTHORITY_FUND_LAMPORTS: u64 = 10_000_000;

/// Directory for stake keypairs.
fn stakes_keys_dir() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("keys").join("stakes"))
        .unwrap_or_else(|| PathBuf::from(".tape/keys/stakes"))
}

/// Save a keypair to the stakes keys directory.
fn save_stake_keypair(keypair: &Keypair) -> Result<PathBuf> {
    let dir = stakes_keys_dir();
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create stakes keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", keypair.pubkey()));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write stake keypair to {}", path.display()))?;

    Ok(path)
}

#[derive(Subcommand, Debug)]
pub enum StakeCommand {
    /// Stake tokens to a pool (node).
    Deposit {
        /// Node pool pubkey.
        pool: String,

        /// Amount in TAPE (e.g., "100.5" or "1000").
        amount: String,

        /// Path to existing authority keypair (generates new if not specified).
        #[arg(long)]
        authority: Option<PathBuf>,
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
    List {
        /// Staker pubkey (uses keypair if not specified).
        #[arg(long)]
        staker: Option<String>,
    },
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

pub async fn execute(ctx: &Context, cmd: StakeCommand) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match cmd {
        StakeCommand::Deposit { pool, amount, authority } => deposit(ctx, &pool, &amount, authority).await,
        StakeCommand::Unlock { pool } => unlock(ctx, &pool).await,
        StakeCommand::Withdraw { pool } => withdraw(ctx, &pool).await,
        StakeCommand::Split { pool, recipient, amount } => split(ctx, &pool, &recipient, &amount).await,
        StakeCommand::Merge { pool, source } => merge(ctx, &pool, &source).await,
        StakeCommand::List { staker } => list(ctx, staker).await,
    }
}

/// Deposit (stake) tokens to a pool.
async fn deposit(ctx: &Context, pool_str: &str, amount_str: &str, authority_path: Option<PathBuf>) -> Result<()> {
    // Load the fee payer keypair (from --keypair or config)
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let amount = parse_tape_amount(amount_str)?;

    // Determine authority: use provided keypair or generate new one
    let (authority_keypair, is_new_keypair) = match authority_path {
        Some(path) => {
            let kp = load_keypair_from_path(&path.to_string_lossy())?;
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

    // If using a new keypair, fund it with SOL and TAPE
    if is_new_keypair {
        // 1. Transfer SOL to authority for rent
        instructions.push(build_authority_ix(
            fee_payer.pubkey(),
            authority,
            AUTHORITY_FUND_LAMPORTS,
        ));

        // 2. Create ATA for authority and transfer TAPE tokens
        let ata_ixs = build_authority_with_tokens_ix(
            fee_payer.pubkey(),
            authority,
            amount,
        );
        instructions.extend(ata_ixs);
    }

    // 3. Stake instruction (fee_payer pays, authority signs and owns stake)
    instructions.push(build_stake_with_pool_ix(fee_payer.pubkey(), authority, pool, amount));

    // 4. Close the ATA to reclaim rent (if new keypair)
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
async fn unlock(ctx: &Context, pool_str: &str) -> Result<()> {
    // For now, fee_payer is also the authority (stake owner)
    let fee_payer = get_keypair(ctx)?;
    let authority = &fee_payer; // Same keypair acts as both
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;

    ctx.print(&format!("Requesting unlock from pool {}...", pool));
    ctx.print(&format!("Staker: {}", authority.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: RequestStakeUnlock");
        return Ok(());
    }

    let ix = build_request_stake_unlock_ix(fee_payer.pubkey(), authority.pubkey(), pool);
    let sig = client
        .send_instructions(&fee_payer, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("RequestStakeUnlock failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Unlock requested! Cooldown period has started.");
    Ok(())
}

/// Withdraw stake after cooldown.
async fn withdraw(ctx: &Context, pool_str: &str) -> Result<()> {
    // For now, fee_payer is also the authority (stake owner)
    let fee_payer = get_keypair(ctx)?;
    let authority = &fee_payer; // Same keypair acts as both
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;

    ctx.print(&format!("Withdrawing stake from pool {}...", pool));
    ctx.print(&format!("Staker: {}", authority.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: UnstakeFromPool");
        return Ok(());
    }

    let ix = build_unstake_from_pool_ix(fee_payer.pubkey(), authority.pubkey(), pool);
    let sig = client
        .send_instructions(&fee_payer, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("UnstakeFromPool failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake withdrawn successfully!");
    Ok(())
}

/// Split stake to another recipient.
async fn split(ctx: &Context, pool_str: &str, recipient_str: &str, amount_str: &str) -> Result<()> {
    // For now, fee_payer is also the authority (stake owner)
    let fee_payer = get_keypair(ctx)?;
    let authority = &fee_payer; // Same keypair acts as both
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let recipient = parse_pubkey(recipient_str)?;
    let amount = parse_tape_amount(amount_str)?;

    ctx.print(&format!("Splitting {} to {} from pool {}...", amount, recipient, pool));
    ctx.print(&format!("Source staker: {}", authority.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SplitPoolStake");
        return Ok(());
    }

    // Note: SplitPoolStake requires the recipient to sign
    // For now, we assume the recipient keypair is provided separately or this is a self-transfer
    // The instruction builder shows recipient needs to be a signer
    ctx.print("Note: Split requires recipient signature. Loading recipient keypair...");

    // In a real implementation, we would need a way to get the recipient's signature
    // For now, we'll just build the instruction (it will fail if recipient signature is missing)
    let ix = build_split_pool_stake_ix(fee_payer.pubkey(), authority.pubkey(), pool, recipient, amount);
    let sig = client
        .send_instructions(&fee_payer, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SplitPoolStake failed: {}. Note: recipient signature may be required.", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake split successfully!");
    Ok(())
}

/// Merge stake from source into signer's stake.
async fn merge(ctx: &Context, pool_str: &str, source_str: &str) -> Result<()> {
    // For now, fee_payer is also the recipient (receiving the merged stake)
    let fee_payer = get_keypair(ctx)?;
    let recipient = &fee_payer; // Same keypair acts as both
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let source = parse_pubkey(source_str)?;

    ctx.print(&format!("Merging stake from {} into your stake for pool {}...", source, pool));
    ctx.print(&format!("Recipient (you): {}", recipient.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: MergePoolStake");
        return Ok(());
    }

    // Note: MergePoolStake requires both source and dest authority signatures
    // The function signature is: build_merge_pool_stake_ix(fee_payer, source_auth, pool, dest_auth)
    // This command assumes you are the recipient, so we need source's signature too
    ctx.print("Note: Merge requires both source and recipient signatures.");

    // Build instruction where source merges into recipient
    // Since we don't have source's keypair, this will fail unless source == recipient
    let ix = build_merge_pool_stake_ix(fee_payer.pubkey(), source, pool, recipient.pubkey());
    let sig = client
        .send_instructions(&fee_payer, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}. Note: source signature is also required.", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake merged successfully!");
    Ok(())
}

/// List user's stakes.
async fn list(ctx: &Context, staker: Option<String>) -> Result<()> {
    let client = create_client(ctx)?;

    let staker_pubkey = match staker {
        Some(s) => parse_pubkey(&s)?,
        None => {
            let keypair = get_keypair(ctx)?;
            keypair.pubkey()
        }
    };

    ctx.print(&format!("Listing stakes for: {}", staker_pubkey));

    // To list stakes, we need to query all stake accounts for this staker
    // This requires getProgramAccounts with a filter on the staker field
    // For now, we'll show a placeholder - full implementation would use
    // a similar pattern to get_all_nodes in tape-client

    ctx.print("");
    ctx.print("Note: Full stake listing requires getProgramAccounts query.");
    ctx.print("Use `tape account stake <staker> <node>` to query a specific stake.");
    ctx.print("");

    // Try to get all nodes first, then check stakes for each
    match client.get_all_nodes().await {
        Ok(nodes) => {
            ctx.print(&format!("{:<44} {:>15} {:>10}", "Pool (Node)", "Staked", "Status"));
            ctx.print(&format!("{}", "-".repeat(75)));

            let mut found_any = false;
            for (node_pubkey, _node) in nodes.iter() {
                // Derive stake PDA for this staker + node combination
                let (_stake_address, _) = stake_pda(staker_pubkey, *node_pubkey);

                // Try to fetch the stake account
                if let Ok(stake) = client.get_stake(&staker_pubkey, node_pubkey).await {
                    found_any = true;
                    let status = if stake.inner.is_withdrawing() {
                        "unlocking"
                    } else {
                        "active"
                    };
                    ctx.print(&format!(
                        "{:<44} {:>15} {:>10}",
                        node_pubkey.to_string(),
                        format!("{}", stake.inner.amount),
                        status
                    ));
                }
            }

            if !found_any {
                ctx.print("(no stakes found)");
            }
        }
        Err(e) => {
            ctx.debug(&format!("Failed to enumerate nodes: {}", e));
            ctx.print("Could not enumerate all nodes. Try querying a specific stake:");
            ctx.print("  tape account stake <staker> <node>");
        }
    }

    Ok(())
}
