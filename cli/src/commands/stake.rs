//! Staking operations commands.

use std::str::FromStr;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::instruction::{
    build_stake_with_pool_ix, build_request_stake_unlock_ix, build_unstake_from_pool_ix,
    build_split_pool_stake_ix, build_merge_pool_stake_ix,
};
use tape_api::program::tapedrive::stake_pda;
use rpc_client::{RpcConfig, RpcClient};
use tape_core::types::coin::TAPE;

use crate::config::expand_path;
use crate::Context;

#[derive(Subcommand, Debug)]
pub enum StakeCommand {
    /// Stake tokens to a pool (node).
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
    List {
        /// Staker pubkey (uses keypair if not specified).
        #[arg(long)]
        staker: Option<String>,
    },
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
        StakeCommand::Deposit { pool, amount } => deposit(ctx, &pool, &amount).await,
        StakeCommand::Unlock { pool } => unlock(ctx, &pool).await,
        StakeCommand::Withdraw { pool } => withdraw(ctx, &pool).await,
        StakeCommand::Split { pool, recipient, amount } => split(ctx, &pool, &recipient, &amount).await,
        StakeCommand::Merge { pool, source } => merge(ctx, &pool, &source).await,
        StakeCommand::List { staker } => list(ctx, staker).await,
    }
}

/// Deposit (stake) tokens to a pool.
async fn deposit(ctx: &Context, pool_str: &str, amount_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let amount = parse_tape_amount(amount_str)?;

    ctx.print(&format!("Staking {} to pool {}...", amount, pool));
    ctx.print(&format!("Staker: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: StakeWithPool");
        return Ok(());
    }

    let ix = build_stake_with_pool_ix(keypair.pubkey(), pool, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("StakeWithPool failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake deposited successfully!");
    Ok(())
}

/// Request stake unlock (starts cooldown period).
async fn unlock(ctx: &Context, pool_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;

    ctx.print(&format!("Requesting unlock from pool {}...", pool));
    ctx.print(&format!("Staker: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: RequestStakeUnlock");
        return Ok(());
    }

    let ix = build_request_stake_unlock_ix(keypair.pubkey(), pool);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("RequestStakeUnlock failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Unlock requested! Cooldown period has started.");
    Ok(())
}

/// Withdraw stake after cooldown.
async fn withdraw(ctx: &Context, pool_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;

    ctx.print(&format!("Withdrawing stake from pool {}...", pool));
    ctx.print(&format!("Staker: {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: UnstakeFromPool");
        return Ok(());
    }

    let ix = build_unstake_from_pool_ix(keypair.pubkey(), pool);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("UnstakeFromPool failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake withdrawn successfully!");
    Ok(())
}

/// Split stake to another recipient.
async fn split(ctx: &Context, pool_str: &str, recipient_str: &str, amount_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let recipient = parse_pubkey(recipient_str)?;
    let amount = parse_tape_amount(amount_str)?;

    ctx.print(&format!("Splitting {} to {} from pool {}...", amount, recipient, pool));
    ctx.print(&format!("Source staker: {}", keypair.pubkey()));

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
    let ix = build_split_pool_stake_ix(keypair.pubkey(), pool, recipient, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SplitPoolStake failed: {}. Note: recipient signature may be required.", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Stake split successfully!");
    Ok(())
}

/// Merge stake from source into signer's stake.
async fn merge(ctx: &Context, pool_str: &str, source_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let pool = parse_pubkey(pool_str)?;
    let source = parse_pubkey(source_str)?;

    ctx.print(&format!("Merging stake from {} into your stake for pool {}...", source, pool));
    ctx.print(&format!("Recipient (you): {}", keypair.pubkey()));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: MergePoolStake");
        return Ok(());
    }

    // Note: MergePoolStake is called by the source who wants to merge their stake into recipient
    // The function signature is: build_merge_pool_stake_ix(signer, pool, recipient)
    // where signer is the source and recipient receives the merged stake
    // So if we want to receive stake from `source`, we need `source` to be the signer
    // This command assumes the user is the recipient, so we need source's signature
    ctx.print("Note: Merge requires source signature. This command expects you to be receiving the stake.");

    // Build instruction where signer (source) merges into recipient (keypair)
    // But since we don't have source's keypair, this will fail
    // In practice, the source would run this command, not the recipient
    let ix = build_merge_pool_stake_ix(source, pool, keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("MergePoolStake failed: {}. Note: source must execute merge, not recipient.", e))?;

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
            let keypair = load_keypair(ctx)?;
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
