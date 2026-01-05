//! Token exchange commands.

use std::str::FromStr;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::instruction::{
    build_register_exchange_ix, build_set_exchange_rate_ix,
    build_deposit_tape_ix, build_deposit_sol_ix,
    build_withdraw_tape_ix, build_withdraw_sol_ix,
    build_swap_for_tape_ix, build_swap_for_sol_ix,
};
use tape_api::program::exchange::exchange_pda;
use tape_api::utils::ata;
use rpc_client::{RpcConfig, RpcClient};
use tape_core::types::coin::{TAPE, SOL};

use crate::config::expand_path;
use crate::Context;

#[derive(Subcommand, Debug)]
pub enum ExchangeCommand {
    /// Create a new exchange.
    Register,

    /// Set exchange rate (TAPE per SOL ratio).
    SetRate {
        /// TAPE amount in the ratio (e.g., for 1000 TAPE per 1 SOL, use --tape 1000 --sol 1).
        #[arg(long)]
        tape: u64,

        /// SOL amount in the ratio.
        #[arg(long)]
        sol: u64,
    },

    /// Deposit TAPE into your exchange.
    DepositTape {
        /// Amount in TAPE (e.g., "1000.5").
        amount: String,
    },

    /// Deposit SOL into your exchange.
    DepositSol {
        /// Amount in SOL (e.g., "1.5").
        amount: String,
    },

    /// Withdraw TAPE from your exchange.
    WithdrawTape {
        /// Amount in TAPE.
        amount: String,
    },

    /// Withdraw SOL from your exchange.
    WithdrawSol {
        /// Amount in SOL.
        amount: String,
    },

    /// Swap SOL for TAPE at an exchange.
    SwapForTape {
        /// Amount of SOL to swap (e.g., "0.5").
        amount_sol: String,

        /// Exchange authority pubkey (uses your own if not specified).
        #[arg(long)]
        exchange: Option<String>,
    },

    /// Swap TAPE for SOL at an exchange.
    SwapForSol {
        /// Amount of TAPE to swap (e.g., "100.0").
        amount_tape: String,

        /// Exchange authority pubkey (uses your own if not specified).
        #[arg(long)]
        exchange: Option<String>,
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

/// Parse a SOL amount string.
fn parse_sol_amount(s: &str) -> Result<SOL> {
    SOL::parse(s).map_err(|_| anyhow::anyhow!("Invalid SOL amount '{}'. Use format like '1.5' or '10'", s))
}

pub async fn execute(ctx: &Context, cmd: ExchangeCommand) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match cmd {
        ExchangeCommand::Register => register(ctx).await,
        ExchangeCommand::SetRate { tape, sol } => set_rate(ctx, tape, sol).await,
        ExchangeCommand::DepositTape { amount } => deposit_tape(ctx, &amount).await,
        ExchangeCommand::DepositSol { amount } => deposit_sol(ctx, &amount).await,
        ExchangeCommand::WithdrawTape { amount } => withdraw_tape(ctx, &amount).await,
        ExchangeCommand::WithdrawSol { amount } => withdraw_sol(ctx, &amount).await,
        ExchangeCommand::SwapForTape { amount_sol, exchange } => {
            swap_for_tape(ctx, &amount_sol, exchange).await
        }
        ExchangeCommand::SwapForSol { amount_tape, exchange } => {
            swap_for_sol(ctx, &amount_tape, exchange).await
        }
    }
}

/// Register a new exchange.
async fn register(ctx: &Context) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    let (exchange_address, _) = exchange_pda(keypair.pubkey());

    ctx.print("Creating new exchange...");
    ctx.print(&format!("Authority: {}", keypair.pubkey()));
    ctx.print(&format!("Exchange PDA: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: RegisterExchange");
        return Ok(());
    }

    let ix = build_register_exchange_ix(keypair.pubkey());
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("RegisterExchange failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Exchange created successfully!");
    Ok(())
}

/// Set exchange rate.
async fn set_rate(ctx: &Context, tape: u64, sol: u64) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;

    if sol == 0 {
        anyhow::bail!("SOL amount cannot be zero");
    }

    let (exchange_address, _) = exchange_pda(keypair.pubkey());
    let rate = (tape as f64) / (sol as f64);

    ctx.print(&format!("Setting exchange rate: {} TAPE per {} SOL (ratio: {:.6})", tape, sol, rate));
    ctx.print(&format!("Exchange: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SetExchangeRate");
        return Ok(());
    }

    let ix = build_set_exchange_rate_ix(keypair.pubkey(), exchange_address, tape, sol);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SetExchangeRate failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Exchange rate updated successfully!");
    Ok(())
}

/// Deposit TAPE into exchange.
async fn deposit_tape(ctx: &Context, amount_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    let (exchange_address, _) = exchange_pda(keypair.pubkey());
    let signer_ata = ata(&keypair.pubkey());

    ctx.print(&format!("Depositing {} into exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));
    ctx.print(&format!("From ATA: {}", signer_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: DepositTape");
        return Ok(());
    }

    let ix = build_deposit_tape_ix(keypair.pubkey(), signer_ata, exchange_address, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("DepositTape failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("TAPE deposited successfully!");
    Ok(())
}

/// Deposit SOL into exchange.
async fn deposit_sol(ctx: &Context, amount_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    let (exchange_address, _) = exchange_pda(keypair.pubkey());

    ctx.print(&format!("Depositing {} into exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: DepositSol");
        return Ok(());
    }

    let ix = build_deposit_sol_ix(keypair.pubkey(), exchange_address, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("DepositSol failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("SOL deposited successfully!");
    Ok(())
}

/// Withdraw TAPE from exchange.
async fn withdraw_tape(ctx: &Context, amount_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    let (exchange_address, _) = exchange_pda(keypair.pubkey());
    let signer_ata = ata(&keypair.pubkey());

    ctx.print(&format!("Withdrawing {} from exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));
    ctx.print(&format!("To ATA: {}", signer_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: WithdrawTape");
        return Ok(());
    }

    let ix = build_withdraw_tape_ix(keypair.pubkey(), signer_ata, exchange_address, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("WithdrawTape failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("TAPE withdrawn successfully!");
    Ok(())
}

/// Withdraw SOL from exchange.
async fn withdraw_sol(ctx: &Context, amount_str: &str) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    let (exchange_address, _) = exchange_pda(keypair.pubkey());

    ctx.print(&format!("Withdrawing {} from exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: WithdrawSol");
        return Ok(());
    }

    let ix = build_withdraw_sol_ix(keypair.pubkey(), exchange_address, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("WithdrawSol failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("SOL withdrawn successfully!");
    Ok(())
}

/// Swap SOL for TAPE at an exchange.
async fn swap_for_tape(ctx: &Context, amount_str: &str, exchange_authority: Option<String>) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Determine which exchange to use
    let exchange_auth = match exchange_authority {
        Some(s) => parse_pubkey(&s)?,
        None => keypair.pubkey(), // Use own exchange if not specified
    };

    let (exchange_address, _) = exchange_pda(exchange_auth);
    let signer_ata = ata(&keypair.pubkey());

    ctx.print(&format!("Swapping {} for TAPE...", amount));
    ctx.print(&format!("Exchange: {} (authority: {})", exchange_address, exchange_auth));
    ctx.print(&format!("Receiving TAPE at: {}", signer_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SwapForTape");
        return Ok(());
    }

    let ix = build_swap_for_tape_ix(keypair.pubkey(), signer_ata, exchange_address, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SwapForTape failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Swap completed successfully!");
    Ok(())
}

/// Swap TAPE for SOL at an exchange.
async fn swap_for_sol(ctx: &Context, amount_str: &str, exchange_authority: Option<String>) -> Result<()> {
    let keypair = load_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Determine which exchange to use
    let exchange_auth = match exchange_authority {
        Some(s) => parse_pubkey(&s)?,
        None => keypair.pubkey(), // Use own exchange if not specified
    };

    let (exchange_address, _) = exchange_pda(exchange_auth);
    let signer_ata = ata(&keypair.pubkey());

    ctx.print(&format!("Swapping {} for SOL...", amount));
    ctx.print(&format!("Exchange: {} (authority: {})", exchange_address, exchange_auth));
    ctx.print(&format!("Sending TAPE from: {}", signer_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SwapForSol");
        return Ok(());
    }

    let ix = build_swap_for_sol_ix(keypair.pubkey(), signer_ata, exchange_address, amount);
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SwapForSol failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Swap completed successfully!");
    Ok(())
}
