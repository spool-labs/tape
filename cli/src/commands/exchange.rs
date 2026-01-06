//! Token exchange commands.

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
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

use crate::utils::{get_keypair, resolve_authority, authority_keys_dir, AuthorityType};
use crate::Context;

/// Save a keypair to the exchanges keys directory.
fn save_exchange_keypair(keypair: &Keypair) -> Result<PathBuf> {
    let dir = authority_keys_dir(AuthorityType::Exchange);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create exchanges keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", keypair.pubkey()));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write exchange keypair to {}", path.display()))?;

    Ok(path)
}

/// Exchange subcommand arguments with global authority flag.
#[derive(Args, Debug)]
pub struct ExchangeArgs {
    /// Exchange authority keypair: path to file OR pubkey (resolves to ~/.tape/keys/exchanges/{pubkey}.json).
    /// If not specified, uses --keypair as the authority.
    #[arg(long, short = 'a', global = true)]
    pub authority: Option<String>,

    #[command(subcommand)]
    pub command: ExchangeCommand,
}

#[derive(Subcommand, Debug)]
pub enum ExchangeCommand {
    /// Create a new exchange.
    /// If no authority is specified, generates a new keypair.
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

        /// Exchange authority pubkey to swap at (uses your own exchange if not specified).
        #[arg(long)]
        exchange: Option<String>,
    },

    /// Swap TAPE for SOL at an exchange.
    SwapForSol {
        /// Amount of TAPE to swap (e.g., "100.0").
        amount_tape: String,

        /// Exchange authority pubkey to swap at (uses your own exchange if not specified).
        #[arg(long)]
        exchange: Option<String>,
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

/// Parse a SOL amount string.
fn parse_sol_amount(s: &str) -> Result<SOL> {
    SOL::parse(s).map_err(|_| anyhow::anyhow!("Invalid SOL amount '{}'. Use format like '1.5' or '10'", s))
}

pub async fn execute(ctx: &Context, args: ExchangeArgs) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match args.command {
        ExchangeCommand::Register => register(ctx, args.authority).await,
        ExchangeCommand::SetRate { tape, sol } => set_rate(ctx, args.authority, tape, sol).await,
        ExchangeCommand::DepositTape { amount } => deposit_tape(ctx, args.authority, &amount).await,
        ExchangeCommand::DepositSol { amount } => deposit_sol(ctx, args.authority, &amount).await,
        ExchangeCommand::WithdrawTape { amount } => withdraw_tape(ctx, args.authority, &amount).await,
        ExchangeCommand::WithdrawSol { amount } => withdraw_sol(ctx, args.authority, &amount).await,
        ExchangeCommand::SwapForTape { amount_sol, exchange } => {
            swap_for_tape(ctx, &amount_sol, exchange).await
        }
        ExchangeCommand::SwapForSol { amount_tape, exchange } => {
            swap_for_sol(ctx, &amount_tape, exchange).await
        }
    }
}

/// Register a new exchange.
async fn register(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    // Determine authority: resolve from arg, or generate new one
    let (authority_keypair, is_new_keypair) = match authority_arg {
        Some(auth) => {
            let kp = resolve_authority(&auth, AuthorityType::Exchange)?;
            (kp, false)
        }
        None => {
            // Generate a new unique keypair for this exchange
            (Keypair::new(), true)
        }
    };

    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);

    ctx.print("Creating new exchange...");
    ctx.print(&format!("Fee payer: {}", fee_payer.pubkey()));
    ctx.print(&format!("Authority: {}{}", authority, if is_new_keypair { " (new)" } else { "" }));
    ctx.print(&format!("Exchange PDA: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: RegisterExchange");
        if is_new_keypair {
            ctx.print(&format!("[DRY RUN] Would generate new authority: {}", authority));
        }
        return Ok(());
    }

    // Register exchange instruction (fee_payer pays, authority signs and owns)
    let ix = build_register_exchange_ix(fee_payer.pubkey(), authority);

    // Send with both signers if using new keypair or different authority
    let sig = if is_new_keypair || fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("RegisterExchange failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("RegisterExchange failed: {}", e))?
    };

    // Save the new keypair
    let keypair_path = if is_new_keypair {
        let path = save_exchange_keypair(&authority_keypair)?;
        Some(path)
    } else {
        None
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Exchange created successfully!");

    if let Some(path) = keypair_path {
        ctx.print(&format!("Keypair saved: {}", path.display()));
    }

    Ok(())
}

/// Set exchange rate.
async fn set_rate(ctx: &Context, authority_arg: Option<String>, tape: u64, sol: u64) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    if sol == 0 {
        anyhow::bail!("SOL amount cannot be zero");
    }

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Exchange)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);
    let rate = (tape as f64) / (sol as f64);

    ctx.print(&format!("Setting exchange rate: {} TAPE per {} SOL (ratio: {:.6})", tape, sol, rate));
    ctx.print(&format!("Exchange: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SetExchangeRate");
        return Ok(());
    }

    let ix = build_set_exchange_rate_ix(fee_payer.pubkey(), authority, exchange_address, tape, sol);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("SetExchangeRate failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("SetExchangeRate failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Exchange rate updated successfully!");
    Ok(())
}

/// Deposit TAPE into exchange.
async fn deposit_tape(ctx: &Context, authority_arg: Option<String>, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Exchange)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);
    let authority_ata = ata(&authority);

    ctx.print(&format!("Depositing {} into exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));
    ctx.print(&format!("From ATA: {}", authority_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: DepositTape");
        return Ok(());
    }

    let ix = build_deposit_tape_ix(fee_payer.pubkey(), authority, authority_ata, exchange_address, amount);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("DepositTape failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("DepositTape failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("TAPE deposited successfully!");
    Ok(())
}

/// Deposit SOL into exchange.
async fn deposit_sol(ctx: &Context, authority_arg: Option<String>, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Exchange)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);

    ctx.print(&format!("Depositing {} into exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: DepositSol");
        return Ok(());
    }

    let ix = build_deposit_sol_ix(fee_payer.pubkey(), authority, exchange_address, amount);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("DepositSol failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("DepositSol failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("SOL deposited successfully!");
    Ok(())
}

/// Withdraw TAPE from exchange.
async fn withdraw_tape(ctx: &Context, authority_arg: Option<String>, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Exchange)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);
    let authority_ata = ata(&authority);

    ctx.print(&format!("Withdrawing {} from exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));
    ctx.print(&format!("To ATA: {}", authority_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: WithdrawTape");
        return Ok(());
    }

    let ix = build_withdraw_tape_ix(fee_payer.pubkey(), authority, authority_ata, exchange_address, amount);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("WithdrawTape failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("WithdrawTape failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("TAPE withdrawn successfully!");
    Ok(())
}

/// Withdraw SOL from exchange.
async fn withdraw_sol(ctx: &Context, authority_arg: Option<String>, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Exchange)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);

    ctx.print(&format!("Withdrawing {} from exchange...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: WithdrawSol");
        return Ok(());
    }

    let ix = build_withdraw_sol_ix(fee_payer.pubkey(), authority, exchange_address, amount);

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("WithdrawSol failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("WithdrawSol failed: {}", e))?
    };

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("SOL withdrawn successfully!");
    Ok(())
}

/// Swap SOL for TAPE at an exchange.
async fn swap_for_tape(ctx: &Context, amount_str: &str, exchange_authority: Option<String>) -> Result<()> {
    // For swaps, fee_payer is the signer (person swapping)
    let fee_payer = get_keypair(ctx)?;
    let signer = fee_payer.pubkey();
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Determine which exchange to use
    let exchange_auth = match exchange_authority {
        Some(s) => parse_pubkey(&s)?,
        None => signer, // Use own exchange if not specified
    };

    let (exchange_address, _) = exchange_pda(exchange_auth);
    let signer_ata = ata(&signer);

    ctx.print(&format!("Swapping {} for TAPE...", amount));
    ctx.print(&format!("Exchange: {} (authority: {})", exchange_address, exchange_auth));
    ctx.print(&format!("Receiving TAPE at: {}", signer_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SwapForTape");
        return Ok(());
    }

    let ix = build_swap_for_tape_ix(fee_payer.pubkey(), signer, signer_ata, exchange_address, amount);
    let sig = client
        .send_instructions(&fee_payer, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SwapForTape failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Swap completed successfully!");
    Ok(())
}

/// Swap TAPE for SOL at an exchange.
async fn swap_for_sol(ctx: &Context, amount_str: &str, exchange_authority: Option<String>) -> Result<()> {
    // For swaps, fee_payer is the signer (person swapping)
    let fee_payer = get_keypair(ctx)?;
    let signer = fee_payer.pubkey();
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Determine which exchange to use
    let exchange_auth = match exchange_authority {
        Some(s) => parse_pubkey(&s)?,
        None => signer, // Use own exchange if not specified
    };

    let (exchange_address, _) = exchange_pda(exchange_auth);
    let signer_ata = ata(&signer);

    ctx.print(&format!("Swapping {} for SOL...", amount));
    ctx.print(&format!("Exchange: {} (authority: {})", exchange_address, exchange_auth));
    ctx.print(&format!("Sending TAPE from: {}", signer_ata));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: SwapForSol");
        return Ok(());
    }

    let ix = build_swap_for_sol_ix(fee_payer.pubkey(), signer, signer_ata, exchange_address, amount);
    let sig = client
        .send_instructions(&fee_payer, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("SwapForSol failed: {}", e))?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Swap completed successfully!");
    Ok(())
}
