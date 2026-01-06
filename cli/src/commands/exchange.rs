//! Token exchange commands.

use std::str::FromStr;

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use comfy_table::{presets::UTF8_FULL, Table};
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

use crate::output::OutputFormat;
use crate::utils::{get_keypair, resolve_authority, authority_keys_dir, save_exchange_keypair, load_keypair_from_path, AuthorityType};
use crate::Context;

/// Exchange subcommand arguments.
#[derive(Args, Debug)]
pub struct ExchangeArgs {
    #[command(subcommand)]
    pub command: ExchangeCommand,
}

#[derive(Subcommand, Debug)]
pub enum ExchangeCommand {
    /// Create a new exchange. Generates new keypair automatically.
    Register,

    /// Set exchange rate (TAPE per SOL ratio).
    SetRate {
        /// Exchange account address.
        exchange: String,

        /// TAPE amount in the ratio (e.g., for 1000 TAPE per 1 SOL, use --tape 1000 --sol 1).
        #[arg(long)]
        tape: u64,

        /// SOL amount in the ratio.
        #[arg(long)]
        sol: u64,
    },

    /// Deposit TAPE into your exchange.
    DepositTape {
        /// Exchange account address.
        exchange: String,

        /// Amount in TAPE (e.g., "1000.5").
        amount: String,
    },

    /// Deposit SOL into your exchange.
    DepositSol {
        /// Exchange account address.
        exchange: String,

        /// Amount in SOL (e.g., "1.5").
        amount: String,
    },

    /// Withdraw TAPE from your exchange.
    WithdrawTape {
        /// Exchange account address.
        exchange: String,

        /// Amount in TAPE.
        amount: String,
    },

    /// Withdraw SOL from your exchange.
    WithdrawSol {
        /// Exchange account address.
        exchange: String,

        /// Amount in SOL.
        amount: String,
    },

    /// Swap SOL for TAPE at an exchange.
    SwapForTape {
        /// Amount of SOL to swap (e.g., "0.5").
        amount_sol: String,

        /// Exchange account address to swap at.
        #[arg(long)]
        exchange: String,
    },

    /// Swap TAPE for SOL at an exchange.
    SwapForSol {
        /// Amount of TAPE to swap (e.g., "100.0").
        amount_tape: String,

        /// Exchange account address to swap at.
        #[arg(long)]
        exchange: String,
    },

    /// List all saved exchanges.
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

/// Parse a SOL amount string.
fn parse_sol_amount(s: &str) -> Result<SOL> {
    SOL::parse(s).map_err(|_| anyhow::anyhow!("Invalid SOL amount '{}'. Use format like '1.5' or '10'", s))
}

pub async fn execute(ctx: &Context, args: ExchangeArgs) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match args.command {
        ExchangeCommand::Register => register(ctx).await,
        ExchangeCommand::SetRate { exchange, tape, sol } => set_rate(ctx, &exchange, tape, sol).await,
        ExchangeCommand::DepositTape { exchange, amount } => deposit_tape(ctx, &exchange, &amount).await,
        ExchangeCommand::DepositSol { exchange, amount } => deposit_sol(ctx, &exchange, &amount).await,
        ExchangeCommand::WithdrawTape { exchange, amount } => withdraw_tape(ctx, &exchange, &amount).await,
        ExchangeCommand::WithdrawSol { exchange, amount } => withdraw_sol(ctx, &exchange, &amount).await,
        ExchangeCommand::SwapForTape { amount_sol, exchange } => {
            swap_for_tape(ctx, &amount_sol, &exchange).await
        }
        ExchangeCommand::SwapForSol { amount_tape, exchange } => {
            swap_for_sol(ctx, &amount_tape, &exchange).await
        }
        ExchangeCommand::List => list(ctx).await,
    }
}

/// Register a new exchange.
async fn register(ctx: &Context) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    // Generate a new unique keypair for this exchange
    let authority_keypair = Keypair::new();
    let authority = authority_keypair.pubkey();
    let (exchange_address, _) = exchange_pda(authority);

    ctx.print("Creating new exchange...");
    ctx.print(&format!("Fee payer: {}", fee_payer.pubkey()));
    ctx.print(&format!("Exchange: {} (new)", exchange_address));

    if ctx.dry_run {
        ctx.print("[DRY RUN] Would execute: RegisterExchange");
        ctx.print(&format!("[DRY RUN] Would create exchange: {}", exchange_address));
        return Ok(());
    }

    // Register exchange instruction (fee_payer pays, authority signs and owns)
    let ix = build_register_exchange_ix(fee_payer.pubkey(), authority);

    let sig = client
        .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
        .await
        .map_err(|e| anyhow::anyhow!("RegisterExchange failed: {}", e))?;

    // Save the new keypair (indexed by exchange address)
    let (_, keypair_path) = save_exchange_keypair(&authority_keypair)?;

    ctx.print(&format!("Transaction: {}", sig));
    ctx.print("Exchange created successfully!");
    ctx.print(&format!("Exchange: {}", exchange_address));
    ctx.print(&format!("Keypair saved: {}", keypair_path.display()));

    Ok(())
}

/// Set exchange rate.
async fn set_rate(ctx: &Context, exchange_address_str: &str, tape: u64, sol: u64) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;

    if sol == 0 {
        anyhow::bail!("SOL amount cannot be zero");
    }

    // Resolve authority keypair from exchange address
    let authority_keypair = resolve_authority(exchange_address_str, AuthorityType::Exchange)?;
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
async fn deposit_tape(ctx: &Context, exchange_address_str: &str, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Resolve authority keypair from exchange address
    let authority_keypair = resolve_authority(exchange_address_str, AuthorityType::Exchange)?;
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
async fn deposit_sol(ctx: &Context, exchange_address_str: &str, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Resolve authority keypair from exchange address
    let authority_keypair = resolve_authority(exchange_address_str, AuthorityType::Exchange)?;
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
async fn withdraw_tape(ctx: &Context, exchange_address_str: &str, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Resolve authority keypair from exchange address
    let authority_keypair = resolve_authority(exchange_address_str, AuthorityType::Exchange)?;
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
async fn withdraw_sol(ctx: &Context, exchange_address_str: &str, amount_str: &str) -> Result<()> {
    let fee_payer = get_keypair(ctx)?;
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Resolve authority keypair from exchange address
    let authority_keypair = resolve_authority(exchange_address_str, AuthorityType::Exchange)?;
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
async fn swap_for_tape(ctx: &Context, amount_str: &str, exchange_address_str: &str) -> Result<()> {
    // For swaps, fee_payer is the signer (person swapping)
    let fee_payer = get_keypair(ctx)?;
    let signer = fee_payer.pubkey();
    let client = create_client(ctx)?;
    let amount = parse_sol_amount(amount_str)?;

    // Parse exchange address
    let exchange_address = parse_pubkey(exchange_address_str)?;
    let signer_ata = ata(&signer);

    ctx.print(&format!("Swapping {} for TAPE...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));
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
async fn swap_for_sol(ctx: &Context, amount_str: &str, exchange_address_str: &str) -> Result<()> {
    // For swaps, fee_payer is the signer (person swapping)
    let fee_payer = get_keypair(ctx)?;
    let signer = fee_payer.pubkey();
    let client = create_client(ctx)?;
    let amount = parse_tape_amount(amount_str)?;

    // Parse exchange address
    let exchange_address = parse_pubkey(exchange_address_str)?;
    let signer_ata = ata(&signer);

    ctx.print(&format!("Swapping {} for SOL...", amount));
    ctx.print(&format!("Exchange: {}", exchange_address));
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

/// Fetch exchange account by authority
async fn fetch_exchange(ctx: &Context, authority: &Pubkey) -> Result<Option<tape_api::state::Exchange>> {
    use rpc_solana::{Rpc, RpcConfig as SolanaRpcConfig, SolanaRpc};
    use tape_api::state::Exchange;

    let config = SolanaRpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    let rpc = SolanaRpc::new(config).map_err(|e| anyhow::anyhow!("Failed to create RPC: {}", e))?;

    let (address, _) = exchange_pda(*authority);
    match rpc.get_account(&address).await {
        Ok(account) => {
            let exchange = Exchange::unpack_with_discriminator(&account.data)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize exchange: {}", e))?;
            Ok(Some(exchange.clone()))
        }
        Err(rpc_solana::RpcError::AccountNotFound(_)) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("RPC error: {}", e)),
    }
}

/// List all saved exchanges.
async fn list(ctx: &Context) -> Result<()> {
    // Collect exchanges to display: (exchange_address, exchange_data)
    let mut exchanges: Vec<(Pubkey, tape_api::state::Exchange)> = Vec::new();
    let mut not_found: Vec<Pubkey> = Vec::new();

    // List all saved exchange keypairs (filenames are exchange addresses)
    let exchanges_dir = authority_keys_dir(AuthorityType::Exchange);

    if !exchanges_dir.exists() {
        match ctx.output {
            OutputFormat::Json => println!("[]"),
            _ => {
                println!("No exchanges found.");
                println!("Use `tape exchange register` to create one.");
            }
        }
        return Ok(());
    }

    let entries: Vec<_> = std::fs::read_dir(&exchanges_dir)
        .with_context(|| format!("Failed to read exchanges directory: {}", exchanges_dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .collect();

    if entries.is_empty() {
        match ctx.output {
            OutputFormat::Json => println!("[]"),
            _ => {
                println!("No exchanges found.");
                println!("Use `tape exchange register` to create one.");
            }
        }
        return Ok(());
    }

    for entry in entries {
        let path = entry.path();
        let filename = entry.file_name();
        let exchange_address_str = filename.to_string_lossy();
        let exchange_address_str = exchange_address_str.trim_end_matches(".json");

        // Parse exchange address from filename
        let exchange_address: Pubkey = match exchange_address_str.parse() {
            Ok(pk) => pk,
            Err(_) => continue,
        };

        // Load keypair to get authority
        let keypair = match load_keypair_from_path(&path.to_string_lossy()) {
            Ok(kp) => kp,
            Err(_) => continue,
        };
        let authority = keypair.pubkey();

        // Fetch exchange using authority
        match fetch_exchange(ctx, &authority).await {
            Ok(Some(exchange)) => exchanges.push((exchange_address, exchange)),
            Ok(None) => not_found.push(exchange_address),
            Err(_) => not_found.push(exchange_address),
        }
    }

    // Output based on format
    match ctx.output {
        OutputFormat::Json => {
            let json_exchanges: Vec<_> = exchanges.iter().map(|(exchange_address, exchange)| {
                let rate = if exchange.rate.other > 0 {
                    (exchange.rate.tape as f64) / (exchange.rate.other as f64)
                } else {
                    0.0
                };
                serde_json::json!({
                    "address": exchange_address.to_string(),
                    "balance_tape": exchange.balance_tape.as_u64(),
                    "balance_sol": exchange.balance_sol.as_u64(),
                    "rate_tape": exchange.rate.tape,
                    "rate_sol": exchange.rate.other,
                    "rate": rate,
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&json_exchanges)?);
        }
        _ => {
            if exchanges.is_empty() && not_found.is_empty() {
                println!("No exchanges found.");
                println!("Use `tape exchange register` to create one.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Exchange", "TAPE Balance", "SOL Balance", "Rate (TAPE/SOL)"]);

            for (exchange_address, exchange) in &exchanges {
                let rate = if exchange.rate.other > 0 {
                    format!("{:.2}", (exchange.rate.tape as f64) / (exchange.rate.other as f64))
                } else {
                    "N/A".to_string()
                };
                table.add_row(vec![
                    &exchange_address.to_string(),
                    &format!("{}", exchange.balance_tape),
                    &format!("{}", exchange.balance_sol),
                    &rate,
                ]);
            }

            for exchange_address in &not_found {
                table.add_row(vec![
                    &exchange_address.to_string(),
                    "(not found on-chain)",
                    "",
                    "",
                ]);
            }

            println!("{}", table);
            println!("\nTotal: {} exchange(s)", exchanges.len());
        }
    }

    Ok(())
}
