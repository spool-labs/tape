//! Storage resource (tape) management commands.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::pubkey::Pubkey;

use tape_api::helpers::build_authority_ix;
use tape_sdk::{load_solana_keypair, create_rpc_client};

use crate::config::expand_path;
use crate::Context;

/// Default lamports to fund new authority accounts (0.01 SOL).
/// Covers Tape account rent (~0.001 SOL) plus buffer for transaction fees.
const AUTHORITY_FUND_LAMPORTS: u64 = 10_000_000;

/// Directory for tape keypairs.
fn tapes_keys_dir() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("keys").join("tapes"))
        .unwrap_or_else(|| PathBuf::from(".tape/keys/tapes"))
}

/// Save a keypair to the tapes keys directory.
fn save_tape_keypair(keypair: &Keypair) -> Result<PathBuf> {
    let dir = tapes_keys_dir();
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create tapes keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", keypair.pubkey()));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write tape keypair to {}", path.display()))?;

    Ok(path)
}

#[derive(Subcommand, Debug)]
pub enum TapeCommand {
    /// Reserve storage capacity (buy tape).
    Reserve {
        /// Storage units (MB).
        #[arg(long)]
        size: u64,

        /// Activation epoch.
        #[arg(long)]
        start_epoch: u64,

        /// Expiry epoch.
        #[arg(long)]
        end_epoch: u64,

        /// Path to existing authority keypair (generates new if not specified).
        #[arg(long)]
        authority: Option<PathBuf>,
    },

    /// Destroy tape and reclaim rent.
    Destroy {
        /// Tape authority pubkey (uses keypair if not specified).
        #[arg(long)]
        tape: Option<String>,
    },

    /// Split tape by epoch or size.
    Split {
        /// Recipient pubkey for the split portion.
        recipient: String,

        /// Split at epoch (creates new tape from this epoch onwards).
        #[arg(long, conflicts_with = "at_size")]
        at_epoch: Option<u64>,

        /// Split at size in MB (creates new tape with this capacity).
        #[arg(long, conflicts_with = "at_epoch")]
        at_size: Option<u64>,
    },

    /// Merge tapes (combine two tapes into one).
    Merge {
        /// Recipient authority pubkey (tape to merge into).
        recipient: String,
    },

    /// List user's tapes.
    List {
        /// Authority pubkey (uses keypair if not specified).
        #[arg(long)]
        authority: Option<String>,
    },

    /// Show tape details.
    Show {
        /// Tape authority pubkey (uses keypair if not specified).
        #[arg(long)]
        authority: Option<String>,
    },
}

pub async fn execute(ctx: &Context, cmd: TapeCommand) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match cmd {
        TapeCommand::Reserve { size, start_epoch, end_epoch, authority } => {
            reserve(ctx, size, start_epoch, end_epoch, authority).await
        }
        TapeCommand::Destroy { tape } => {
            destroy(ctx, tape).await
        }
        TapeCommand::Split { recipient, at_epoch, at_size } => {
            split(ctx, &recipient, at_epoch, at_size).await
        }
        TapeCommand::Merge { recipient } => {
            merge(ctx, &recipient).await
        }
        TapeCommand::List { authority } => {
            list(ctx, authority).await
        }
        TapeCommand::Show { authority } => {
            show(ctx, authority).await
        }
    }
}

/// Load keypair from context's configured path.
fn get_keypair(ctx: &Context) -> Result<solana_sdk::signature::Keypair> {
    let path = ctx.keypair.as_ref()
        .ok_or_else(|| anyhow::anyhow!("No keypair configured. Use --keypair or set keys.default in config."))?;

    let expanded = expand_path(&path.to_string_lossy());
    load_solana_keypair(&expanded)
        .map_err(|e| anyhow::anyhow!("{}", e))
}

async fn reserve(
    ctx: &Context,
    size: u64,
    start_epoch: u64,
    end_epoch: u64,
    authority_path: Option<PathBuf>,
) -> Result<()> {
    use tape_api::instruction::build_reserve_tape_ix;
    use tape_core::types::{EpochNumber, StorageUnits};

    if end_epoch <= start_epoch {
        anyhow::bail!("End epoch must be greater than start epoch");
    }

    // Load the fee payer keypair (from --keypair or config)
    let fee_payer = get_keypair(ctx)?;

    // Determine authority: use provided keypair or generate new one
    let (authority_keypair, is_new_keypair) = match authority_path {
        Some(path) => {
            let expanded = expand_path(&path.to_string_lossy());
            let kp = load_solana_keypair(&expanded)
                .map_err(|e| anyhow::anyhow!("Failed to load authority keypair: {}", e))?;
            (kp, false)
        }
        None => {
            // Generate a new unique keypair for this tape
            (Keypair::new(), true)
        }
    };

    let authority = authority_keypair.pubkey();

    if !ctx.quiet {
        eprintln!("Reserving tape:");
        eprintln!("  Fee payer: {}", fee_payer.pubkey());
        eprintln!("  Authority: {}{}", authority, if is_new_keypair { " (new)" } else { "" });
        eprintln!("  Size: {} MB", size);
        eprintln!("  Start epoch: {}", start_epoch);
        eprintln!("  End epoch: {}", end_epoch);
    }

    if ctx.dry_run {
        println!("Dry run - would reserve {} MB from epoch {} to {}", size, start_epoch, end_epoch);
        if is_new_keypair {
            println!("  Would generate new authority: {}", authority);
        }
        return Ok(());
    }

    // Build instructions
    let mut instructions = Vec::new();

    // If using a new keypair, fund it first with SOL for rent
    if is_new_keypair {
        instructions.push(build_authority_ix(
            fee_payer.pubkey(),
            authority,
            AUTHORITY_FUND_LAMPORTS,
        ));
    }

    // Reserve tape instruction (authority is the signer)
    instructions.push(build_reserve_tape_ix(
        authority,
        StorageUnits(size),
        EpochNumber(start_epoch),
        EpochNumber(end_epoch),
    ));

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Send with both signers if using new keypair
    let signature = if is_new_keypair || fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, instructions, &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, instructions)
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    // Save the new keypair
    let keypair_path = if is_new_keypair {
        let path = save_tape_keypair(&authority_keypair)?;
        Some(path)
    } else {
        None
    };

    println!("Tape reserved successfully!");
    println!("  Transaction: {}", signature);
    println!("  Authority: {}", authority);
    println!("  Size: {} MB", size);
    println!("  Active: epoch {} to {}", start_epoch, end_epoch);

    if let Some(path) = keypair_path {
        println!("  Keypair saved: {}", path.display());
    }

    Ok(())
}

async fn destroy(ctx: &Context, tape: Option<String>) -> Result<()> {
    use tape_api::instruction::build_destroy_tape_ix;

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

    // If tape authority specified, it must match the signer
    if let Some(ref tape_auth) = tape {
        let tape_pubkey: Pubkey = tape_auth.parse()
            .with_context(|| format!("Invalid tape authority pubkey: {}", tape_auth))?;
        if tape_pubkey != signer {
            anyhow::bail!("Tape authority {} does not match signer {}. Only the tape owner can destroy it.", tape_auth, signer);
        }
    }

    if !ctx.quiet {
        eprintln!("Destroying tape for authority: {}", signer);
    }

    if ctx.dry_run {
        println!("Dry run - would destroy tape for authority {}", signer);
        return Ok(());
    }

    let ix = build_destroy_tape_ix(signer);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Tape destroyed successfully!");
    println!("  Transaction: {}", signature);
    println!("  Authority: {}", signer);

    Ok(())
}

async fn split(
    ctx: &Context,
    recipient: &str,
    at_epoch: Option<u64>,
    at_size: Option<u64>,
) -> Result<()> {
    use tape_api::instruction::{build_split_tape_by_epoch_ix, build_split_tape_by_size_ix};
    use tape_core::types::{EpochNumber, StorageUnits};

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

    let recipient_pubkey: Pubkey = recipient.parse()
        .with_context(|| format!("Invalid recipient pubkey: {}", recipient))?;

    if at_epoch.is_none() && at_size.is_none() {
        anyhow::bail!("Must specify either --at-epoch or --at-size");
    }

    let ix = if let Some(epoch) = at_epoch {
        if !ctx.quiet {
            eprintln!("Splitting tape at epoch {}", epoch);
            eprintln!("  Source: {}", signer);
            eprintln!("  Recipient: {}", recipient_pubkey);
        }

        if ctx.dry_run {
            println!("Dry run - would split tape at epoch {}", epoch);
            return Ok(());
        }

        build_split_tape_by_epoch_ix(signer, recipient_pubkey, EpochNumber(epoch))
    } else if let Some(size) = at_size {
        if !ctx.quiet {
            eprintln!("Splitting tape at size {} MB", size);
            eprintln!("  Source: {}", signer);
            eprintln!("  Recipient: {}", recipient_pubkey);
        }

        if ctx.dry_run {
            println!("Dry run - would split tape at size {} MB", size);
            return Ok(());
        }

        build_split_tape_by_size_ix(signer, recipient_pubkey, StorageUnits(size))
    } else {
        unreachable!()
    };

    // Note: The recipient needs to sign too for the split instruction
    // For now, we assume the recipient keypair is also available or this is a self-split
    // In a real implementation, you might need to pass the recipient keypair separately

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // If recipient is different from signer, we need recipient to sign
    // This is a limitation - in production you'd need multi-party signing
    if recipient_pubkey != signer {
        anyhow::bail!(
            "Split requires recipient ({}) to sign. Multi-party signing not yet supported in CLI. \
             For self-split, use your own pubkey as recipient.",
            recipient_pubkey
        );
    }

    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Tape split successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source: {}", signer);
    println!("  Recipient: {}", recipient_pubkey);

    Ok(())
}

async fn merge(ctx: &Context, recipient: &str) -> Result<()> {
    use tape_api::instruction::build_merge_tape_ix;

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

    let recipient_pubkey: Pubkey = recipient.parse()
        .with_context(|| format!("Invalid recipient pubkey: {}", recipient))?;

    if !ctx.quiet {
        eprintln!("Merging tape:");
        eprintln!("  Source: {}", signer);
        eprintln!("  Destination: {}", recipient_pubkey);
    }

    if ctx.dry_run {
        println!("Dry run - would merge tape {} into {}", signer, recipient_pubkey);
        return Ok(());
    }

    // Note: The recipient needs to sign too for the merge instruction
    if recipient_pubkey != signer {
        anyhow::bail!(
            "Merge requires recipient ({}) to sign. Multi-party signing not yet supported in CLI. \
             For self-merge, use your own pubkey as recipient.",
            recipient_pubkey
        );
    }

    let ix = build_merge_tape_ix(signer, recipient_pubkey);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Tapes merged successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source (closed): {}", signer);
    println!("  Destination: {}", recipient_pubkey);

    Ok(())
}

async fn list(ctx: &Context, authority: Option<String>) -> Result<()> {
    let authority_pubkey = match authority {
        Some(auth) => auth.parse()
            .with_context(|| format!("Invalid authority pubkey: {}", auth))?,
        None => {
            let keypair = get_keypair(ctx)?;
            keypair.pubkey()
        }
    };

    if !ctx.quiet {
        eprintln!("Listing tapes for authority: {}", authority_pubkey);
    }

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Get the tape for this authority
    match client.get_tape(&authority_pubkey).await {
        Ok(tape) => {
            println!("{:<20} {:>12} {:>12} {:>12} {:>12} {:>8}",
                "Authority", "ID", "Capacity", "Used", "Epochs", "Tracks");
            println!("{}", "-".repeat(80));

            let epoch_range = format!("{}-{}", tape.active_epoch, tape.expiry_epoch);
            println!("{:<20} {:>12} {:>12} {:>12} {:>12} {:>8}",
                &authority_pubkey.to_string()[..20],
                tape.id,
                format!("{} MB", tape.capacity),
                format!("{} MB", tape.used),
                epoch_range,
                tape.track_count
            );
        }
        Err(e) => {
            if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                println!("No tape found for authority: {}", authority_pubkey);
                println!("Use `tape tape reserve` to create one.");
            } else {
                return Err(anyhow::anyhow!("Failed to fetch tape: {}", e));
            }
        }
    }

    Ok(())
}

async fn show(ctx: &Context, authority: Option<String>) -> Result<()> {
    use tape_api::program::tapedrive::tape_pda;

    let authority_pubkey: Pubkey = match authority {
        Some(auth) => auth.parse()
            .with_context(|| format!("Invalid authority pubkey: {}", auth))?,
        None => {
            let keypair = get_keypair(ctx)?;
            keypair.pubkey()
        }
    };

    let (tape_address, _) = tape_pda(authority_pubkey);

    if !ctx.quiet {
        eprintln!("Fetching tape for authority: {}", authority_pubkey);
    }

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    match client.get_tape(&authority_pubkey).await {
        Ok(tape) => {
            println!("Tape Details:");
            println!("  Account: {}", tape_address);
            println!("  Authority: {}", tape.authority);
            println!("  ID: {}", tape.id);
            println!("  Capacity: {} MB", tape.capacity);
            println!("  Used: {} MB", tape.used);
            println!("  Available: {} MB", tape.capacity.as_u64().saturating_sub(tape.used.as_u64()));
            println!("  Active Epoch: {}", tape.active_epoch);
            println!("  Expiry Epoch: {}", tape.expiry_epoch);
            println!("  Track Count: {}", tape.track_count);
        }
        Err(e) => {
            if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                println!("No tape found for authority: {}", authority_pubkey);
                println!("Use `tape tape reserve` to create one.");
            } else {
                return Err(anyhow::anyhow!("Failed to fetch tape: {}", e));
            }
        }
    }

    Ok(())
}
