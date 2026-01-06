//! Storage resource (tape) management commands.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::pubkey::Pubkey;

use tape_sdk::create_rpc_client;

use crate::utils::{get_keypair, resolve_authority, authority_keys_dir, AuthorityType};
use crate::Context;

/// Save a keypair to the tapes keys directory.
fn save_tape_keypair(keypair: &Keypair) -> Result<PathBuf> {
    let dir = authority_keys_dir(AuthorityType::Tape);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create tapes keys directory: {}", dir.display()))?;

    let path = dir.join(format!("{}.json", keypair.pubkey()));
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&path, &json)
        .with_context(|| format!("Failed to write tape keypair to {}", path.display()))?;

    Ok(path)
}

/// Tape subcommand arguments with global authority flag.
#[derive(Args, Debug)]
pub struct TapeArgs {
    /// Authority keypair: path to file OR pubkey (resolves to ~/.tape/keys/tapes/{pubkey}.json).
    /// If not specified, uses --keypair as the authority.
    #[arg(long, short = 'a', global = true)]
    pub authority: Option<String>,

    #[command(subcommand)]
    pub command: TapeCommand,
}

#[derive(Subcommand, Debug)]
pub enum TapeCommand {
    /// Reserve storage capacity (creates new tape).
    /// If no authority is specified, generates a new keypair.
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
    },

    /// Destroy tape and reclaim rent.
    Destroy,

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

    /// List tapes (queries on-chain, authority can be pubkey).
    List,

    /// Show tape details (queries on-chain, authority can be pubkey).
    Show,
}

pub async fn execute(ctx: &Context, args: TapeArgs) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match args.command {
        TapeCommand::Reserve { size, start_epoch, end_epoch } => {
            reserve(ctx, args.authority, size, start_epoch, end_epoch).await
        }
        TapeCommand::Destroy => {
            destroy(ctx, args.authority).await
        }
        TapeCommand::Split { recipient, at_epoch, at_size } => {
            split(ctx, args.authority, &recipient, at_epoch, at_size).await
        }
        TapeCommand::Merge { recipient } => {
            merge(ctx, args.authority, &recipient).await
        }
        TapeCommand::List => {
            list(ctx, args.authority).await
        }
        TapeCommand::Show => {
            show(ctx, args.authority).await
        }
    }
}


async fn reserve(
    ctx: &Context,
    authority_arg: Option<String>,
    size: u64,
    start_epoch: u64,
    end_epoch: u64,
) -> Result<()> {
    use tape_api::helpers::build_authority_with_tokens_ix;
    use tape_api::instruction::build_reserve_tape_ix;
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_core::types::coin::TAPE;

    if end_epoch <= start_epoch {
        anyhow::bail!("End epoch must be greater than start epoch");
    }

    // Load the fee payer keypair (from --keypair or config)
    let fee_payer = get_keypair(ctx)?;

    // Create RPC client early to fetch archive for cost calculation
    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Fetch archive to calculate reservation cost
    let archive = client.get_archive().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch archive: {}", e))?;

    let num_epochs = end_epoch.saturating_sub(start_epoch);
    let price_per_unit = archive.storage_price.as_u64();
    let single_epoch_cost = price_per_unit.saturating_mul(size);
    let total_cost = single_epoch_cost.saturating_mul(num_epochs);

    // Determine authority: resolve from arg, or generate new one
    let (authority_keypair, is_new_keypair) = match authority_arg {
        Some(auth) => {
            let kp = resolve_authority(&auth, AuthorityType::Tape)?;
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
        eprintln!("  Cost: {} TAPE", TAPE(total_cost));
    }

    if ctx.dry_run {
        println!("Dry run - would reserve {} MB from epoch {} to {}", size, start_epoch, end_epoch);
        println!("  Cost: {} TAPE", TAPE(total_cost));
        if is_new_keypair {
            println!("  Would generate new authority: {}", authority);
        }
        return Ok(());
    }

    // Build instructions
    let mut instructions = Vec::new();

    // If using a new keypair, create ATA and transfer TAPE tokens from fee_payer
    if is_new_keypair {
        let ata_ixs = build_authority_with_tokens_ix(
            fee_payer.pubkey(),
            authority,
            TAPE(total_cost),
        );
        instructions.extend(ata_ixs);
    }

    // Reserve tape instruction (fee_payer pays rent, authority owns)
    instructions.push(build_reserve_tape_ix(
        fee_payer.pubkey(),
        authority,
        StorageUnits(size),
        EpochNumber(start_epoch),
        EpochNumber(end_epoch),
    ));

    // Send with both signers if using new keypair or different authority
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
    println!("  Cost: {} TAPE", TAPE(total_cost));

    if let Some(path) = keypair_path {
        println!("  Keypair saved: {}", path.display());
    }

    Ok(())
}

async fn destroy(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    use tape_api::instruction::build_destroy_tape_ix;

    let fee_payer = get_keypair(ctx)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => {
            // Fall back to fee_payer as authority
            get_keypair(ctx)?
        }
    };

    let authority = authority_keypair.pubkey();

    if !ctx.quiet {
        eprintln!("Destroying tape for authority: {}", authority);
    }

    if ctx.dry_run {
        println!("Dry run - would destroy tape for authority {}", authority);
        return Ok(());
    }

    let ix = build_destroy_tape_ix(fee_payer.pubkey(), authority);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Send with authority signer if different from fee_payer
    let signature = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    println!("Tape destroyed successfully!");
    println!("  Transaction: {}", signature);
    println!("  Authority: {}", authority);

    Ok(())
}

async fn split(
    ctx: &Context,
    authority_arg: Option<String>,
    recipient: &str,
    at_epoch: Option<u64>,
    at_size: Option<u64>,
) -> Result<()> {
    use tape_api::instruction::{build_split_tape_by_epoch_ix, build_split_tape_by_size_ix};
    use tape_core::types::{EpochNumber, StorageUnits};

    let fee_payer = get_keypair(ctx)?;

    // Resolve authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();

    let recipient_pubkey: Pubkey = recipient.parse()
        .with_context(|| format!("Invalid recipient pubkey: {}", recipient))?;

    if at_epoch.is_none() && at_size.is_none() {
        anyhow::bail!("Must specify either --at-epoch or --at-size");
    }

    let ix = if let Some(epoch) = at_epoch {
        if !ctx.quiet {
            eprintln!("Splitting tape at epoch {}", epoch);
            eprintln!("  Source: {}", authority);
            eprintln!("  Recipient: {}", recipient_pubkey);
        }

        if ctx.dry_run {
            println!("Dry run - would split tape at epoch {}", epoch);
            return Ok(());
        }

        build_split_tape_by_epoch_ix(fee_payer.pubkey(), authority, recipient_pubkey, EpochNumber(epoch))
    } else if let Some(size) = at_size {
        if !ctx.quiet {
            eprintln!("Splitting tape at size {} MB", size);
            eprintln!("  Source: {}", authority);
            eprintln!("  Recipient: {}", recipient_pubkey);
        }

        if ctx.dry_run {
            println!("Dry run - would split tape at size {} MB", size);
            return Ok(());
        }

        build_split_tape_by_size_ix(fee_payer.pubkey(), authority, recipient_pubkey, StorageUnits(size))
    } else {
        unreachable!()
    };

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // If recipient is different from authority, we need recipient to sign
    // This is a limitation - in production you'd need multi-party signing
    if recipient_pubkey != authority {
        anyhow::bail!(
            "Split requires recipient ({}) to sign. Multi-party signing not yet supported in CLI. \
             For self-split, use your own pubkey as recipient.",
            recipient_pubkey
        );
    }

    let signature = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    println!("Tape split successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source: {}", authority);
    println!("  Recipient: {}", recipient_pubkey);

    Ok(())
}

async fn merge(ctx: &Context, authority_arg: Option<String>, recipient: &str) -> Result<()> {
    use tape_api::instruction::build_merge_tape_ix;

    let fee_payer = get_keypair(ctx)?;

    // Resolve authority keypair (source tape owner)
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();

    let recipient_pubkey: Pubkey = recipient.parse()
        .with_context(|| format!("Invalid recipient pubkey: {}", recipient))?;

    if !ctx.quiet {
        eprintln!("Merging tape:");
        eprintln!("  Source: {}", authority);
        eprintln!("  Destination: {}", recipient_pubkey);
    }

    if ctx.dry_run {
        println!("Dry run - would merge tape {} into {}", authority, recipient_pubkey);
        return Ok(());
    }

    // Note: The recipient needs to sign too for the merge instruction
    if recipient_pubkey != authority {
        anyhow::bail!(
            "Merge requires recipient ({}) to sign. Multi-party signing not yet supported in CLI. \
             For self-merge, use your own pubkey as recipient.",
            recipient_pubkey
        );
    }

    let ix = build_merge_tape_ix(fee_payer.pubkey(), authority, recipient_pubkey);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    let signature = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    println!("Tapes merged successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source (closed): {}", authority);
    println!("  Destination: {}", recipient_pubkey);

    Ok(())
}

async fn list(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    // For list, authority can be just a pubkey (no signing needed)
    let authority_pubkey: Pubkey = match authority_arg {
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

async fn show(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    use tape_api::program::tapedrive::tape_pda;

    // For show, authority can be just a pubkey (no signing needed)
    let authority_pubkey: Pubkey = match authority_arg {
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
