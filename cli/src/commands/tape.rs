//! Storage resource (tape) management commands.

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::Signer;
use solana_sdk::pubkey::Pubkey;

use tape_sdk::{load_solana_keypair, create_rpc_client};

use crate::config::expand_path;
use crate::Context;

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
        TapeCommand::Reserve { size, start_epoch, end_epoch } => {
            reserve(ctx, size, start_epoch, end_epoch).await
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
) -> Result<()> {
    use tape_api::instruction::build_reserve_tape_ix;
    use tape_core::types::{EpochNumber, StorageUnits};

    if end_epoch <= start_epoch {
        anyhow::bail!("End epoch must be greater than start epoch");
    }

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

    if !ctx.quiet {
        eprintln!("Reserving tape:");
        eprintln!("  Authority: {}", signer);
        eprintln!("  Size: {} MB", size);
        eprintln!("  Start epoch: {}", start_epoch);
        eprintln!("  End epoch: {}", end_epoch);
    }

    if ctx.dry_run {
        println!("Dry run - would reserve {} MB from epoch {} to {}", size, start_epoch, end_epoch);
        return Ok(());
    }

    let ix = build_reserve_tape_ix(
        signer,
        StorageUnits(size),
        EpochNumber(start_epoch),
        EpochNumber(end_epoch),
    );

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Tape reserved successfully!");
    println!("  Transaction: {}", signature);
    println!("  Authority: {}", signer);
    println!("  Size: {} MB", size);
    println!("  Active: epoch {} to {}", start_epoch, end_epoch);

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
