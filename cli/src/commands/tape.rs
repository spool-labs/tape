//! Storage resource (tape) management commands.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use comfy_table::{presets::UTF8_FULL, Table};
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::pubkey::Pubkey;

use tape_sdk::create_rpc_client;

use crate::output::OutputFormat;
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

    /// Split tape by epoch or size (creates new tape with new keypair).
    Split {
        /// Source tape authority pubkey (tape to split from).
        source: String,

        /// Split at epoch (creates new tape from this epoch onwards).
        #[arg(long, conflicts_with = "at_size")]
        at_epoch: Option<u64>,

        /// Split at size in MB (creates new tape with this capacity).
        #[arg(long, conflicts_with = "at_epoch")]
        at_size: Option<u64>,
    },

    /// Merge tapes (combine two tapes into one).
    Merge {
        /// Source tape authority pubkey (tape to merge from).
        source: String,

        /// Destination tape authority pubkey (tape to merge into).
        destination: String,
    },

    /// List tapes (queries on-chain, authority can be pubkey).
    List,
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
        TapeCommand::Split { source, at_epoch, at_size } => {
            split(ctx, &source, at_epoch, at_size).await
        }
        TapeCommand::Merge { source, destination } => {
            merge(ctx, &source, &destination).await
        }
        TapeCommand::List => {
            list(ctx, args.authority).await
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
    source: &str,
    at_epoch: Option<u64>,
    at_size: Option<u64>,
) -> Result<()> {
    use tape_api::instruction::{build_split_tape_by_epoch_ix, build_split_tape_by_size_ix};
    use tape_core::types::{EpochNumber, StorageUnits};

    if at_epoch.is_none() && at_size.is_none() {
        anyhow::bail!("Must specify either --at-epoch or --at-size");
    }

    let fee_payer = get_keypair(ctx)?;

    // Resolve source keypair
    let source_keypair = resolve_authority(source, AuthorityType::Tape)?;
    let source_pubkey = source_keypair.pubkey();

    // Generate a new keypair for the recipient (new tape)
    let recipient_keypair = Keypair::new();
    let recipient_pubkey = recipient_keypair.pubkey();

    if !ctx.quiet {
        if let Some(epoch) = at_epoch {
            eprintln!("Splitting tape at epoch {}", epoch);
        } else if let Some(size) = at_size {
            eprintln!("Splitting tape at size {} MB", size);
        }
        eprintln!("  Source: {}", source_pubkey);
        eprintln!("  Recipient: {} (new)", recipient_pubkey);
    }

    if ctx.dry_run {
        if let Some(epoch) = at_epoch {
            println!("Dry run - would split tape at epoch {}", epoch);
        } else if let Some(size) = at_size {
            println!("Dry run - would split tape at size {} MB", size);
        }
        println!("  Would generate new recipient: {}", recipient_pubkey);
        return Ok(());
    }

    let ix = if let Some(epoch) = at_epoch {
        build_split_tape_by_epoch_ix(fee_payer.pubkey(), source_pubkey, recipient_pubkey, EpochNumber(epoch))
    } else if let Some(size) = at_size {
        build_split_tape_by_size_ix(fee_payer.pubkey(), source_pubkey, recipient_pubkey, StorageUnits(size))
    } else {
        unreachable!()
    };

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Source and recipient both need to sign, recipient is always new
    let signature = if fee_payer.pubkey() == source_pubkey {
        // Fee payer is source, recipient is new
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&recipient_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        // Fee payer is neither, need both additional signers
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&source_keypair, &recipient_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    // Save the new recipient keypair
    let keypair_path = save_tape_keypair(&recipient_keypair)?;

    println!("Tape split successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source: {}", source_pubkey);
    println!("  Recipient: {}", recipient_pubkey);
    println!("  Keypair saved: {}", keypair_path.display());

    Ok(())
}

async fn merge(ctx: &Context, source: &str, destination: &str) -> Result<()> {
    use tape_api::instruction::build_merge_tape_ix;

    let fee_payer = get_keypair(ctx)?;

    // Resolve both keypairs
    let source_keypair = resolve_authority(source, AuthorityType::Tape)?;
    let dest_keypair = resolve_authority(destination, AuthorityType::Tape)?;

    let source_pubkey = source_keypair.pubkey();
    let dest_pubkey = dest_keypair.pubkey();

    if !ctx.quiet {
        eprintln!("Merging tape:");
        eprintln!("  Source: {}", source_pubkey);
        eprintln!("  Destination: {}", dest_pubkey);
    }

    if ctx.dry_run {
        println!("Dry run - would merge tape {} into {}", source_pubkey, dest_pubkey);
        return Ok(());
    }

    let ix = build_merge_tape_ix(fee_payer.pubkey(), source_pubkey, dest_pubkey);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Both source and destination need to sign
    let fee_payer_is_source = fee_payer.pubkey() == source_pubkey;
    let fee_payer_is_dest = fee_payer.pubkey() == dest_pubkey;

    let signature = if fee_payer_is_source && fee_payer_is_dest {
        // Fee payer owns both tapes (same keypair)
        client
            .send_instructions(&fee_payer, vec![ix])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else if fee_payer_is_source {
        // Fee payer is source, dest is different
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&dest_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else if fee_payer_is_dest {
        // Fee payer is dest, source is different
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&source_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        // Fee payer is neither, need both additional signers
        client
            .send_instructions_with_signers(&fee_payer, vec![ix], &[&source_keypair, &dest_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    println!("Tapes merged successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source (closed): {}", source_pubkey);
    println!("  Destination: {}", dest_pubkey);

    Ok(())
}

async fn list(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Collect tapes to display
    let mut tapes: Vec<(Pubkey, tape_api::state::Tape)> = Vec::new();
    let mut not_found: Vec<Pubkey> = Vec::new();

    // If authority is provided, query just that tape
    if let Some(auth) = authority_arg {
        let authority_pubkey: Pubkey = auth.parse()
            .with_context(|| format!("Invalid authority pubkey: {}", auth))?;

        match client.get_tape(&authority_pubkey).await {
            Ok(tape) => tapes.push((authority_pubkey, tape)),
            Err(e) => {
                if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                    not_found.push(authority_pubkey);
                } else {
                    return Err(anyhow::anyhow!("Failed to fetch tape: {}", e));
                }
            }
        }
    } else {
        // No authority provided - list all saved tape keypairs
        let tapes_dir = authority_keys_dir(AuthorityType::Tape);

        if !tapes_dir.exists() {
            match ctx.output {
                OutputFormat::Json => println!("[]"),
                _ => {
                    println!("No tapes found.");
                    println!("Use `tape tape reserve` to create one.");
                }
            }
            return Ok(());
        }

        let entries: Vec<_> = std::fs::read_dir(&tapes_dir)
            .with_context(|| format!("Failed to read tapes directory: {}", tapes_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
            .collect();

        if entries.is_empty() {
            match ctx.output {
                OutputFormat::Json => println!("[]"),
                _ => {
                    println!("No tapes found.");
                    println!("Use `tape tape reserve` to create one.");
                }
            }
            return Ok(());
        }

        for entry in entries {
            let filename = entry.file_name();
            let pubkey_str = filename.to_string_lossy();
            let pubkey_str = pubkey_str.trim_end_matches(".json");

            let authority_pubkey: Pubkey = match pubkey_str.parse() {
                Ok(pk) => pk,
                Err(_) => continue,
            };

            match client.get_tape(&authority_pubkey).await {
                Ok(tape) => tapes.push((authority_pubkey, tape)),
                Err(e) => {
                    if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                        not_found.push(authority_pubkey);
                    }
                }
            }
        }
    }

    // Output based on format
    match ctx.output {
        OutputFormat::Json => {
            let json_tapes: Vec<_> = tapes.iter().map(|(authority, tape)| {
                serde_json::json!({
                    "authority": authority.to_string(),
                    "id": tape.id.as_u64(),
                    "capacity": tape.capacity.as_u64(),
                    "used": tape.used.as_u64(),
                    "active_epoch": tape.active_epoch.as_u64(),
                    "expiry_epoch": tape.expiry_epoch.as_u64(),
                    "track_count": tape.track_count,
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&json_tapes)?);
        }
        _ => {
            if tapes.is_empty() && not_found.is_empty() {
                println!("No tapes found.");
                println!("Use `tape tape reserve` to create one.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Authority", "ID", "Capacity", "Used", "Epochs", "Tracks"]);

            for (authority, tape) in &tapes {
                let epoch_range = format!("{}-{}", tape.active_epoch.as_u64(), tape.expiry_epoch.as_u64());
                table.add_row(vec![
                    &authority.to_string(),
                    &tape.id.as_u64().to_string(),
                    &format!("{} MB", tape.capacity.as_u64()),
                    &format!("{} MB", tape.used.as_u64()),
                    &epoch_range,
                    &tape.track_count.to_string(),
                ]);
            }

            for authority in &not_found {
                table.add_row(vec![
                    &authority.to_string(),
                    "(not found on-chain)",
                    "",
                    "",
                    "",
                    "",
                ]);
            }

            println!("{}", table);
            println!("\nTotal: {} tape(s)", tapes.len());
        }
    }

    Ok(())
}
