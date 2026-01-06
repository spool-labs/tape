//! Storage resource (tape) management commands.

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use comfy_table::{presets::UTF8_FULL, Table};
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::pubkey::Pubkey;
use tape_api::program::tapedrive::tape_pda;

use tape_sdk::create_rpc_client;

use crate::output::OutputFormat;
use crate::utils::{get_keypair, resolve_authority, authority_keys_dir, save_tape_keypair, AuthorityType};
use crate::Context;

/// Tape subcommand arguments.
#[derive(Args, Debug)]
pub struct TapeArgs {
    #[command(subcommand)]
    pub command: TapeCommand,
}

#[derive(Subcommand, Debug)]
pub enum TapeCommand {
    /// Initialize a new tape (reserve storage capacity).
    Init {
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
        /// Tape account address (on-chain PDA).
        tape: String,
    },

    /// Split tape by epoch or size (creates new tape with new keypair).
    Split {
        /// Source tape account address (tape to split from).
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
        /// Source tape account address (tape to merge from, will be closed).
        source: String,

        /// Destination tape account address (tape to merge into).
        destination: String,
    },

    /// List all saved tapes.
    List,
}

pub async fn execute(ctx: &Context, args: TapeArgs) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match args.command {
        TapeCommand::Init { size, start_epoch, end_epoch } => {
            init(ctx, size, start_epoch, end_epoch).await
        }
        TapeCommand::Destroy { tape } => {
            destroy(ctx, &tape).await
        }
        TapeCommand::Split { source, at_epoch, at_size } => {
            split(ctx, &source, at_epoch, at_size).await
        }
        TapeCommand::Merge { source, destination } => {
            merge(ctx, &source, &destination).await
        }
        TapeCommand::List => {
            list(ctx).await
        }
    }
}


async fn init(
    ctx: &Context,
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

    // Generate a new unique keypair for this tape
    let authority_keypair = Keypair::new();
    let authority = authority_keypair.pubkey();
    let (tape_address, _) = tape_pda(authority);

    if !ctx.quiet {
        eprintln!("Reserving tape:");
        eprintln!("  Fee payer: {}", fee_payer.pubkey());
        eprintln!("  Tape: {} (new)", tape_address);
        eprintln!("  Size: {} MB", size);
        eprintln!("  Start epoch: {}", start_epoch);
        eprintln!("  End epoch: {}", end_epoch);
        eprintln!("  Cost: {} TAPE", TAPE(total_cost));
    }

    if ctx.dry_run {
        println!("Dry run - would reserve {} MB from epoch {} to {}", size, start_epoch, end_epoch);
        println!("  Cost: {} TAPE", TAPE(total_cost));
        println!("  Would create tape: {}", tape_address);
        return Ok(());
    }

    // Build instructions: create ATA and transfer TAPE tokens from fee_payer
    let mut instructions = Vec::new();
    let ata_ixs = build_authority_with_tokens_ix(
        fee_payer.pubkey(),
        authority,
        TAPE(total_cost),
    );
    instructions.extend(ata_ixs);

    // Reserve tape instruction (fee_payer pays rent, authority owns)
    instructions.push(build_reserve_tape_ix(
        fee_payer.pubkey(),
        authority,
        StorageUnits(size),
        EpochNumber(start_epoch),
        EpochNumber(end_epoch),
    ));

    // Send with authority as additional signer
    let signature = client
        .send_instructions_with_signers(&fee_payer, instructions, &[&authority_keypair])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    // Save the new keypair (indexed by tape address)
    let (_, keypair_path) = save_tape_keypair(&authority_keypair)?;

    println!("Tape reserved successfully!");
    println!("  Transaction: {}", signature);
    println!("  Tape: {}", tape_address);
    println!("  Size: {} MB", size);
    println!("  Active: epoch {} to {}", start_epoch, end_epoch);
    println!("  Cost: {} TAPE", TAPE(total_cost));
    println!("  Keypair saved: {}", keypair_path.display());

    Ok(())
}

async fn destroy(ctx: &Context, tape_address: &str) -> Result<()> {
    use tape_api::instruction::build_destroy_tape_ix;

    let fee_payer = get_keypair(ctx)?;

    // Resolve authority keypair from tape address
    let authority_keypair = resolve_authority(tape_address, AuthorityType::Tape)?;
    let authority = authority_keypair.pubkey();
    let (derived_tape_address, _) = tape_pda(authority);

    if !ctx.quiet {
        eprintln!("Destroying tape: {}", derived_tape_address);
    }

    if ctx.dry_run {
        println!("Dry run - would destroy tape {}", derived_tape_address);
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
    println!("  Tape: {}", derived_tape_address);

    Ok(())
}

async fn split(
    ctx: &Context,
    source_tape: &str,
    at_epoch: Option<u64>,
    at_size: Option<u64>,
) -> Result<()> {
    use tape_api::instruction::{build_split_tape_by_epoch_ix, build_split_tape_by_size_ix};
    use tape_core::types::{EpochNumber, StorageUnits};

    if at_epoch.is_none() && at_size.is_none() {
        anyhow::bail!("Must specify either --at-epoch or --at-size");
    }

    let fee_payer = get_keypair(ctx)?;

    // Resolve source keypair from tape address
    let source_keypair = resolve_authority(source_tape, AuthorityType::Tape)?;
    let source_authority = source_keypair.pubkey();
    let (source_tape_address, _) = tape_pda(source_authority);

    // Generate a new keypair for the recipient (new tape)
    let recipient_keypair = Keypair::new();
    let recipient_authority = recipient_keypair.pubkey();
    let (new_tape_address, _) = tape_pda(recipient_authority);

    if !ctx.quiet {
        if let Some(epoch) = at_epoch {
            eprintln!("Splitting tape at epoch {}", epoch);
        } else if let Some(size) = at_size {
            eprintln!("Splitting tape at size {} MB", size);
        }
        eprintln!("  Source: {}", source_tape_address);
        eprintln!("  New tape: {} (new)", new_tape_address);
    }

    if ctx.dry_run {
        if let Some(epoch) = at_epoch {
            println!("Dry run - would split tape at epoch {}", epoch);
        } else if let Some(size) = at_size {
            println!("Dry run - would split tape at size {} MB", size);
        }
        println!("  Would create new tape: {}", new_tape_address);
        return Ok(());
    }

    let ix = if let Some(epoch) = at_epoch {
        build_split_tape_by_epoch_ix(fee_payer.pubkey(), source_authority, recipient_authority, EpochNumber(epoch))
    } else if let Some(size) = at_size {
        build_split_tape_by_size_ix(fee_payer.pubkey(), source_authority, recipient_authority, StorageUnits(size))
    } else {
        unreachable!()
    };

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Source and recipient both need to sign, recipient is always new
    let signature = if fee_payer.pubkey() == source_authority {
        // Fee payer is source authority, recipient is new
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

    // Save the new recipient keypair (indexed by tape address)
    let (_, keypair_path) = save_tape_keypair(&recipient_keypair)?;

    println!("Tape split successfully!");
    println!("  Transaction: {}", signature);
    println!("  Source: {}", source_tape_address);
    println!("  New tape: {}", new_tape_address);
    println!("  Keypair saved: {}", keypair_path.display());

    Ok(())
}

async fn merge(ctx: &Context, source_tape: &str, dest_tape: &str) -> Result<()> {
    use tape_api::instruction::build_merge_tape_ix;

    let fee_payer = get_keypair(ctx)?;

    // Resolve both keypairs from tape addresses
    let source_keypair = resolve_authority(source_tape, AuthorityType::Tape)?;
    let dest_keypair = resolve_authority(dest_tape, AuthorityType::Tape)?;

    let source_authority = source_keypair.pubkey();
    let dest_authority = dest_keypair.pubkey();
    let (source_tape_address, _) = tape_pda(source_authority);
    let (dest_tape_address, _) = tape_pda(dest_authority);

    if !ctx.quiet {
        eprintln!("Merging tapes:");
        eprintln!("  Source: {}", source_tape_address);
        eprintln!("  Destination: {}", dest_tape_address);
    }

    if ctx.dry_run {
        println!("Dry run - would merge tape {} into {}", source_tape_address, dest_tape_address);
        return Ok(());
    }

    let ix = build_merge_tape_ix(fee_payer.pubkey(), source_authority, dest_authority);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Both source and destination need to sign
    let fee_payer_is_source = fee_payer.pubkey() == source_authority;
    let fee_payer_is_dest = fee_payer.pubkey() == dest_authority;

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
    println!("  Source (closed): {}", source_tape_address);
    println!("  Destination: {}", dest_tape_address);

    Ok(())
}

async fn list(ctx: &Context) -> Result<()> {
    use crate::utils::load_keypair_from_path;

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Collect tapes to display: (tape_address, tape_data)
    let mut tapes: Vec<(Pubkey, tape_api::state::Tape)> = Vec::new();
    let mut not_found: Vec<Pubkey> = Vec::new();

    // List all saved tape keypairs (filenames are tape addresses)
    let tapes_dir = authority_keys_dir(AuthorityType::Tape);

    if !tapes_dir.exists() {
        match ctx.output {
            OutputFormat::Json => println!("[]"),
            _ => {
                println!("No tapes found.");
                println!("Use `tape tape init` to create one.");
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
                println!("Use `tape tape init` to create one.");
            }
        }
        return Ok(());
    }

    for entry in entries {
        let path = entry.path();
        let filename = entry.file_name();
        let tape_address_str = filename.to_string_lossy();
        let tape_address_str = tape_address_str.trim_end_matches(".json");

        // Parse tape address from filename
        let tape_address: Pubkey = match tape_address_str.parse() {
            Ok(pk) => pk,
            Err(_) => continue,
        };

        // Load keypair to get authority
        let keypair = match load_keypair_from_path(&path.to_string_lossy()) {
            Ok(kp) => kp,
            Err(_) => continue,
        };
        let authority = keypair.pubkey();

        // Fetch tape using authority
        match client.get_tape(&authority).await {
            Ok(tape) => tapes.push((tape_address, tape)),
            Err(e) => {
                if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                    not_found.push(tape_address);
                }
            }
        }
    }

    // Output based on format
    match ctx.output {
        OutputFormat::Json => {
            let json_tapes: Vec<_> = tapes.iter().map(|(tape_address, tape)| {
                serde_json::json!({
                    "address": tape_address.to_string(),
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
                println!("Use `tape tape init` to create one.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Tape", "ID", "Capacity", "Used", "Epochs", "Tracks"]);

            for (tape_address, tape) in &tapes {
                let epoch_range = format!("{}-{}", tape.active_epoch.as_u64(), tape.expiry_epoch.as_u64());
                table.add_row(vec![
                    &tape_address.to_string(),
                    &tape.id.as_u64().to_string(),
                    &format!("{} MB", tape.capacity.as_u64()),
                    &format!("{} MB", tape.used.as_u64()),
                    &epoch_range,
                    &tape.track_count.to_string(),
                ]);
            }

            for tape_address in &not_found {
                table.add_row(vec![
                    &tape_address.to_string(),
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
