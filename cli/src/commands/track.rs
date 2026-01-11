//! Track/blob management commands.

use anyhow::Result;
use clap::{Args, Subcommand};
use solana_sdk::signature::Signer;

use tape_sdk::{parse_hash, create_rpc_client};

use crate::utils::{get_keypair, resolve_authority, AuthorityType, CERTIFY_TRACK_COMPUTE_UNITS};
use crate::Context;

/// Track subcommand arguments with global authority flag.
/// Note: Track authority is the tape authority (tracks belong to tapes).
#[derive(Args, Debug)]
pub struct TrackArgs {
    /// Tape authority keypair: path to file OR pubkey (resolves to ~/.tape/keys/tapes/{pubkey}.json).
    /// If not specified, uses --keypair as the authority.
    #[arg(long, short = 'a', global = true)]
    pub authority: Option<String>,

    #[command(subcommand)]
    pub command: TrackCommand,
}

#[derive(Subcommand, Debug)]
pub enum TrackCommand {
    /// Register new track on-chain.
    Register {
        /// Track key hash (hex encoded, 32 bytes).
        #[arg(long)]
        key: String,

        /// Merkle root (hex encoded, 32 bytes).
        #[arg(long)]
        root: String,

        /// Erasure commitment (hex encoded, 32 bytes). Same as root if using standard encoding.
        #[arg(long)]
        commitment: Option<String>,

        /// Total size in bytes.
        #[arg(long)]
        size: u64,
    },

    /// Delete track and free storage.
    Delete {
        /// Track key hash (hex encoded).
        key: String,
    },

    /// Submit track certification (requires BLS signature from committee).
    Certify {
        /// Track key hash (hex encoded).
        key: String,

        /// Committee bitmap (hex encoded).
        #[arg(long)]
        bitmap: String,

        /// BLS signature (hex encoded).
        #[arg(long)]
        signature: String,
    },

    /// Show track status.
    Status {
        /// Track key hash (hex encoded).
        key: String,
    },

    /// List user's tracks.
    List,
}

pub async fn execute(ctx: &Context, args: TrackArgs) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match args.command {
        TrackCommand::Register { key, root, commitment, size } => {
            register(ctx, args.authority, &key, &root, commitment.as_deref(), size).await
        }
        TrackCommand::Delete { key } => {
            delete(ctx, args.authority, &key).await
        }
        TrackCommand::Certify { key, bitmap, signature } => {
            certify(ctx, args.authority, &key, &bitmap, &signature).await
        }
        TrackCommand::Status { key } => {
            status(ctx, args.authority, &key).await
        }
        TrackCommand::List => {
            list(ctx, args.authority).await
        }
    }
}

async fn register(
    ctx: &Context,
    authority_arg: Option<String>,
    key: &str,
    root: &str,
    commitment: Option<&str>,
    size: u64,
) -> Result<()> {
    use tape_api::instruction::build_register_track_ix;
    use tape_core::types::StorageUnits;

    let fee_payer = get_keypair(ctx)?;

    // Resolve tape authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();

    // Parse hashes using SDK helper
    let key_hash = parse_hash(key, "key").map_err(|e| anyhow::anyhow!("{}", e))?;
    let root_hash = parse_hash(root, "root").map_err(|e| anyhow::anyhow!("{}", e))?;
    let commitment_hash = match commitment {
        Some(c) => parse_hash(c, "commitment").map_err(|e| anyhow::anyhow!("{}", e))?,
        None => root_hash, // Default: commitment == root
    };

    // Convert size to storage units (MB)
    let storage_units = StorageUnits::from_bytes(size);

    if !ctx.quiet {
        eprintln!("Registering track:");
        eprintln!("  Fee payer: {}", fee_payer.pubkey());
        eprintln!("  Tape authority: {}", authority);
        eprintln!("  Key: {}", key);
        eprintln!("  Root: {}", root);
        eprintln!("  Commitment: {}", commitment.unwrap_or(root));
        eprintln!("  Size: {} bytes ({} MB)", size, storage_units);
    }

    if ctx.dry_run {
        println!("Dry run - would register track with key {}", key);
        return Ok(());
    }

    let ix = build_register_track_ix(
        fee_payer.pubkey(),
        authority,
        storage_units,
        root_hash,
        commitment_hash,
        key_hash,
    );

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

    println!("Track registered successfully!");
    println!("  Transaction: {}", signature);
    println!("  Tape authority: {}", authority);
    println!("  Key: {}", key);
    println!("  Size: {} bytes", size);

    Ok(())
}

async fn delete(ctx: &Context, authority_arg: Option<String>, key: &str) -> Result<()> {
    use tape_api::instruction::build_delete_track_ix;

    let fee_payer = get_keypair(ctx)?;

    // Resolve tape authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let key_hash = parse_hash(key, "key").map_err(|e| anyhow::anyhow!("{}", e))?;

    if !ctx.quiet {
        eprintln!("Deleting track:");
        eprintln!("  Tape authority: {}", authority);
        eprintln!("  Key: {}", key);
    }

    if ctx.dry_run {
        println!("Dry run - would delete track with key {}", key);
        return Ok(());
    }

    let ix = build_delete_track_ix(fee_payer.pubkey(), authority, key_hash);

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

    println!("Track deleted successfully!");
    println!("  Transaction: {}", signature);
    println!("  Tape authority: {}", authority);
    println!("  Key: {}", key);

    Ok(())
}

async fn certify(
    ctx: &Context,
    authority_arg: Option<String>,
    key: &str,
    bitmap: &str,
    signature: &str,
) -> Result<()> {
    use tape_api::instruction::build_certify_track_ix;
    use tape_api::program::tapedrive::CommitteeBitmap;
    use tape_core::bls::BlsSignature;
    use tape_sdk::parse_hex_bytes;

    let fee_payer = get_keypair(ctx)?;

    // Resolve tape authority keypair
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };

    let authority = authority_keypair.pubkey();
    let key_hash = parse_hash(key, "key").map_err(|e| anyhow::anyhow!("{}", e))?;

    // Parse bitmap (16 bytes for 128 members)
    let bitmap_bytes = parse_hex_bytes(bitmap, "bitmap", 16)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let mut bitmap_arr = [0u8; 16];
    bitmap_arr.copy_from_slice(&bitmap_bytes);
    let committee_bitmap: CommitteeBitmap = bytemuck::cast(bitmap_arr);

    // Parse BLS signature (32 bytes compressed G1)
    let sig_bytes = parse_hex_bytes(signature, "signature", 32)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let mut sig_arr = [0u8; 32];
    sig_arr.copy_from_slice(&sig_bytes);
    let bls_sig: BlsSignature = bytemuck::cast(sig_arr);

    if !ctx.quiet {
        eprintln!("Certifying track:");
        eprintln!("  Tape authority: {}", authority);
        eprintln!("  Key: {}", key);
        eprintln!("  Bitmap: {}", bitmap);
        eprintln!("  Signature: {}...", &signature[..16.min(signature.len())]);
    }

    if ctx.dry_run {
        println!("Dry run - would certify track with key {}", key);
        return Ok(());
    }

    // BLS verification is expensive, request higher compute budget
    let compute_budget_ix =
        solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(CERTIFY_TRACK_COMPUTE_UNITS);
    let certify_ix = build_certify_track_ix(fee_payer.pubkey(), authority, key_hash, committee_bitmap, bls_sig);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    let sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![compute_budget_ix, certify_ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![compute_budget_ix, certify_ix])
            .await
            .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?
    };

    println!("Track certified successfully!");
    println!("  Transaction: {}", sig);
    println!("  Tape authority: {}", authority);
    println!("  Key: {}", key);

    Ok(())
}

async fn status(ctx: &Context, authority_arg: Option<String>, key: &str) -> Result<()> {
    use tape_api::program::tapedrive::track_pda;
    use tape_core::tape::TrackPhase;

    // Resolve authority keypair (same as other tape commands)
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };
    let authority_pubkey = authority_keypair.pubkey();

    let key_hash = parse_hash(key, "key").map_err(|e| anyhow::anyhow!("{}", e))?;
    let (track_address, _) = track_pda(authority_pubkey, key_hash);

    if !ctx.quiet {
        eprintln!("Fetching track status:");
        eprintln!("  Tape authority: {}", authority_pubkey);
        eprintln!("  Key: {}", key);
    }

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    match client.get_track(&authority_pubkey, &key_hash).await {
        Ok(track) => {
            // Determine phase string
            let phase = match TrackPhase::try_from(track.data.state.phase) {
                Ok(TrackPhase::Registered) => "Registered",
                Ok(TrackPhase::Certified) => "Certified",
                Ok(TrackPhase::Invalidated) => "Invalidated",
                Err(_) => "Unknown",
            };

            println!("Track Details:");
            println!("  Account: {}", track_address);
            println!("  ID: {}", track.id);
            println!("  Tape: {}", track.tape);
            println!("  Key: {}", hex::encode(track.key));
            println!("  Size: {} MB", track.size);
            println!("  Phase: {}", phase);
            println!("  Registered Epoch: {}", track.data.registered_epoch);
            println!("  Commitment: {}", hex::encode(track.data.commitment_hash));

            if track.data.is_certified() {
                if let Some(epoch) = track.data.certified_epoch() {
                    println!("  Certified Epoch: {}", epoch);
                }
            }
        }
        Err(e) => {
            if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                println!("Track not found:");
                println!("  Tape authority: {}", authority_pubkey);
                println!("  Key: {}", key);
                println!("  Expected Account: {}", track_address);
            } else {
                return Err(anyhow::anyhow!("Failed to fetch track: {}", e));
            }
        }
    }

    Ok(())
}

async fn list(ctx: &Context, authority_arg: Option<String>) -> Result<()> {
    // Resolve authority keypair (same as other tape commands)
    let authority_keypair = match authority_arg {
        Some(auth) => resolve_authority(&auth, AuthorityType::Tape)?,
        None => get_keypair(ctx)?,
    };
    let authority_pubkey = authority_keypair.pubkey();

    if !ctx.quiet {
        eprintln!("Listing tracks for tape authority: {}", authority_pubkey);
    }

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    // First, get the tape to see track count
    match client.get_tape(&authority_pubkey).await {
        Ok(tape) => {
            if tape.track_count == 0 {
                println!("No tracks registered on tape for authority: {}", authority_pubkey);
                println!("Use `tape track register` to create one.");
                return Ok(());
            }

            println!("Tape has {} track(s) registered.", tape.track_count);
            println!();
            println!("Note: To view individual tracks, use `tape track status --key <KEY>`");
            println!("Track listing requires scanning on-chain accounts which is expensive.");
            println!();
            println!("Tape summary:");
            println!("  Authority: {}", authority_pubkey);
            println!("  Capacity: {} MB", tape.capacity);
            println!("  Used: {} MB", tape.used);
            println!("  Track Count: {}", tape.track_count);
        }
        Err(e) => {
            if e.to_string().contains("not found") || e.to_string().contains("AccountNotFound") {
                println!("No tape found for authority: {}", authority_pubkey);
                println!("Create a tape first with `tape tape reserve`.");
            } else {
                return Err(anyhow::anyhow!("Failed to fetch tape: {}", e));
            }
        }
    }

    Ok(())
}
