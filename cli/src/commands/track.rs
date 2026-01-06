//! Track/blob management commands.

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::Signer;
use solana_sdk::pubkey::Pubkey;

use tape_sdk::{load_solana_keypair, parse_hash, create_rpc_client};

use crate::config::expand_path;
use crate::Context;

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

        /// Authority pubkey (uses keypair if not specified).
        #[arg(long)]
        authority: Option<String>,
    },

    /// List user's tracks.
    List {
        /// Authority pubkey (uses keypair if not specified).
        #[arg(long)]
        authority: Option<String>,
    },
}

pub async fn execute(ctx: &Context, cmd: TrackCommand) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    match cmd {
        TrackCommand::Register { key, root, commitment, size } => {
            register(ctx, &key, &root, commitment.as_deref(), size).await
        }
        TrackCommand::Delete { key } => {
            delete(ctx, &key).await
        }
        TrackCommand::Certify { key, bitmap, signature } => {
            certify(ctx, &key, &bitmap, &signature).await
        }
        TrackCommand::Status { key, authority } => {
            status(ctx, &key, authority).await
        }
        TrackCommand::List { authority } => {
            list(ctx, authority).await
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

async fn register(
    ctx: &Context,
    key: &str,
    root: &str,
    commitment: Option<&str>,
    size: u64,
) -> Result<()> {
    use tape_api::instruction::build_register_track_ix;
    use tape_core::types::StorageUnits;

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

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
        eprintln!("  Authority: {}", signer);
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
        signer,
        signer,
        storage_units,
        root_hash,
        commitment_hash,
        key_hash,
    );

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Track registered successfully!");
    println!("  Transaction: {}", signature);
    println!("  Authority: {}", signer);
    println!("  Key: {}", key);
    println!("  Size: {} bytes", size);

    Ok(())
}

async fn delete(ctx: &Context, key: &str) -> Result<()> {
    use tape_api::instruction::build_delete_track_ix;

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

    let key_hash = parse_hash(key, "key").map_err(|e| anyhow::anyhow!("{}", e))?;

    if !ctx.quiet {
        eprintln!("Deleting track:");
        eprintln!("  Authority: {}", signer);
        eprintln!("  Key: {}", key);
    }

    if ctx.dry_run {
        println!("Dry run - would delete track with key {}", key);
        return Ok(());
    }

    let ix = build_delete_track_ix(signer, signer, key_hash);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    let signature = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Track deleted successfully!");
    println!("  Transaction: {}", signature);
    println!("  Authority: {}", signer);
    println!("  Key: {}", key);

    Ok(())
}

async fn certify(
    ctx: &Context,
    key: &str,
    bitmap: &str,
    signature: &str,
) -> Result<()> {
    use tape_api::instruction::build_certify_track_ix;
    use tape_api::program::tapedrive::CommitteeBitmap;
    use tape_core::bls::BlsSignature;
    use tape_sdk::parse_hex_bytes;

    let keypair = get_keypair(ctx)?;
    let signer = keypair.pubkey();

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
        eprintln!("  Authority: {}", signer);
        eprintln!("  Key: {}", key);
        eprintln!("  Bitmap: {}", bitmap);
        eprintln!("  Signature: {}...", &signature[..16.min(signature.len())]);
    }

    if ctx.dry_run {
        println!("Dry run - would certify track with key {}", key);
        return Ok(());
    }

    let ix = build_certify_track_ix(signer, signer, key_hash, committee_bitmap, bls_sig);

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;
    let sig = client
        .send_instructions(&keypair, vec![ix])
        .await
        .map_err(|e| anyhow::anyhow!("Transaction failed: {}", e))?;

    println!("Track certified successfully!");
    println!("  Transaction: {}", sig);
    println!("  Authority: {}", signer);
    println!("  Key: {}", key);

    Ok(())
}

async fn status(ctx: &Context, key: &str, authority: Option<String>) -> Result<()> {
    use tape_api::program::tapedrive::track_pda;
    use tape_core::tape::TrackPhase;

    let authority_pubkey: Pubkey = match authority {
        Some(auth) => auth.parse()
            .with_context(|| format!("Invalid authority pubkey: {}", auth))?,
        None => {
            let keypair = get_keypair(ctx)?;
            keypair.pubkey()
        }
    };

    let key_hash = parse_hash(key, "key").map_err(|e| anyhow::anyhow!("{}", e))?;
    let (track_address, _) = track_pda(authority_pubkey, key_hash);

    if !ctx.quiet {
        eprintln!("Fetching track status:");
        eprintln!("  Authority: {}", authority_pubkey);
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
                println!("  Authority: {}", authority_pubkey);
                println!("  Key: {}", key);
                println!("  Expected Account: {}", track_address);
            } else {
                return Err(anyhow::anyhow!("Failed to fetch track: {}", e));
            }
        }
    }

    Ok(())
}

async fn list(ctx: &Context, authority: Option<String>) -> Result<()> {
    let authority_pubkey: Pubkey = match authority {
        Some(auth) => auth.parse()
            .with_context(|| format!("Invalid authority pubkey: {}", auth))?,
        None => {
            let keypair = get_keypair(ctx)?;
            keypair.pubkey()
        }
    };

    if !ctx.quiet {
        eprintln!("Listing tracks for authority: {}", authority_pubkey);
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
