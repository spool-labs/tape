//! Blob upload/download commands.
//!
//! The upload command implements the full track lifecycle:
//! 1. Encode blob with Reed-Solomon erasure coding
//! 2. Register track on-chain (requires tape with capacity)
//! 3. Upload slices to storage nodes
//! 4. Collect BLS signatures from committee (certification)
//! 5. Submit CertifyTrack on-chain

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signature::Signer;

use tape_api::instruction::{build_certify_track_ix, build_register_track_ix};
use tape_api::program::tapedrive::track_pda;
use tape_core::types::{NodeId, StorageUnits};
use tape_crypto::Hash;
use tape_sdk::{
    discover_committee_addresses, parse_hash, BlobEncoder, CertificationCollector,
    RpcConfig, TapeClient,
};

use crate::utils::{get_keypair, resolve_authority, AuthorityType};
use crate::Context;

/// Minimum slice size to avoid excessive overhead.
const MIN_SLICE_BYTES: usize = 1024;

/// Maximum slice size for upload.
const MAX_SLICE_BYTES: usize = 256 * 1024;

#[derive(Subcommand, Debug)]
pub enum StorageCommand {
    /// Upload a file to storage nodes with full track registration and certification.
    Upload {
        /// Path to file to upload.
        file: PathBuf,

        /// Tape authority: pubkey (resolves keypair from ~/.tape/keys/tapes/{pubkey}.json)
        /// or path to keypair file.
        #[arg(long, short = 't')]
        tape: String,

        /// Custom key hash (hex encoded, 32 bytes).
        /// If not provided, computed from Blake3 hash of file content.
        #[arg(long)]
        key: Option<String>,

        /// Override storage nodes (comma-separated).
        /// If not provided, auto-discovers from on-chain committee.
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,

        /// Maximum slice size in bytes.
        #[arg(long)]
        max_slice_bytes: Option<usize>,

        /// Skip certification step (just register and upload).
        #[arg(long)]
        skip_certify: bool,
    },

    /// Download a blob from storage nodes.
    Download {
        /// Track ID of the blob.
        track_id: String,

        /// Output file path (stdout if not specified).
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Override storage nodes.
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,

        /// Verify against commitment (hex encoded).
        #[arg(long)]
        verify: Option<String>,
    },

    /// Verify a blob against on-chain commitment.
    Verify {
        /// Track ID.
        track_id: String,

        /// Expected merkle root (hex encoded).
        #[arg(long)]
        root: String,

        /// Override storage nodes.
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,
    },
}

pub async fn execute(ctx: &Context, cmd: StorageCommand) -> Result<()> {
    match cmd {
        StorageCommand::Upload {
            file,
            tape,
            key,
            nodes,
            max_slice_bytes,
            skip_certify,
        } => upload_with_certification(ctx, file, tape, key, nodes, max_slice_bytes, skip_certify).await,
        StorageCommand::Download {
            track_id,
            output,
            nodes,
            verify,
        } => download(ctx, &track_id, output, nodes, verify).await,
        StorageCommand::Verify {
            track_id,
            root,
            nodes,
        } => verify(ctx, &track_id, &root, nodes).await,
    }
}

/// Calculate optimal slice size for a given data size.
fn calculate_slice_size(data_len: usize) -> usize {
    use tape_sdk::DATA_SLICES;

    if data_len == 0 {
        return MIN_SLICE_BYTES;
    }

    let min_needed = (data_len + DATA_SLICES - 1) / DATA_SLICES;
    min_needed.clamp(MIN_SLICE_BYTES, MAX_SLICE_BYTES)
}

/// Resolve node addresses: prefer explicit override, then auto-discover from on-chain, then config fallback.
async fn resolve_node_addresses(
    ctx: &Context,
    explicit_nodes: Option<Vec<String>>,
) -> Result<Vec<String>> {
    // 1. Use explicit --nodes if provided
    if let Some(nodes) = explicit_nodes {
        if !nodes.is_empty() {
            return Ok(nodes);
        }
    }

    // 2. Try auto-discovery from on-chain committee (via SDK)
    let rpc_config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };

    match discover_committee_addresses(&rpc_config).await {
        Ok(result) => {
            // Log any warnings
            for warning in &result.warnings {
                ctx.debug(warning);
            }

            if result.has_nodes() {
                if !ctx.quiet {
                    eprintln!("Discovered {} nodes from on-chain committee", result.node_count());
                }
                return Ok(result.addresses);
            }
        }
        Err(e) => {
            ctx.debug(&format!("Auto-discovery failed: {}", e));
        }
    }

    // 3. Fall back to config
    if !ctx.nodes.is_empty() {
        if !ctx.quiet {
            eprintln!("Using {} nodes from config", ctx.nodes.len());
        }
        return Ok(ctx.nodes.clone());
    }

    anyhow::bail!(
        "No storage nodes available. Either:\n  \
         - Ensure active nodes are registered on-chain, or\n  \
         - Use --nodes to specify manually, or\n  \
         - Set 'nodes' in config file"
    )
}

/// Upload a file with full track registration and certification.
///
/// This implements the complete flow:
/// 1. Encode blob with Reed-Solomon erasure coding
/// 2. Register track on-chain (requires tape with capacity)
/// 3. Upload slices to storage nodes
/// 4. Collect BLS signatures from committee (certification)
/// 5. Submit CertifyTrack on-chain
async fn upload_with_certification(
    ctx: &Context,
    file: PathBuf,
    tape_arg: String,
    key_arg: Option<String>,
    nodes: Option<Vec<String>>,
    max_slice_bytes: Option<usize>,
    skip_certify: bool,
) -> Result<()> {
    use tape_sdk::create_rpc_client;

    // 1. Load keypairs and resolve tape authority
    let fee_payer = get_keypair(ctx)?;
    let authority_keypair = resolve_authority(&tape_arg, AuthorityType::Tape)?;
    let authority = authority_keypair.pubkey();

    ctx.debug(&format!("Fee payer: {}", fee_payer.pubkey()));
    ctx.debug(&format!("Tape authority: {}", authority));

    // 2. Read file
    let data = tokio::fs::read(&file)
        .await
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    let file_size = data.len();
    let slice_size = max_slice_bytes.unwrap_or_else(|| calculate_slice_size(file_size));

    // 3. Compute key hash (from argument or file content)
    let key_hash: Hash = match key_arg {
        Some(ref key_hex) => parse_hash(key_hex, "key").map_err(|e| anyhow::anyhow!("{}", e))?,
        None => {
            // Use Blake3 hash of file content as key
            tape_crypto::hash::hash(&data)
        }
    };

    // 4. Encode blob to get slices with merkle proofs
    let mut encoder = BlobEncoder::with_max_slice_bytes(slice_size);
    let (slices_with_proofs, merkle_root) = encoder
        .encode_with_proofs(data.clone())
        .context("Failed to encode blob")?;

    // For RegisterTrack, root == commitment when using standard encoding
    let commitment_hash: Hash = merkle_root.into();
    let root_hash: Hash = merkle_root.into();
    let storage_units = StorageUnits::from_bytes(file_size as u64);

    // 5. Create RPC client and verify tape exists with capacity
    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    let tape = client
        .get_tape(&authority)
        .await
        .context("Failed to fetch tape - ensure tape exists")?;

    // Verify tape has enough capacity
    let remaining_capacity = tape.capacity.as_u64().saturating_sub(tape.used.as_u64());
    if storage_units.as_u64() > remaining_capacity {
        anyhow::bail!(
            "Tape has insufficient capacity: need {} MB, available {} MB",
            storage_units.as_u64(),
            remaining_capacity
        );
    }

    // Display upload info
    if !ctx.quiet {
        eprintln!("Uploading file:");
        eprintln!("  File: {}", file.display());
        eprintln!("  Size: {} bytes ({} MB)", file_size, storage_units);
        eprintln!("  Slice size: {} bytes", slice_size);
        eprintln!("  Key: {}", hex::encode(key_hash));
        eprintln!("  Merkle root: {}", hex::encode(merkle_root));
        eprintln!("  Tape authority: {}", authority);
    }

    // Dry run check
    if ctx.dry_run {
        println!("Dry run - would upload file with key {}", hex::encode(key_hash));
        return Ok(());
    }

    // 6. Register track on-chain
    if !ctx.quiet {
        eprintln!();
        eprintln!("[1/4] Registering track on-chain...");
    }

    let register_ix = build_register_track_ix(
        fee_payer.pubkey(),
        authority,
        storage_units,
        root_hash,
        commitment_hash,
        key_hash,
    );

    let register_sig = if fee_payer.pubkey() != authority {
        client
            .send_instructions_with_signers(&fee_payer, vec![register_ix], &[&authority_keypair])
            .await
            .map_err(|e| anyhow::anyhow!("RegisterTrack failed: {}", e))?
    } else {
        client
            .send_instructions(&fee_payer, vec![register_ix])
            .await
            .map_err(|e| anyhow::anyhow!("RegisterTrack failed: {}", e))?
    };

    if !ctx.quiet {
        eprintln!("  Transaction: {}", register_sig);
    }

    // Derive track address for later use
    let (track_address, _) = track_pda(authority, key_hash);

    // 7. Resolve node addresses and upload slices
    if !ctx.quiet {
        eprintln!();
        eprintln!("[2/4] Uploading slices to storage nodes...");
    }

    let node_addresses = resolve_node_addresses(ctx, nodes).await?;

    if !ctx.quiet {
        eprintln!("  Nodes: {}", node_addresses.len());
    }

    // Use track address as track_id (base58 string)
    let track_id = track_address.to_string();

    let tape_client = TapeClient::builder()
        .node_addresses(node_addresses.clone())
        .max_slice_bytes(slice_size)
        .build();

    tape_client
        .upload_slices(&track_id, slices_with_proofs)
        .await
        .context("Failed to upload slices")?;

    if !ctx.quiet {
        eprintln!("  Upload complete");
    }

    // 8. Collect BLS signatures and certify (unless skipped)
    let certify_sig = if skip_certify {
        if !ctx.quiet {
            eprintln!();
            eprintln!("[3/4] Skipping certification (--skip-certify)");
            eprintln!("[4/4] Skipping CertifyTrack (--skip-certify)");
        }
        None
    } else {
        if !ctx.quiet {
            eprintln!();
            eprintln!("[3/4] Collecting BLS signatures from committee...");
        }

        // Fetch current system state for committee info
        let system = client
            .get_system()
            .await
            .context("Failed to fetch system state")?;

        // Build map of NodeId -> network address
        let mut node_address_map: HashMap<NodeId, String> = HashMap::new();
        for member in system.committee.iter() {
            if member.id == NodeId(0) {
                continue;
            }
            if let Ok((_, node)) = client.get_node_by_id(member.id).await {
                if let Ok(socket_addr) = node.metadata.network_address.to_socket_addr() {
                    node_address_map.insert(member.id, format!("http://{}", socket_addr));
                }
            }
        }

        if node_address_map.is_empty() {
            anyhow::bail!("No committee members with valid network addresses found");
        }

        // Collect signatures using CertificationCollector
        let collector = CertificationCollector::with_defaults();
        let collected = collector
            .collect_signatures(&track_address, &system, &node_address_map)
            .await
            .context("Failed to collect BLS signatures")?;

        if !ctx.quiet {
            eprintln!(
                "  Signatures: {}/{}",
                collected.signature_count, collected.committee_size
            );
            if collected.early_exit {
                eprintln!("  (early exit - supermajority reached)");
            }
        }

        // 9. Submit CertifyTrack instruction
        if !ctx.quiet {
            eprintln!();
            eprintln!("[4/4] Certifying track on-chain...");
        }

        let certify_ix = build_certify_track_ix(
            fee_payer.pubkey(),
            authority,
            key_hash,
            collected.bitmap,
            collected.aggregated_signature,
        );

        let sig = if fee_payer.pubkey() != authority {
            client
                .send_instructions_with_signers(&fee_payer, vec![certify_ix], &[&authority_keypair])
                .await
                .map_err(|e| anyhow::anyhow!("CertifyTrack failed: {}", e))?
        } else {
            client
                .send_instructions(&fee_payer, vec![certify_ix])
                .await
                .map_err(|e| anyhow::anyhow!("CertifyTrack failed: {}", e))?
        };

        if !ctx.quiet {
            eprintln!("  Transaction: {}", sig);
        }

        Some(sig)
    };

    // 10. Output final results
    println!();
    println!("Upload complete!");
    println!();
    println!("Track Details:");
    println!("  Key: {}", hex::encode(key_hash));
    println!("  Address: {}", track_address);
    println!("  Tape authority: {}", authority);
    println!("  Size: {} bytes ({} MB)", file_size, storage_units);
    println!("  Merkle Root: {}", hex::encode(merkle_root));
    if skip_certify {
        println!("  Status: Registered (not certified)");
    } else {
        println!("  Status: Certified");
    }
    println!();
    println!("Transactions:");
    println!("  RegisterTrack: {}", register_sig);
    if let Some(sig) = certify_sig {
        println!("  CertifyTrack: {}", sig);
    }

    Ok(())
}

async fn download(
    ctx: &Context,
    track_id: &str,
    output: Option<PathBuf>,
    nodes: Option<Vec<String>>,
    verify: Option<String>,
) -> Result<()> {
    use tape_sdk::TapeClient;

    // Resolve node addresses (auto-discover or use override/config)
    let nodes = resolve_node_addresses(ctx, nodes).await?;

    if !ctx.quiet {
        eprintln!("Downloading track {}...", track_id);
    }

    let client = TapeClient::builder()
        .node_addresses(nodes)
        .build();

    let data = if let Some(commitment_hex) = verify {
        // Download with verification
        let commitment_bytes = hex::decode(&commitment_hex)
            .context("Invalid commitment hex")?;

        if commitment_bytes.len() != 32 {
            anyhow::bail!("Commitment must be 32 bytes (got {})", commitment_bytes.len());
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&commitment_bytes);
        let commitment: tape_sdk::BlobMerkleRoot = arr.into();

        if !ctx.quiet {
            eprintln!("Verifying against commitment: {}", commitment_hex);
        }

        client
            .download_blob_verified(track_id, &commitment)
            .await
            .context("Failed to download and verify blob")?
    } else {
        // Download without verification
        client
            .download_blob(track_id)
            .await
            .context("Failed to download blob")?
    };

    if !ctx.quiet {
        eprintln!("Download complete! ({} bytes)", data.len());
    }

    match output {
        Some(path) => {
            tokio::fs::write(&path, &data)
                .await
                .with_context(|| format!("Failed to write file: {}", path.display()))?;
            if !ctx.quiet {
                eprintln!("Written to: {}", path.display());
            }
        }
        None => {
            use std::io::Write;
            std::io::stdout()
                .write_all(&data)
                .context("Failed to write to stdout")?;
        }
    }

    Ok(())
}

async fn verify(
    ctx: &Context,
    track_id: &str,
    root: &str,
    nodes: Option<Vec<String>>,
) -> Result<()> {
    use tape_sdk::{TapeClient, BlobEncoder, BlobMerkleRoot};

    // Resolve node addresses (auto-discover or use override/config)
    let nodes = resolve_node_addresses(ctx, nodes).await?;

    // Parse expected root
    let expected_bytes = hex::decode(root)
        .context("Invalid merkle root hex")?;

    if expected_bytes.len() != 32 {
        anyhow::bail!("Merkle root must be 32 bytes (got {})", expected_bytes.len());
    }

    let mut arr = [0u8; 32];
    arr.copy_from_slice(&expected_bytes);
    let expected_root: BlobMerkleRoot = arr.into();

    if !ctx.quiet {
        eprintln!("Downloading track {} for verification...", track_id);
    }

    // Download the blob
    let client = TapeClient::builder()
        .node_addresses(nodes)
        .build();

    let data = client
        .download_blob(track_id)
        .await
        .context("Failed to download blob")?;

    if !ctx.quiet {
        eprintln!("Downloaded {} bytes, computing commitment...", data.len());
    }

    // Re-encode to compute commitment
    let mut encoder = BlobEncoder::new();
    let (_, actual_root) = encoder
        .encode_to_vec_with_root(data)
        .context("Failed to encode blob for verification")?;

    // Compare
    if actual_root == expected_root {
        println!("Verification PASSED");
        println!("  Track ID: {}", track_id);
        println!("  Merkle Root: {}", hex::encode(actual_root));
        Ok(())
    } else {
        println!("Verification FAILED");
        println!("  Track ID: {}", track_id);
        println!("  Expected: {}", hex::encode(expected_root));
        println!("  Actual:   {}", hex::encode(actual_root));
        anyhow::bail!("Commitment mismatch - data may be corrupted or tampered")
    }
}
