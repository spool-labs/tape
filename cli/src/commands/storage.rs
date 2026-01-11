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
    discover_full, parse_hash, BlobEncoder, CertificationCollector,
    RpcConfig, TapeClient,
};
use rpc_client::Rpc;

use crate::utils::{get_keypair, resolve_authority, AuthorityType, CERTIFY_TRACK_COMPUTE_UNITS};
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
        /// or path to keypair file. If not provided, auto-creates a new tape.
        #[arg(long, short = 't')]
        tape: Option<String>,

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
        #[arg(short = 'O', long = "outfile")]
        outfile: Option<PathBuf>,

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
        } => upload_with_certification(ctx, file, tape.as_deref(), key, nodes, max_slice_bytes, skip_certify).await,
        StorageCommand::Download {
            track_id,
            outfile,
            nodes,
            verify,
        } => download(ctx, &track_id, outfile, nodes, verify).await,
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

/// Discover on-chain state (committee, spool assignment) and resolve node addresses.
///
/// This function always fetches on-chain state, then optionally allows overriding
/// addresses via explicit nodes or config.
async fn discover_network_state(
    ctx: &Context,
    explicit_nodes: Option<Vec<String>>,
) -> Result<tape_sdk::NetworkState> {
    use tape_core::types::network::NetworkAddress;

    let rpc_config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };

    // Always fetch on-chain state for committee and spool assignment
    let mut result = discover_full(&rpc_config).await
        .context("Failed to discover on-chain state")?;

    // Log any warnings from discovery
    for warning in &result.warnings {
        ctx.debug(warning);
    }

    // Override node addresses if explicit nodes provided
    if let Some(nodes) = explicit_nodes {
        if !nodes.is_empty() {
            if !ctx.quiet {
                eprintln!("Using {} explicitly specified nodes", nodes.len());
            }
            result.node_addresses = nodes
                .into_iter()
                .enumerate()
                .filter_map(|(idx, addr_str)| {
                    let addr_str = addr_str.strip_prefix("http://").unwrap_or(&addr_str);
                    let addr_str = addr_str.strip_prefix("https://").unwrap_or(addr_str);
                    NetworkAddress::from(addr_str).ok().map(|addr| (idx, addr))
                })
                .collect();
            return Ok(result);
        }
    }

    // Check if we have discovered addresses
    if result.has_nodes() {
        if !ctx.quiet {
            eprintln!("Discovered {} nodes from on-chain committee", result.node_count());
        }
        return Ok(result);
    }

    // Fall back to config addresses
    if !ctx.nodes.is_empty() {
        if !ctx.quiet {
            eprintln!("Using {} nodes from config", ctx.nodes.len());
        }
        result.node_addresses = ctx.nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, addr_str)| {
                let addr_str = addr_str.strip_prefix("http://").unwrap_or(addr_str);
                let addr_str = addr_str.strip_prefix("https://").unwrap_or(addr_str);
                NetworkAddress::from(addr_str).ok().map(|addr| (idx, addr))
            })
            .collect();
        return Ok(result);
    }

    anyhow::bail!(
        "No storage nodes available. Either:\n  \
         - Ensure active nodes are registered on-chain, or\n  \
         - Use --nodes to specify manually, or\n  \
         - Set 'nodes' in config file"
    )
}

async fn create_tape_for_file<R: Rpc>(
    ctx: &Context,
    client: &rpc_client::RpcClient<R>,
    fee_payer: &solana_sdk::signature::Keypair,
    file_size: usize,
) -> Result<solana_sdk::signature::Keypair> {
    use solana_sdk::signature::{Keypair, Signer};
    use tape_api::helpers::build_authority_with_tokens_ix;
    use tape_api::instruction::build_reserve_tape_ix;
    use tape_api::program::tapedrive::tape_pda;
    use tape_core::types::EpochNumber;
    use tape_core::types::coin::TAPE;
    use crate::utils::save_tape_keypair;

    let size_mb = ((file_size as u64 + 1024 * 1024 - 1) / (1024 * 1024)).max(1) + 1;

    let epoch = client.get_epoch().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch epoch: {}", e))?;
    let start_epoch = epoch.id.as_u64();
    let end_epoch = start_epoch + 10;

    let archive = client.get_archive().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch archive: {}", e))?;
    let total_cost = archive.storage_price.as_u64()
        .saturating_mul(size_mb)
        .saturating_mul(end_epoch - start_epoch);

    let tape_keypair = Keypair::new();
    let tape_authority = tape_keypair.pubkey();
    let (tape_address, _) = tape_pda(tape_authority);

    if !ctx.quiet {
        eprintln!("Auto-creating tape:");
        eprintln!("  Tape: {}", tape_address);
        eprintln!("  Size: {} MB", size_mb);
        eprintln!("  Epochs: {}-{}", start_epoch, end_epoch);
        eprintln!("  Cost: {} TAPE", TAPE(total_cost));
    }

    let mut instructions = build_authority_with_tokens_ix(
        fee_payer.pubkey(),
        tape_authority,
        TAPE(total_cost),
    );
    instructions.push(build_reserve_tape_ix(
        fee_payer.pubkey(),
        tape_authority,
        StorageUnits(size_mb),
        EpochNumber(start_epoch),
        EpochNumber(end_epoch),
    ));

    let sig = client
        .send_instructions_with_signers(fee_payer, instructions, &[&tape_keypair])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create tape: {}", e))?;

    let (_, keypair_path) = save_tape_keypair(&tape_keypair)?;

    if !ctx.quiet {
        eprintln!("  Transaction: {}", sig);
        eprintln!("  Keypair saved: {}", keypair_path.display());
        eprintln!();
    }

    Ok(tape_keypair)
}

async fn upload_with_certification(
    ctx: &Context,
    file: PathBuf,
    tape_arg: Option<&str>,
    key_arg: Option<String>,
    nodes: Option<Vec<String>>,
    max_slice_bytes: Option<usize>,
    skip_certify: bool,
) -> Result<()> {
    use solana_sdk::signature::Keypair;
    use tape_sdk::create_rpc_client;

    let fee_payer = get_keypair(ctx)?;
    ctx.debug(&format!("Fee payer: {}", fee_payer.pubkey()));

    let data = tokio::fs::read(&file)
        .await
        .with_context(|| format!("Failed to read file: {}", file.display()))?;
    let file_size = data.len();

    let client = create_rpc_client(&ctx.rpc_url()).map_err(|e| anyhow::anyhow!("{}", e))?;

    let authority_keypair: Keypair = if let Some(tape) = tape_arg {
        resolve_authority(tape, AuthorityType::Tape)?
    } else {
        create_tape_for_file(ctx, &client, &fee_payer, file_size).await?
    };
    let authority = authority_keypair.pubkey();
    ctx.debug(&format!("Tape authority: {}", authority));

    // 4. Calculate slice size
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

    // 5. Verify tape exists with capacity
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

    // 7. Discover network state and upload slices
    if !ctx.quiet {
        eprintln!();
        eprintln!("[2/4] Uploading slices to storage nodes...");
    }

    let discovery = discover_network_state(ctx, nodes).await?;

    if !ctx.quiet {
        eprintln!("  Nodes: {}", discovery.node_count());
    }

    // Use track address as track_id (base58 string)
    let track_id = track_address.to_string();

    let tape_client = TapeClient::builder()
        .committee(discovery.committee.clone())
        .spool_assignment(discovery.spool_assignment.clone())
        .node_addresses(discovery.node_addresses.clone())
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
            // committee.iter() only returns active members, so all are valid
            match client.get_node_by_id(member.id).await {
                Ok((_, node)) => {
                    match node.metadata.network_address.to_socket_addr() {
                        Ok(socket_addr) => {
                            ctx.debug(&format!(
                                "Found node {} at {}",
                                member.id.as_u64(),
                                socket_addr
                            ));
                            node_address_map.insert(member.id, format!("http://{}", socket_addr));
                        }
                        Err(e) => {
                            ctx.debug(&format!(
                                "Node {} has invalid network address: {}",
                                member.id.as_u64(),
                                e
                            ));
                        }
                    }
                }
                Err(e) => {
                    ctx.debug(&format!(
                        "Failed to look up node {}: {}",
                        member.id.as_u64(),
                        e
                    ));
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

        // BLS verification is expensive, request higher compute budget
        let compute_budget_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(CERTIFY_TRACK_COMPUTE_UNITS);

        let certify_ix = build_certify_track_ix(
            fee_payer.pubkey(),
            authority,
            key_hash,
            collected.bitmap,
            collected.aggregated_signature,
        );

        let sig = if fee_payer.pubkey() != authority {
            client
                .send_instructions_with_signers(&fee_payer, vec![compute_budget_ix, certify_ix], &[&authority_keypair])
                .await
                .map_err(|e| anyhow::anyhow!("CertifyTrack failed: {}", e))?
        } else {
            client
                .send_instructions(&fee_payer, vec![compute_budget_ix, certify_ix])
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
    outfile: Option<PathBuf>,
    nodes: Option<Vec<String>>,
    verify: Option<String>,
) -> Result<()> {
    use tape_sdk::TapeClient;

    // Discover network state (committee, spool assignment, node addresses)
    let discovery = discover_network_state(ctx, nodes).await?;

    if !ctx.quiet {
        eprintln!("Downloading track {}...", track_id);
    }

    let client = TapeClient::builder()
        .committee(discovery.committee)
        .spool_assignment(discovery.spool_assignment)
        .node_addresses(discovery.node_addresses)
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

    match outfile {
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

    // Discover network state (committee, spool assignment, node addresses)
    let discovery = discover_network_state(ctx, nodes).await?;

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
        .committee(discovery.committee)
        .spool_assignment(discovery.spool_assignment)
        .node_addresses(discovery.node_addresses)
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
