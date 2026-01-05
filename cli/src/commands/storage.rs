//! Blob upload/download commands.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;

use crate::Context;

use tape_sdk::{discover_committee_addresses, RpcConfig};

/// Minimum slice size to avoid excessive overhead.
const MIN_SLICE_BYTES: usize = 1024;

/// Maximum slice size for upload.
const MAX_SLICE_BYTES: usize = 256 * 1024;

#[derive(Subcommand, Debug)]
pub enum StorageCommand {
    /// Upload a file to storage nodes.
    Upload {
        /// Path to file to upload.
        file: PathBuf,

        /// Custom track ID (generates random if not provided).
        #[arg(short, long)]
        track_id: Option<String>,

        /// Override storage nodes.
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,

        /// Maximum slice size in bytes.
        #[arg(long)]
        max_slice_bytes: Option<usize>,

        /// Also register track on-chain.
        #[arg(long)]
        register: bool,
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
        StorageCommand::Upload { file, track_id, nodes, max_slice_bytes, register } => {
            upload(ctx, file, track_id, nodes, max_slice_bytes, register).await
        }
        StorageCommand::Download { track_id, output, nodes, verify } => {
            download(ctx, &track_id, output, nodes, verify).await
        }
        StorageCommand::Verify { track_id, root, nodes } => {
            verify(ctx, &track_id, &root, nodes).await
        }
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

async fn upload(
    ctx: &Context,
    file: PathBuf,
    track_id: Option<String>,
    nodes: Option<Vec<String>>,
    max_slice_bytes: Option<usize>,
    _register: bool,
) -> Result<()> {
    use tape_crypto::Pubkey;
    use tape_sdk::TapeClient;

    // Resolve node addresses (auto-discover or use override/config)
    let nodes = resolve_node_addresses(ctx, nodes).await?;

    // Read file
    let data = tokio::fs::read(&file)
        .await
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    let file_size = data.len();
    let slice_size = max_slice_bytes.unwrap_or_else(|| calculate_slice_size(file_size));

    if !ctx.quiet {
        eprintln!(
            "Uploading {} ({} bytes, slice size: {} bytes)...",
            file.display(),
            file_size,
            slice_size
        );
    }

    // Generate or use provided track ID
    let track_id = track_id.unwrap_or_else(|| {
        let bytes: [u8; 32] = rand::random();
        Pubkey::new_from_array(bytes).to_string()
    });

    // Create client and upload
    let client = TapeClient::builder()
        .node_addresses(nodes.clone())
        .max_slice_bytes(slice_size)
        .build();

    let root = client
        .upload_blob(&track_id, data)
        .await
        .context("Failed to upload blob")?;

    if !ctx.quiet {
        eprintln!("Upload complete!");
        eprintln!();
    }

    println!("Track ID: {}", track_id);
    println!("Merkle Root: {}", hex::encode(root));
    println!("Nodes: {}", nodes.join(", "));

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
