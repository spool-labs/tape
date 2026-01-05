//! Tapedrive CLI for uploading and downloading blobs.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tape_crypto::Pubkey;
use tape_sdk::{TapeClient, DATA_SLICES};

/// Minimum slice size to avoid excessive overhead from tiny slices.
const MIN_SLICE_BYTES: usize = 1024; // 1 KB

/// Maximum slice size for upload to prevent excessive memory during encoding.
/// Download auto-detects slice size from stored data.
const MAX_SLICE_BYTES: usize = 256 * 1024; // 256 KB

/// Tapedrive blob storage CLI.
#[derive(Parser, Debug)]
#[command(name = "tape")]
#[command(author, version, about = "Upload and download blobs to tapedrive storage nodes")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Upload a file to storage nodes.
    Upload {
        /// Path to the file to upload.
        file: PathBuf,

        /// Comma-separated list of node URLs.
        #[arg(short, long, value_delimiter = ',')]
        nodes: Vec<String>,

        /// Optional track ID (generates random pubkey if not provided).
        #[arg(short, long)]
        track_id: Option<String>,

        /// Maximum slice size in bytes. If not specified, automatically
        /// calculated based on file size to minimize memory usage.
        #[arg(long)]
        max_slice_bytes: Option<usize>,
    },

    /// Download a blob from storage nodes.
    Download {
        /// Track ID of the blob to download.
        track_id: String,

        /// Comma-separated list of node URLs.
        #[arg(short, long, value_delimiter = ',')]
        nodes: Vec<String>,

        /// Output file path (defaults to stdout if not provided).
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Check health of storage nodes.
    Health {
        /// Comma-separated list of node URLs.
        #[arg(short, long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Upload {
            file,
            nodes,
            track_id,
            max_slice_bytes,
        } => {
            upload_file(file, nodes, track_id, max_slice_bytes).await?;
        }
        Commands::Download {
            track_id,
            nodes,
            output,
        } => {
            download_blob(track_id, nodes, output).await?;
        }
        Commands::Health { nodes } => {
            check_health(nodes).await?;
        }
    }

    Ok(())
}

/// Calculate optimal slice size for a given data size.
///
/// The slice size is chosen to:
/// 1. Be at least MIN_SLICE_BYTES to avoid excessive overhead
/// 2. Be at most MAX_SLICE_BYTES to prevent OOM
/// 3. Be just large enough to fit the data in DATA_SLICES slices
fn calculate_slice_size(data_len: usize) -> usize {
    if data_len == 0 {
        return MIN_SLICE_BYTES;
    }

    // Minimum slice size needed: ceil(data_len / DATA_SLICES)
    let min_needed = (data_len + DATA_SLICES - 1) / DATA_SLICES;

    // Clamp to our bounds
    min_needed.clamp(MIN_SLICE_BYTES, MAX_SLICE_BYTES)
}

async fn upload_file(
    file: PathBuf,
    nodes: Vec<String>,
    track_id: Option<String>,
    max_slice_bytes: Option<usize>,
) -> Result<()> {
    if nodes.is_empty() {
        anyhow::bail!("At least one node URL is required");
    }

    // Read file
    let data = tokio::fs::read(&file)
        .await
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    let file_size = data.len();

    // Calculate or use provided slice size
    let slice_size = max_slice_bytes.unwrap_or_else(|| calculate_slice_size(file_size));
    eprintln!(
        "Uploading {} ({} bytes, slice size: {} bytes)...",
        file.display(),
        file_size,
        slice_size
    );

    // Generate or use provided track ID (must be valid base58 pubkey)
    let track_id = track_id.unwrap_or_else(|| {
        let bytes: [u8; 32] = rand::random();
        Pubkey::new_from_array(bytes).to_string()
    });

    // Create client
    let client = TapeClient::builder()
        .node_addresses(nodes.clone())
        .max_slice_bytes(slice_size)
        .build();

    // Upload
    let root = client
        .upload_blob(&track_id, data)
        .await
        .context("Failed to upload blob")?;

    eprintln!("Upload complete!");
    eprintln!();
    println!("Track ID: {}", track_id);
    println!("Merkle Root: {}", hex::encode(root));
    println!("Nodes: {}", nodes.join(", "));

    Ok(())
}

async fn download_blob(
    track_id: String,
    nodes: Vec<String>,
    output: Option<PathBuf>,
) -> Result<()> {
    if nodes.is_empty() {
        anyhow::bail!("At least one node URL is required");
    }

    eprintln!("Downloading track {}...", track_id);

    // Create client - slice size is auto-detected from stored slices
    let client = TapeClient::builder()
        .node_addresses(nodes)
        .build();

    // Download
    let data = client
        .download_blob(&track_id)
        .await
        .context("Failed to download blob")?;

    eprintln!("Download complete! ({} bytes)", data.len());

    // Write output
    match output {
        Some(path) => {
            tokio::fs::write(&path, &data)
                .await
                .with_context(|| format!("Failed to write file: {}", path.display()))?;
            eprintln!("Written to: {}", path.display());
        }
        None => {
            // Write to stdout
            use std::io::Write;
            std::io::stdout()
                .write_all(&data)
                .context("Failed to write to stdout")?;
        }
    }

    Ok(())
}

async fn check_health(nodes: Vec<String>) -> Result<()> {
    if nodes.is_empty() {
        anyhow::bail!("At least one node URL is required");
    }

    let client = TapeClient::builder()
        .node_addresses(nodes.clone())
        .build();

    eprintln!("Checking health of {} nodes...", nodes.len());
    eprintln!();

    for node in &nodes {
        match client.health_check(node).await {
            Ok(true) => println!("{}: OK", node),
            Ok(false) => println!("{}: UNHEALTHY", node),
            Err(e) => println!("{}: ERROR - {}", node, e),
        }
    }

    Ok(())
}
