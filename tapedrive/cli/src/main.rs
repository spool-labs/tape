//! Tapedrive CLI for uploading and downloading blobs.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tape_crypto::Pubkey;
use tape_sdk::TapeClient;

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

        /// Optional track ID (generates UUID if not provided).
        #[arg(short, long)]
        track_id: Option<String>,

        /// Maximum slice size in bytes (default: 1MiB).
        #[arg(long, default_value = "1048576")]
        max_slice_bytes: usize,
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

async fn upload_file(
    file: PathBuf,
    nodes: Vec<String>,
    track_id: Option<String>,
    max_slice_bytes: usize,
) -> Result<()> {
    if nodes.is_empty() {
        anyhow::bail!("At least one node URL is required");
    }

    // Read file
    let data = tokio::fs::read(&file)
        .await
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    let file_size = data.len();
    eprintln!("Uploading {} ({} bytes)...", file.display(), file_size);

    // Generate or use provided track ID (must be valid base58 pubkey)
    let track_id = track_id.unwrap_or_else(|| {
        let bytes: [u8; 32] = rand::random();
        Pubkey::new_from_array(bytes).to_string()
    });

    // Create client
    let client = TapeClient::builder()
        .node_addresses(nodes.clone())
        .max_slice_bytes(max_slice_bytes)
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

    // Create client
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
