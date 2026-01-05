//! Key management commands.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use solana_sdk::signer::Signer;

use crate::config::expand_path;
use crate::Context;

/// Default keys directory.
fn keys_dir() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("keys"))
        .unwrap_or_else(|| PathBuf::from(".tape/keys"))
}

#[derive(Subcommand, Debug)]
pub enum KeysCommand {
    /// Generate a new keypair.
    Generate {
        /// Key type (ed25519, bls).
        #[arg(short, long, default_value = "ed25519")]
        r#type: String,

        /// Name for the keypair.
        #[arg(short, long)]
        name: Option<String>,

        /// Output file path (defaults to ~/.tape/keys/<name>.json).
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Import an existing keypair.
    Import {
        /// Path to keypair file.
        file: PathBuf,

        /// Name for the keypair.
        #[arg(short, long)]
        name: Option<String>,
    },

    /// Export a keypair.
    Export {
        /// Name of the keypair.
        name: String,

        /// Output file path.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// List managed keypairs.
    List,

    /// Show public key for a keypair.
    Show {
        /// Name of the keypair or path to file.
        name: Option<String>,
    },

    /// Show the configured default keypair.
    Default,
}

pub async fn execute(ctx: &Context, cmd: KeysCommand) -> Result<()> {
    match cmd {
        KeysCommand::Generate { r#type, name, output } => {
            generate(ctx, &r#type, name, output).await
        }
        KeysCommand::Import { file, name } => {
            import(ctx, &file, name).await
        }
        KeysCommand::Export { name, output } => {
            export(ctx, &name, output).await
        }
        KeysCommand::List => list(ctx).await,
        KeysCommand::Show { name } => show(ctx, name).await,
        KeysCommand::Default => show_default(ctx).await,
    }
}

async fn generate(
    ctx: &Context,
    key_type: &str,
    name: Option<String>,
    output: Option<PathBuf>,
) -> Result<()> {
    match key_type {
        "ed25519" => generate_ed25519(ctx, name, output).await,
        "bls" => {
            anyhow::bail!("BLS key generation not yet implemented")
        }
        _ => anyhow::bail!("Unknown key type: {}. Use ed25519 or bls.", key_type),
    }
}

async fn generate_ed25519(
    _ctx: &Context,
    name: Option<String>,
    output: Option<PathBuf>,
) -> Result<()> {
    use solana_sdk::signature::Keypair;

    // Generate keypair
    let keypair = Keypair::new();
    let pubkey = keypair.pubkey();

    // Determine output path
    let output_path = match output {
        Some(p) => p,
        None => {
            let name = name.unwrap_or_else(|| pubkey.to_string()[..8].to_string());
            let dir = keys_dir();
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create keys directory: {}", dir.display()))?;
            dir.join(format!("{}.json", name))
        }
    };

    // Serialize to Solana CLI format (JSON array of bytes)
    let bytes = keypair.to_bytes();
    let json = serde_json::to_string(&bytes.to_vec())?;

    std::fs::write(&output_path, &json)
        .with_context(|| format!("Failed to write keypair to {}", output_path.display()))?;

    println!("Generated new Ed25519 keypair");
    println!("Public key: {}", pubkey);
    println!("Saved to: {}", output_path.display());

    Ok(())
}

async fn import(_ctx: &Context, file: &PathBuf, name: Option<String>) -> Result<()> {
    use solana_sdk::signature::Keypair;

    // Read and parse keypair
    let contents = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read keypair file: {}", file.display()))?;

    let bytes: Vec<u8> = serde_json::from_str(&contents)
        .with_context(|| "Failed to parse keypair file (expected JSON array of bytes)")?;

    let keypair = Keypair::from_bytes(&bytes)
        .map_err(|e| anyhow::anyhow!("Invalid keypair data: {}", e))?;

    let pubkey = keypair.pubkey();

    // Determine destination
    let name = name.unwrap_or_else(|| pubkey.to_string()[..8].to_string());
    let dest = keys_dir().join(format!("{}.json", name));

    std::fs::create_dir_all(keys_dir())?;
    std::fs::copy(file, &dest)
        .with_context(|| format!("Failed to copy keypair to {}", dest.display()))?;

    println!("Imported keypair as '{}'", name);
    println!("Public key: {}", pubkey);
    println!("Saved to: {}", dest.display());

    Ok(())
}

async fn export(_ctx: &Context, name: &str, output: Option<PathBuf>) -> Result<()> {
    let source = keys_dir().join(format!("{}.json", name));

    if !source.exists() {
        anyhow::bail!("Keypair '{}' not found at {}", name, source.display());
    }

    let dest = output.unwrap_or_else(|| PathBuf::from(format!("{}.json", name)));

    std::fs::copy(&source, &dest)
        .with_context(|| format!("Failed to export keypair to {}", dest.display()))?;

    println!("Exported '{}' to {}", name, dest.display());

    Ok(())
}

async fn list(_ctx: &Context) -> Result<()> {
    use solana_sdk::signature::Keypair;

    let dir = keys_dir();

    if !dir.exists() {
        println!("No keys directory found at {}", dir.display());
        println!("Use `tape keys generate` to create a new keypair.");
        return Ok(());
    }

    let entries = std::fs::read_dir(&dir)
        .with_context(|| format!("Failed to read keys directory: {}", dir.display()))?;

    println!("{:<20} {}", "Name", "Public Key");
    println!("{}", "-".repeat(70));

    let mut count = 0;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map_or(false, |e| e == "json") {
            let name = path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            // Try to load and show pubkey
            if let Ok(contents) = std::fs::read_to_string(&path) {
                if let Ok(bytes) = serde_json::from_str::<Vec<u8>>(&contents) {
                    if let Ok(keypair) = Keypair::from_bytes(&bytes) {
                        println!("{:<20} {}", name, keypair.pubkey());
                        count += 1;
                    }
                }
            }
        }
    }

    if count == 0 {
        println!("(no keypairs found)");
    }

    Ok(())
}

async fn show(ctx: &Context, name: Option<String>) -> Result<()> {
    use solana_sdk::signature::Keypair;

    let path = match name {
        Some(n) => {
            // Check if it's a path or a name
            let p = PathBuf::from(&n);
            if p.exists() {
                p
            } else {
                keys_dir().join(format!("{}.json", n))
            }
        }
        None => {
            // Use default keypair from config
            ctx.keypair.clone()
                .ok_or_else(|| anyhow::anyhow!("No keypair specified and no default configured"))?
        }
    };

    let path = expand_path(&path.to_string_lossy());

    if !path.exists() {
        anyhow::bail!("Keypair not found at {}", path.display());
    }

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read keypair: {}", path.display()))?;

    let bytes: Vec<u8> = serde_json::from_str(&contents)
        .with_context(|| "Failed to parse keypair file")?;

    let keypair = Keypair::from_bytes(&bytes)
        .map_err(|e| anyhow::anyhow!("Invalid keypair data: {}", e))?;

    println!("Path: {}", path.display());
    println!("Public key: {}", keypair.pubkey());

    Ok(())
}

async fn show_default(ctx: &Context) -> Result<()> {
    match &ctx.keypair {
        Some(path) => {
            println!("Default keypair: {}", path.display());
            show(ctx, Some(path.to_string_lossy().to_string())).await
        }
        None => {
            println!("No default keypair configured.");
            println!("Set one with: tape config set keys.default /path/to/keypair.json");
            Ok(())
        }
    }
}
