//! Configuration management commands.

use anyhow::Result;
use clap::Subcommand;

use crate::Context;
use crate::config::{ConfigFile, default_config_path, file::default_config_content};

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Initialize config file.
    Init,

    /// Display current configuration.
    Show,

    /// Set a config value.
    Set {
        /// Config key (e.g., cluster, node.commission).
        key: String,
        /// Value to set.
        value: String,
    },

    /// Get a config value.
    Get {
        /// Config key.
        key: String,
    },
}

pub async fn execute(ctx: &Context, cmd: ConfigCommand) -> Result<()> {
    match cmd {
        ConfigCommand::Init => init(ctx).await,
        ConfigCommand::Show => show(ctx).await,
        ConfigCommand::Set { key, value } => set(ctx, &key, &value).await,
        ConfigCommand::Get { key } => get(ctx, &key).await,
    }
}

async fn init(_ctx: &Context) -> Result<()> {
    let path = default_config_path();

    if path.exists() {
        println!("Config file already exists at: {}", path.display());
        println!("Use `tape config show` to view current configuration.");
        return Ok(());
    }

    // Create parent directory
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write default config
    std::fs::write(&path, default_config_content())?;

    println!("Created config file at: {}", path.display());
    println!("Edit this file to configure your defaults.");
    Ok(())
}

async fn show(ctx: &Context) -> Result<()> {
    println!("Cluster: {}", ctx.cluster);
    println!("RPC URL: {}", ctx.rpc_url());

    if let Some(keypair) = &ctx.keypair {
        println!("Keypair: {}", keypair.display());
    }

    if !ctx.nodes.is_empty() {
        println!("Nodes:");
        for node in &ctx.nodes {
            println!("  - {}", node);
        }
    }

    println!("Output: {:?}", ctx.output);
    Ok(())
}

async fn set(_ctx: &Context, key: &str, value: &str) -> Result<()> {
    let path = default_config_path();
    let mut config = ConfigFile::load_from(&path)?;

    match key {
        "cluster" => config.cluster = Some(value.to_string()),
        "output" => config.output = Some(value.to_string()),
        "log_level" => config.log_level = Some(value.to_string()),
        _ => anyhow::bail!("Unknown config key: {}", key),
    }

    config.save_to(&path)?;
    println!("Set {} = {}", key, value);
    Ok(())
}

async fn get(ctx: &Context, key: &str) -> Result<()> {
    let value = match key {
        "cluster" => ctx.config.cluster.clone(),
        "output" => ctx.config.output.clone(),
        "log_level" => ctx.config.log_level.clone(),
        _ => anyhow::bail!("Unknown config key: {}", key),
    };

    match value {
        Some(v) => println!("{}", v),
        None => println!("(not set)"),
    }
    Ok(())
}
