mod cli;
mod keypair;
mod log;
mod commands;
mod utils;
mod config;


use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use commands::{admin, read, write, info, snapshot, network, claim};
use env_logger::{self, Env};
use tape_network::store::TapeStore;

use crate::cli::Context;
use crate::config::{TapeConfig, TapeConfigError};


fn main() -> Result<()>{
    // setup env_logger
    env_logger::Builder::from_env(Env::default()
        .default_filter_or("tape_network=trace,tape_client=trace".to_string())).init();
    
    let num_threads = num_cpus::get();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(run_tape_cli())
}


async fn run_tape_cli() -> Result<()> {

    log::print_title(format!("⊙⊙ TAPEDRIVE {}", env!("CARGO_PKG_VERSION")).as_str());

    let cli = Cli::parse();

    let config = match TapeConfig::load(&cli.config) {
        Ok(config) => config,
        Err(e) => match e {
            TapeConfigError::ConfigFileNotFound => {
                log::print_info("tape.toml not found, creating default configuration...");
                match TapeConfig::create_default() {
                    Ok(config) => {
                        log::print_info("✓ Default configuration created successfully");
                        config
                    },
                    Err(creation_error) => {
                        log::print_error(&format!("{}", creation_error));
                        std::process::exit(1);
                    }
                }
            },

            TapeConfigError::CustomConfigFileNotFound(path) => {
            // This happens when user explicitly provided a path that doesn't exist
            log::print_error(&format!("Custom config file not found: {}", path));
            log::print_info("Please check the path and try again.");
            std::process::exit(1);
        },
            
            TapeConfigError::InvalidUrl(msg) => {
                log::print_error(&format!("URL Configuration Error: {}", msg));
                log::print_info("Please fix the URL in your tape.toml file and try again.");
                std::process::exit(1);
            },
            
            TapeConfigError::KeypairNotFound(path) => {
                log::print_error(&format!("Keypair not found at path: {}", path));
                log::print_info("Please ensure the keypair file exists at the specified path in tape.toml");
                std::process::exit(1);
            },
            
            TapeConfigError::FileReadError(io_err) => {
                log::print_error(&format!("Could not read config file: {}", io_err));
                std::process::exit(1);
            },
            
            TapeConfigError::ParseError(parse_err) => {
                log::print_error(&format!("Invalid tape.toml format: {}", parse_err));
                log::print_info("Please check your tape.toml file syntax.");
                std::process::exit(1);
            },
            
            TapeConfigError::HomeDirectoryNotFound => {
                log::print_error("Could not determine home directory");
                std::process::exit(1);
            },
            
            TapeConfigError::DefaultConfigCreationFailed(msg) => {
                log::print_error(&format!("Failed to create default config: {}", msg));
                std::process::exit(1);
            },
        }
    };

    let context = Context::try_build(&cli, &config)?;

    match cli.command {
        Commands::Init {} |
        Commands::Write { .. } | 
        Commands::Register { .. } |
        Commands::Mine { .. }
        => {
            log::print_message(&format!(
                "Using keypair from {}",
                context.keyapir_path().display()
                
            ));
        }
        _ => {}
    }

    log::print_message(&format!("Connected to: {}", context.rpc().url()));

    match cli.command {
        // Admin Commands

        Commands::Init { .. } |
        Commands::Airdrop { .. } 
        => {
            admin::handle_admin_commands(cli, context).await?;
        }

        // Tape Commands

        Commands::Read { .. } => {
            read::handle_read_command(cli, context).await?;
        }
        Commands::Write { .. } => {
            write::handle_write_command(cli, context).await?;
        }

        // Miner Commands

        Commands::Claim { .. } => {
            claim::handle_claim_command(cli, context).await?;
        }

        // Network Commands

        Commands::Register { .. } |
        Commands::Web { .. } |
        Commands::Archive { .. } |
        Commands::Mine { .. } => {
            TapeStore::try_init_store()?;
            network::handle_network_commands(cli, context).await?;
        }

        // Info Commands
        Commands::Info(_) => {
            info::handle_info_commands(cli, context).await?;
        }

        // Store Commands
        Commands::Snapshot(_) => {
            snapshot::handle_snapshot_commands(cli, context).await?;
        }
    }

    Ok(())
}
