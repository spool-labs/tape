mod cli;
mod keypair;
mod log;
mod commands;
mod utils;

use std::sync::Arc;

use anyhow::{Ok, Result};
use clap::Parser;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use cli::{Cli, Commands};
use keypair::{ get_payer, get_keypair_path };
use commands::{admin, read, write, info, snapshot, network, claim};
use env_logger::{self, Env};


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
    let rpc_url = cli.cluster.rpc_url();
    let rpc_client = Arc::new(RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::finalized()));
    let keypair_path = get_keypair_path(cli.keypair_path.clone());

    match cli.command {
        Commands::Init {} |
        Commands::Write { .. } | 
        Commands::Register { .. } |
        Commands::Mine { .. }
        => {
            log::print_message(&format!(
                "Using keypair from {}",
                keypair_path.display()
            ));
        }
        _ => {}
    }

    log::print_message(&format!("Connected to: {rpc_url}"));

    match cli.command {
        // Admin Commands

        Commands::Init { .. } => {
            let payer = get_payer(keypair_path)?;
            admin::handle_admin_commands(cli, rpc_client, payer).await?;
        }

        // Tape Commands

        Commands::Read { .. } => {
            read::handle_read_command(cli, rpc_client).await?;
        }
        Commands::Write { .. } => {
            let payer = get_payer(keypair_path)?;
            write::handle_write_command(cli, rpc_client, payer).await?;
        }

        // Miner Commands

        Commands::Claim { .. } => {
            let payer = get_payer(keypair_path)?;
            claim::handle_claim_command(cli, rpc_client, payer).await?;
        }

        // Network Commands

        Commands::Register { .. } |
        Commands::Web { .. } |
        Commands::Archive { .. } |
        Commands::Mine { .. } => {
            let payer = get_payer(keypair_path)?;
            network::handle_network_commands(cli, rpc_client, payer).await?;
        }

        // Info Commands
        Commands::Info(_) => {
            let payer = get_payer(keypair_path)?;
            info::handle_info_commands(cli, rpc_client, payer).await?;
        }

        // Store Commands
        Commands::Snapshot(_) => {
            snapshot::handle_snapshot_commands(cli, rpc_client).await?;
        }
    }

    Ok(())
}
