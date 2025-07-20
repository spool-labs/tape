mod cli;
mod keypair;
mod log;
mod commands;
mod utils;

use anyhow::Result;
use clap::Parser;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

use cli::{Cli, Commands};
use keypair::{ get_payer, get_keypair_path };
use commands::{admin, read, write, info, snapshot, network, claim};
use env_logger::{self, Env};

#[tokio::main]
async fn main() -> Result<()> {

    // setup env_logger
    env_logger::Builder::from_env(Env::default()
        .default_filter_or(format!("tape_network=trace,tape_client=trace"))).init();
    
   
    log::print_title(format!("⊙⊙ TAPEDRIVE {}", env!("CARGO_PKG_VERSION")).as_str());

    let cli = Cli::parse();
    let rpc_url = cli.cluster.rpc_url();
    let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::finalized());
    let keypair_path = get_keypair_path(cli.keypair_path.clone());

    match cli.command {
        Commands::Initialize {} |
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

    log::print_message(&format!("Connected to: {}", rpc_url));

    match cli.command {
        // Admin Commands

        Commands::Initialize { .. } => {
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

        Commands::Register { .. } |
        Commands::Claim { .. } => {
            let payer = get_payer(keypair_path)?;
            claim::handle_claim_command(cli, rpc_client, payer).await?;
        }

        // Network Commands

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
