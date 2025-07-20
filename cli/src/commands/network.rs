use anyhow::{bail, Result};
use std::str::FromStr;
use dialoguer::{theme::ColorfulTheme, Confirm};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer, pubkey::Pubkey};

use tape_api::prelude::*;
use tape_client::{register::register_miner, get_miner_account};
use tape_network::{
    archive::archive_loop,
    mine::mine_loop,
    web::web_loop,
};

const DEVNET: &str = "https://devnet.tapedrive.io/api";

use crate::cli::{Cli, Commands};
use crate::log;

pub async fn handle_network_commands(cli: Cli, client: RpcClient, payer: Keypair) -> Result<()> {
    log::print_divider();

    match cli.command {
        Commands::Web { port } => {
            handle_web(port).await?;
        }
        Commands::Archive { starting_slot, trusted_peer } => {
            handle_archive(&client, starting_slot, trusted_peer).await?;
        }
        Commands::Mine { pubkey, name } => {
            handle_mine(&client, &payer, pubkey, name).await?;
        }
        Commands::Register { name } => {
            handle_register(&client, &payer, name).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_web(port: Option<u16>) -> Result<()> {
    let port = port.unwrap_or(3000);

    log::print_info("Starting web RPC service...");
    log::print_message(format!("Listening on port {}", port).as_str());

    let secondary_store = tape_network::store::secondary()?;
    web_loop(secondary_store, port).await?;
    Ok(())
}

async fn handle_archive(client: &RpcClient, starting_slot: Option<u64>, trusted_peer: Option<String>) -> Result<()> {
    // Use the public devnet peer if none is provided
    let trusted_peer = match client.url() {
        url if url.contains("devnet") => {
            Some(trusted_peer.unwrap_or(DEVNET.to_string()))
        }
        _ => trusted_peer
    };

    log::print_info("Starting archive service...");

    let primary_store = tape_network::store::primary()?;
    archive_loop(&primary_store, client, starting_slot, trusted_peer).await?;
    Ok(())
}

async fn handle_mine(client: &RpcClient, payer: &Keypair, pubkey: Option<String>, name: Option<String>) -> Result<()> {
    log::print_info("Starting mining service...");

    let miner_address = resolve_miner(client, payer, pubkey, name, true).await?;

    log::print_message(&format!("Using miner address: {}", miner_address));

    let secondary_store = tape_network::store::secondary()?;
    mine_loop(&secondary_store, client, &miner_address, payer).await?;
    Ok(())
}

async fn handle_register(client: &RpcClient, payer: &Keypair, name: String) -> Result<()> {
    log::print_info("Registering miner...");

    let (miner_address, _) = miner_pda(payer.pubkey(), to_name(&name));

    let proceed = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("→ Are you sure?")
        .default(false)
        .interact()
        .map_err(|e| anyhow::anyhow!("Failed to get user input: {}", e))?;
    if !proceed {
        log::print_error("Write operation cancelled");
        return Ok(());
    }

    register_miner(client, payer, &name).await?;

    log::print_section_header("Miner Registered");
    log::print_message(&format!("Name: {}", name));
    log::print_message(&format!("Address: {}", miner_address));

    log::print_divider();
    log::print_info("More info:");
    log::print_title(&format!("tapedrive get-miner {}", miner_address));
    log::print_divider();
    Ok(())
}

pub async fn resolve_miner(
    client: &RpcClient,
    payer: &Keypair,
    pubkey_opt: Option<String>,
    name_opt: Option<String>,
    auto_register: bool,
) -> Result<Pubkey> {
    let (miner_address, name) = match (pubkey_opt, name_opt) {
        (Some(_), Some(_)) => bail!("Cannot provide both pubkey and name"),
        (Some(p), None) => (Pubkey::from_str(&p)?, None),
        (None, Some(n)) => (miner_pda(payer.pubkey(), to_name(&n)).0, Some(n)),
        (None, None) => (miner_pda(payer.pubkey(), to_name("default")).0, Some("default".to_string())),
    };

    let miner_account = get_miner_account(client, &miner_address).await;

    if miner_account.is_ok() {
        return Ok(miner_address);
    }

    if !auto_register {
        bail!("Miner not registered");
    }

    let Some(name) = name else {
        bail!("Cannot auto-register when pubkey is provided. Please use name instead or register manually.");
    };

    log::print_message("Miner not registered, registering now...");
    register_miner(client, payer, &name).await?;
    log::print_message("Miner registered successfully");
    log::print_message(&format!("Name: {}", name));

    Ok(miner_address)
}
