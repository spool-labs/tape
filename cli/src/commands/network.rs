use anyhow::{bail, Result};
use std::str::FromStr;
use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer, pubkey::Pubkey};

use tape_api::prelude::*;
use tape_client::{register::register_miner, get_miner_account};
use tape_network::{
    archive,
    mine::mine_loop,
    web::web_loop,
};

const DEVNET: &str = "https://devnet.tapedrive.io/api";

use crate::cli::{Cli, Commands, Context};
use crate::log;

pub async fn handle_network_commands(cli: Cli, context: Context) -> Result<()> {
    log::print_divider();

    match cli.command {
        Commands::Web { port } => {
            handle_web(context, port).await?;
        }
        Commands::Archive { trusted_peer, miner_address } => {
            handle_archive(context, trusted_peer, miner_address).await?;
        }
        Commands::Mine { pubkey, name } => {
            handle_mine(context, pubkey, name).await?;
        }
        Commands::Register { name } => {
            handle_register(context, name).await?;
        }
        _ => {}
    }
    Ok(())
}

pub async fn handle_web(context: Context, port: Option<u16>) -> Result<()> {
    let port = port.unwrap_or(3000);

    log::print_info("Starting web RPC service...");
    log::print_message(format!("Listening on port {port}").as_str());
    let store = context.open_secondary_store_conn_web()?;
    web_loop(store, port).await?;
    Ok(())
}

pub async fn handle_archive(
    context: Context,
    trusted_peer: Option<String>,
    miner_address: Option<String>
) -> Result<()> {
    log::print_info("Starting archive service...");

    // Use the public devnet peer if none is provided
    let trusted_peer = match context.rpc().url() {
        url if url.contains("devnet") => {
            Some(trusted_peer.unwrap_or(DEVNET.to_string()))
        }
        _ => trusted_peer
    };

    let miner = get_or_create_miner(
        context.rpc(), 
        context.payer(), 
        miner_address, 
        None, 
        true
    ).await?;

    log::print_message(&format!("Using miner address: {miner}"));

    let rpc_client = context.rpc().clone();
    let store      = Arc::new(context.open_primary_store_conn()?);

    archive::orchestrator::run(miner, store, rpc_client, trusted_peer).await?;

    Ok(())
}

pub async fn handle_mine(
    context: Context,
    miner_address: Option<String>,
    miner_name: Option<String>
) -> Result<()> {

    log::print_info("Starting mining service...");

    let miner_address = get_or_create_miner(
        context.rpc(), context.payer(), miner_address, miner_name, true).await?;

    log::print_message(&format!("Using miner address: {miner_address}"));

    let store = context.open_secondary_store_conn_mine()
        .map_err(|e| anyhow::anyhow!("Failed to open archive db: {}, are you running the archive service?", e))?;

    log::print_info("Starting mine service...");

    mine_loop(store, context.rpc(), &miner_address, context.payer()).await?;
    Ok(())
}

pub async fn handle_register(
    context: Context,
    name: String
) -> Result<()> {

    log::print_info("Registering miner...");

    let (miner_address, _) = miner_find_pda(&context.payer().pubkey(), to_name(&name));

    register_miner(context.rpc(), context.payer(), &name).await?;

    log::print_section_header("Miner Registered");
    log::print_message(&format!("Name: {name}"));
    log::print_message(&format!("Address: {miner_address}"));

    log::print_divider();
    log::print_info("More info:");
    log::print_title(&format!("tapedrive get-miner {miner_address}"));
    log::print_divider();
    Ok(())
}

pub async fn get_or_create_miner(
    client: &Arc<RpcClient>,
    payer: &Keypair,
    pubkey_opt: Option<String>,
    name_opt: Option<String>,
    auto_register: bool,
) -> Result<Pubkey> {
    let (miner_address, name) = match (pubkey_opt, name_opt) {
        (Some(_), Some(_)) => bail!("Cannot provide both pubkey and name"),
        (Some(p), None) => (Pubkey::from_str(&p)?, None),
        (None, Some(n)) => (miner_find_pda(&payer.pubkey(), to_name(&n)).0, Some(n)),
        (None, None) => (miner_find_pda(&payer.pubkey(), to_name("default")).0, Some("default".to_string())),
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
    log::print_message(&format!("Name: {name}"));

    Ok(miner_address)
}
