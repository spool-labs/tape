use anyhow::Result;
use dialoguer::{theme::ColorfulTheme, Confirm};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;

use crate::cli::{Cli, Commands};
use crate::log;

pub async fn handle_admin_commands(cli: Cli, client: RpcClient, payer: Keypair) -> Result<()> {

    log::print_divider();
    let proceed = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("→ Are you sure?")
        .default(false)
        .interact()
        .map_err(|e| anyhow::anyhow!("Failed to get user input: {}", e))?;
    if !proceed {
        log::print_error("Write operation cancelled");
        return Ok(());
    }

    match cli.command {
        Commands::Initialize {} => {
            let signature = tape_client::initialize(&client, &payer).await?;
            log::print_section_header("Program Initialized");
            log::print_message(&format!("Signature: {}", signature));
            log::print_divider();
        }

        _ => {}
    }
    Ok(())
}

