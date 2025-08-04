use anyhow::{anyhow, Result};
use dialoguer::{theme::ColorfulTheme, Confirm};
use solana_sdk::signer::Signer;

use crate::cli::{Cli, Context, Commands};
use crate::log;

use tape_api::consts::ONE_TAPE;
use tape_client::{
    program::{initialize, airdrop_tokens}, 
    utils::create_ata
};

pub async fn handle_admin_commands(cli:Cli, context: Context) -> Result<()> {

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
        Commands::Init {} => {

            let sig = initialize(context.rpc(), context.payer()).await?;
            log::print_section_header("Program Initialized");
            log::print_message(&format!("Signature: {sig}"));
            log::print_divider();

        },
        Commands::Airdrop { amount } => {

            let (beneficiary_ata, _) = create_ata(context.rpc(), context.payer())
                .await
                .map_err(|e| anyhow!("Failed to create/ensure ATA for payer {}: {}", context.payer().pubkey(), e))?;

            let sig = airdrop_tokens(
                context.rpc(),
                context.payer(),
                beneficiary_ata,
                amount * ONE_TAPE,
            ).await?;

            log::print_section_header("Airdrop Completed");
            log::print_message(&format!("Signature: {sig}"));
            log::print_divider();
        },
        _ => {}
    }

    Ok(())
}

