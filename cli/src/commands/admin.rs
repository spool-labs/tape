
use anyhow::Result;
use dialoguer::{theme::ColorfulTheme, Confirm};

use crate::cli::{Cli, Context, Commands};
use crate::log;

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

    if let Commands::Init {} = cli.command {
        let signature = tape_client::initialize(context.rpc(), context.payer()).await?;
        log::print_section_header("Program Initialized");
        log::print_message(&format!("Signature: {signature}"));
        log::print_divider();
    }
    Ok(())
}

