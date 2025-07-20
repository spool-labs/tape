use anyhow::{anyhow, Result};
use std::str::FromStr;
use std::sync::Arc;
use dialoguer::{theme::ColorfulTheme, Confirm};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer, pubkey::Pubkey};

use crate::cli::{Cli, Commands};
use crate::log;
use tape_client::{claim::claim_rewards, utils::create_ata};

pub async fn handle_claim_command(
    cli: Cli,
    client: Arc<RpcClient>,
    payer: Keypair,
) -> Result<()> {
    if let Commands::Claim { miner, amount } = cli.command {
        log::print_divider();
        log::print_info("Claiming rewards...");

        // Parse miner public key
        let miner_pubkey = Pubkey::from_str(&miner)
            .map_err(|e| anyhow!("Invalid miner pubkey '{}': {}", miner, e))?;

        // Ensure payer's associated token account (ATA) exists for the mint
        let (beneficiary_ata, ata_sig) = create_ata(&client, &payer)
            .await
            .map_err(|e| anyhow!("Failed to create/ensure ATA for payer {}: {}", payer.pubkey(), e))?;

        // Log ATA creation
        if ata_sig != solana_sdk::signature::Signature::default() {
            log::print_message(&format!("Created ATA {} (payer), signature {}", beneficiary_ata, ata_sig));
        }

        log::print_message(&format!(
            "Miner: {}\n→ Beneficiary ATA (payer): {}\n→ Amount: {}",
            miner_pubkey, beneficiary_ata, amount
        ));

        // Confirm action with the user
        let proceed = Confirm::with_theme(&ColorfulTheme::default())
            .with_prompt("→ Proceed with claim?")
            .default(false)
            .interact()
            .map_err(|e| anyhow!("Failed to get user input: {}", e))?;
        if !proceed {
            log::print_error("Claim cancelled");
            return Ok(());
        }

        // Execute claim using the ensured ATA
        let signature = claim_rewards(&client, &payer, miner_pubkey, beneficiary_ata, amount)
            .await
            .map_err(|e| anyhow!("Failed to claim rewards: {}", e))?;

        log::print_section_header("Claim Submitted");
        log::print_message(&format!("Signature: {}", signature));
        log::print_divider();
    }
    Ok(())
}
