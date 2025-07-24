use std::sync::Arc;

use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::instruction::create_associated_token_account;

use crate::utils::{deserialize, get_latest_blockhash, send_and_confirm_transaction};

pub async fn create_ata(
    client: &Arc<RpcClient>,
    payer: &Keypair,
) -> Result<(Pubkey, Signature)> {
    let token_program_id = &spl_token::ID;
    let mint             = &tape_api::MINT_ADDRESS;
    let payer_pk         = payer.pubkey();
    let owner            = &payer_pk;

    let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        owner,
        mint,
        token_program_id,
    );

    // Check if ATA already exists
    match client.get_account(&ata).await {
        Ok(account) => {
            if account.owner == *token_program_id {
                return Ok((ata, Signature::default()));
            } else {
                return Err(anyhow!("Account {} exists but is owned by {}, not the expected token program {}", ata, account.owner, token_program_id));
            }
        }
        Err(_) => {
            // Account doesn't exist, proceed with creation
        }
    }

    // Check if mint is valid
    match client.get_account(mint).await {
        Ok(account) if account.owner == *token_program_id => {
            // Mint is valid
        }
        Ok(account) => {
            return Err(anyhow!("Mint {} is owned by {}, not the expected token program {}", mint, account.owner, token_program_id));
        }
        Err(e) => {
            return Err(anyhow!("Failed to fetch mint {}: {}", mint, e));
        }
    }

    // Check if payer has sufficient balance
    let rent = client.get_minimum_balance_for_rent_exemption(165).await?;
    let payer_balance = client.get_balance(&payer_pk).await?;
    if payer_balance < rent {
        return Err(anyhow!("Payer {} has insufficient balance: {} lamports, required: {} lamports", payer_pk, payer_balance, rent));
    }

    let create_ata_ix = create_associated_token_account(&payer_pk, owner, mint, token_program_id);

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let tx = Transaction::new_signed_with_payer(
        &[create_ata_ix],
        Some(&payer_pk),
        &[payer],
        recent_blockhash,
    );

    let signature_bytes = send_and_confirm_transaction(client, &tx)
        .await
        .map_err(|e| {
            let error_msg = format!("Failed to create ATA {ata} for mint {mint} and owner {owner}: {e}");
            eprintln!("{error_msg}");
            anyhow!(error_msg)
        })?;
    let signature: Signature = deserialize(&signature_bytes)?;

    Ok((ata, signature))
}
