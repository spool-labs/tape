use anyhow::{anyhow, Result};
use base64;
use log::debug;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig, RpcTransactionConfig},
    rpc_response::RpcConfirmedTransactionStatusWithSignature,
};

use solana_transaction_status::UiConfirmedBlock;
use solana_transaction_status_client_types::TransactionDetails;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::{Transaction, VersionedTransaction},
};

use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use tokio::time::{sleep, Duration};

use crate::utils::{deserialize, serialize, retry, with_logs};

/// Initial backoff duration for retries (milliseconds).
const INITIAL_BACKOFF: u64 = 200;

/// Returns the default transaction configuration for RPC calls.
pub fn rpc_tx_config() -> RpcTransactionConfig {
    RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    }
}

/// Sends a transaction and returns its serialized signature.
pub async fn send_transaction(client: &RpcClient, tx: &Transaction) -> Result<Vec<u8>> {
    let signature: Signature = with_logs(client.send_transaction(tx).await)?;
    serialize(&signature)
}

/// Sends and confirms a transaction, returning its serialized signature.
pub async fn send_and_confirm_transaction(client: &RpcClient, tx: &Transaction) -> Result<Vec<u8>> {
    let signature: Signature = with_logs(client.send_and_confirm_transaction(tx).await)?;
    serialize(&signature)
}

/// Fetches the latest blockhash and returns it serialized.
pub async fn get_latest_blockhash(client: &RpcClient) -> Result<Vec<u8>> {
    let hash: Hash = client.get_latest_blockhash().await?;
    serialize(&hash)
}

/// Fetches a transaction by signature, returning its serialized data.
pub async fn get_transaction(client: &RpcClient, signature: &Signature) -> Result<Vec<u8>> {
    let tx: EncodedConfirmedTransactionWithStatusMeta = client
        .get_transaction_with_config(signature, rpc_tx_config())
        .await?;

    let tx = tx.transaction.transaction;
    let tx = match tx {
        solana_transaction_status::EncodedTransaction::Binary(s, _) => s,
        _ => return Err(anyhow!("Expected binary transaction encoding")),
    };

    let tx = base64::decode(&tx)?;
    Ok(tx)
}

/// Sends a transaction and returns its signature.
pub async fn send(client: &RpcClient, tx: &Transaction) -> Result<Signature> {
    let signature_bytes = send_transaction(client, tx).await?;
    deserialize(&signature_bytes)
}

/// Sends and confirms a transaction, returning its signature.
pub async fn send_and_confirm(client: &RpcClient, tx: &Transaction) -> Result<Signature> {
    let signature_bytes = send_and_confirm_transaction(client, tx).await?;
    deserialize(&signature_bytes)
}

/// Sends a transaction with retry logic, returning its signature.
pub async fn send_with_retry(
    client: &RpcClient,
    instruction: &Instruction,
    payer: &Keypair,
    max_retries: u32,
) -> Result<Signature> {
    let mut attempts = 0;
    loop {
        let blockhash_bytes = get_latest_blockhash(client).await?;
        let recent_blockhash = deserialize(&blockhash_bytes)?;

        let tx = Transaction::new_signed_with_payer(
            &[instruction.clone()],
            Some(&payer.pubkey()),
            &[payer],
            recent_blockhash,
        );

        match send(client, &tx).await {
            Ok(signature) => return Ok(signature),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                let delay_ms = INITIAL_BACKOFF * (1 << attempts);

                debug!(
                    "send_with_retry attempt {}/{}, waiting {}ms: {}",
                    attempts, max_retries, delay_ms, e
                );

                sleep(Duration::from_millis(delay_ms)).await;
                continue;
            }
            Err(e) => {
                return Err(anyhow!(
                    "Failed to send transaction after {} attempts: {}",
                    max_retries,
                    e
                ))
            }
        }
    }
}

/// Fetches a transaction with retry logic, returning the deserialized transaction.
pub async fn get_transaction_with_retry(
    client: &RpcClient,
    signature: &Signature,
    max_retries: u32,
) -> Result<VersionedTransaction> {
    let mut attempts = 0;
    loop {
        match get_transaction(client, signature).await {
            Ok(tx_bytes) => return deserialize(&tx_bytes),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                let delay_ms = INITIAL_BACKOFF * (1 << attempts);
                debug!(
                    "get_transaction_with_retry attempt {}/{}, waiting {}ms: {}",
                    attempts, max_retries, delay_ms, e
                );
                sleep(Duration::from_millis(delay_ms)).await;
                continue;
            }
            Err(e) => {
                return Err(anyhow!(
                    "Failed to fetch transaction {} after {} attempts: {}",
                    signature,
                    max_retries,
                    e
                ))
            }
        }
    }
}

/// Fetches an account by address and returns its serialized data.
pub async fn get_account(client: &RpcClient, address: &Pubkey) -> Result<Vec<u8>> {
    let account: Account = client.get_account(address).await?;
    serialize(&account)
}

/// Fetches program accounts with the given configuration.
pub async fn get_program_account(
    client: &RpcClient,
    config: RpcProgramAccountsConfig,
) -> Result<Vec<(Pubkey, Account)>> {
    client
        .get_program_accounts_with_config(&tape_api::ID, config)
        .await
        .map_err(|e| anyhow!("Failed to fetch program accounts: {}", e))
}

/// Fetches a block by slot number with retry logic, using the specified transaction details.
pub async fn get_block_by_number(
    client: &RpcClient,
    slot_number: u64,
    transaction_details: TransactionDetails,
) -> Result<UiConfirmedBlock> {
    let config = RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Json),
        transaction_details: Some(transaction_details),
        rewards: None,
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    retry(|| async {
        client
            .get_block_with_config(slot_number, config)
            .await
            .map_err(|e| anyhow!("Failed to fetch block {}: {}", slot_number, e))
    })
    .await
}

/// Fetches the latest confirmed block height with retry logic.
pub async fn get_block_height(client: &RpcClient) -> Result<u64> {
    retry(|| async {
        client
            .get_block_height()
            .await
            .map_err(|e| {
                anyhow!("Failed to fetch block height: {}", e)
            })
    })
    .await
}

/// Fetches the current slot with retry logic.
pub async fn get_slot(client: &RpcClient) -> Result<u64> {
    retry(|| async {
        client
            .get_slot()
            .await
            .map_err(|e| {
                anyhow!("Failed to fetch current slot: {}", e)
            })
    })
    .await
}

/// Fetches a list of confirmed slots starting from `start_slot` with a `limit`, using retry logic.
pub async fn get_blocks_with_limit(client: &RpcClient, start_slot: u64, limit: usize) -> Result<Vec<u64>> {
    retry(|| async {
        client
            .get_blocks_with_limit(start_slot, limit)
            .await
            .map_err(|e| {
                anyhow!("Failed to fetch blocks from slot {}: {}", start_slot, e)
            })
    })
    .await
}

/// Fetches transaction signatures for an address with the given configuration, with retry logic.
pub async fn get_signatures_for_address(
    client: &RpcClient,
    address: &Pubkey,
    before: Option<Signature>,
    until: Option<Signature>,
    limit: Option<usize>,
) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
    let address = *address;
    retry(|| async {
        let config = GetConfirmedSignaturesForAddress2Config {
            before,
            until,
            limit,
            commitment: None,
        };
        client
            .get_signatures_for_address_with_config(&address, config)
            .await
            .map_err(|e| anyhow!("Failed to fetch signatures for address {}: {}", address, e))
    })
    .await
}
