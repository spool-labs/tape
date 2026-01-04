//! # rpc-test
//!
//! Test RPC implementation using Solana's TestValidator.
//!
//! This crate provides `TestRpc`, an implementation of the `Rpc` trait that wraps
//! a local test validator. Use this for integration tests that need to interact
//! with a real Solana validator without connecting to devnet/mainnet.
//!
//! ## Pattern
//!
//! This follows the same pattern as `store-memory/` in `tapedrive/archive/`:
//! ```text
//! tape-rpc (trait)  →  TapeRpcClient (production) | TestRpc (testing)
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use rpc_test::TestRpc;
//! use solana_test_validator::TestValidatorGenesis;
//! use tape_rpc::Rpc;
//!
//! #[tokio::test]
//! async fn test_rpc_operations() {
//!     let (validator, _payer) = TestValidatorGenesis::default()
//!         .start_async()
//!         .await;
//!
//!     let rpc = TestRpc::new(&validator);
//!     let slot = rpc.get_slot().await.unwrap();
//!     assert!(slot >= 0);
//! }
//! ```

use async_trait::async_trait;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig};
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use solana_test_validator::TestValidator;
use solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding};
use tape_rpc::{Rpc, RpcError};

/// Test RPC implementation wrapping a TestValidator
///
/// This is the test equivalent of `TapeRpcClient`. It provides the same interface
/// but delegates to a local test validator instead of a remote RPC endpoint.
///
/// Unlike `TapeRpcClient`, this implementation:
/// - Does not implement retry/failover (not needed for local validator)
/// - Does not track metrics
/// - Returns errors directly without classification
pub struct TestRpc {
    client: RpcClient,
}

impl TestRpc {
    /// Create a new TestRpc from a running TestValidator
    ///
    /// The validator must already be started. The RPC client will connect
    /// to the validator's RPC endpoint.
    pub fn new(validator: &TestValidator) -> Self {
        Self {
            client: validator.get_async_rpc_client(),
        }
    }

    /// Create a new TestRpc from an RPC URL
    ///
    /// This is useful when you have the URL but not the validator reference.
    pub fn from_url(url: &str) -> Self {
        Self {
            client: RpcClient::new_with_commitment(
                url.to_string(),
                CommitmentConfig::confirmed(),
            ),
        }
    }

    /// Get a reference to the underlying RPC client
    pub fn client(&self) -> &RpcClient {
        &self.client
    }
}

#[async_trait]
impl Rpc for TestRpc {
    fn commitment(&self) -> solana_sdk::commitment_config::CommitmentLevel {
        // TestRpc always uses Confirmed commitment
        solana_sdk::commitment_config::CommitmentLevel::Confirmed
    }

    async fn get_slot(&self) -> Result<u64, RpcError> {
        self.client
            .get_slot()
            .await
            .map_err(|e| RpcError::Request(e))
    }

    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        self.client
            .get_latest_blockhash()
            .await
            .map_err(|e| RpcError::Request(e))
    }

    async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Json),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        self.client
            .get_block_with_config(slot, config)
            .await
            .map_err(|e| RpcError::Request(e))
    }

    async fn get_block_height(&self) -> Result<u64, RpcError> {
        self.client
            .get_block_height()
            .await
            .map_err(|e| RpcError::Request(e))
    }

    async fn get_account(&self, pubkey: &Pubkey) -> Result<Account, RpcError> {
        match self.client.get_account(pubkey).await {
            Ok(account) => Ok(account),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("AccountNotFound") || err_str.contains("could not find account") {
                    Err(RpcError::AccountNotFound(*pubkey))
                } else {
                    Err(RpcError::Request(e))
                }
            }
        }
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>, RpcError> {
        self.client
            .get_multiple_accounts(pubkeys)
            .await
            .map_err(|e| RpcError::Request(e))
    }

    async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Pubkey, Account)>, RpcError> {
        self.client
            .get_program_accounts_with_config(program_id, config)
            .await
            .map_err(|e| RpcError::Request(e))
    }

    async fn send_transaction(&self, transaction: &Transaction) -> Result<Signature, RpcError> {
        self.client
            .send_transaction(transaction)
            .await
            .map_err(|e| {
                let err_str = e.to_string();
                if err_str.contains("blockhash not found") || err_str.contains("Blockhash not found") {
                    RpcError::BlockhashExpired
                } else {
                    RpcError::Request(e)
                }
            })
    }

    async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, RpcError> {
        self.client
            .send_and_confirm_transaction(transaction)
            .await
            .map_err(|e| {
                let err_str = e.to_string();
                if err_str.contains("blockhash not found") || err_str.contains("Blockhash not found") {
                    RpcError::BlockhashExpired
                } else if err_str.contains("Transaction simulation failed") {
                    RpcError::Transaction(err_str)
                } else {
                    RpcError::Request(e)
                }
            })
    }

    async fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> Result<Option<Result<(), solana_sdk::transaction::TransactionError>>, RpcError> {
        self.client
            .get_signature_status(signature)
            .await
            .map_err(|e| RpcError::Request(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running test validator.
    // They are marked #[ignore] by default and can be run with:
    // cargo test -p rpc-test -- --ignored

    #[tokio::test]
    #[ignore]
    async fn test_basic_rpc_operations() {
        use solana_test_validator::TestValidatorGenesis;

        let (validator, _payer) = TestValidatorGenesis::default().start_async().await;
        let rpc = TestRpc::new(&validator);

        // Test get_slot
        let slot = rpc.get_slot().await.unwrap();
        assert!(slot >= 0);

        // Test get_block_height
        let height = rpc.get_block_height().await.unwrap();
        assert!(height >= 0);

        // Test get_latest_blockhash
        let blockhash = rpc.get_latest_blockhash().await.unwrap();
        assert_ne!(blockhash, Hash::default());
    }
}
