//! Core RPC trait definition
//!
//! This trait defines the interface for Solana RPC operations.
//! Implementations include:
//! - `TapeRpcClient` (tape-rpc) - Production client with retry/failover
//! - Test backends (for simulation/integration environments)

use async_trait::async_trait;
use solana_account::Account;
use solana_client::rpc_config::RpcProgramAccountsConfig;
use solana_commitment_config::CommitmentLevel;
use solana_hash::Hash;
use solana_transaction::{Transaction, TransactionError};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiConfirmedBlock};
use tape_crypto::address::Address;
use tape_crypto::tx::Txid;

use crate::error::RpcError;

/// Core RPC trait for Solana operations
///
/// This trait mirrors the Store pattern from `tapedrive/archive/store/`.
/// Implementations provide the actual RPC communication while consumers
/// remain generic over `R: Rpc`.
///
/// # Example
///
/// ```ignore
/// async fn fetch_slot<R: Rpc>(rpc: &R) -> Result<u64, RpcError> {
///     rpc.get_slot().await
/// }
/// ```
#[async_trait]
pub trait Rpc: Send + Sync {
    // ========================================================================
    // Configuration
    // ========================================================================

    /// Get the commitment level used by this RPC client
    fn commitment(&self) -> CommitmentLevel;

    // ========================================================================
    // Slot/Block Operations
    // ========================================================================

    /// Get the current slot number
    async fn get_slot(&self) -> Result<u64, RpcError>;

    /// Get the most recently finalized slot.
    async fn get_finalized_slot(&self) -> Result<u64, RpcError>;

    /// Get the latest blockhash for transaction signing
    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError>;

    /// Get a confirmed block by slot number
    async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError>;

    /// Get the lowest slot the node still has a confirmed block for. Slots
    /// below this have been pruned from the ledger and can never be fetched.
    async fn get_first_available_block(&self) -> Result<u64, RpcError>;

    /// Get a confirmed transaction by signature.
    async fn get_transaction(
        &self,
        txid: &Txid,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, RpcError>;

    /// Get the current block height
    async fn get_block_height(&self) -> Result<u64, RpcError>;

    // ========================================================================
    // Account Operations
    // ========================================================================

    /// Fetch a single account's data
    async fn get_account(&self, pubkey: &Address) -> Result<Account, RpcError>;

    /// Fetch a single account's data at an explicit commitment.
    ///
    /// Returns `Err(RpcError::AccountNotFound)` if the account doesn't exist.
    async fn get_account_with_commitment(
        &self,
        pubkey: &Address,
        commitment: CommitmentLevel,
    ) -> Result<Account, RpcError>;

    /// Fetch multiple accounts in a single request
    ///
    /// Returns `None` for accounts that don't exist.
    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Address],
    ) -> Result<Vec<Option<Account>>, RpcError>;

    /// Fetch multiple accounts in a single request at an explicit commitment.
    ///
    /// Returns `None` for accounts that don't exist.
    async fn get_multiple_accounts_with_commitment(
        &self,
        pubkeys: &[Address],
        commitment: CommitmentLevel,
    ) -> Result<Vec<Option<Account>>, RpcError> {
        let _ = commitment;
        self.get_multiple_accounts(pubkeys).await
    }

    /// Fetch program accounts with filters
    async fn get_program_accounts(
        &self,
        program_id: &Address,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Address, Account)>, RpcError>;

    // ========================================================================
    // Transaction Operations
    // ========================================================================

    /// Send a transaction without waiting for confirmation
    async fn send_transaction(&self, transaction: &Transaction) -> Result<Txid, RpcError>;

    /// Send a transaction and wait for confirmation
    async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Txid, RpcError>;

    /// Check the status of a transaction signature
    ///
    /// Returns:
    /// - `Ok(Some(Ok(())))` - Transaction confirmed successfully
    /// - `Ok(Some(Err(e)))` - Transaction confirmed with error
    /// - `Ok(None)` - Transaction not yet confirmed
    async fn get_signature_status(
        &self,
        txid: &Txid,
    ) -> Result<Option<Result<(), TransactionError>>, RpcError>;
}
