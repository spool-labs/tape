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
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction::TransactionError;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use tape_blocks::wire::Block;
use tape_crypto::address::Address;
use tape_crypto::tx::Txid;

use crate::error::RpcError;

/// Outcome of simulating a transaction without submitting it.
#[derive(Debug, Clone)]
pub struct SimulationResult {
    /// Execution error, if the simulated transaction failed.
    pub err: Option<TransactionError>,

    /// Compute units the transaction consumed.
    pub units_consumed: Option<u64>,
}

/// Which chain a backend answers for
///
/// A tapesvm daemon or an in-process litesvm belongs to the operator that
/// started it; an agave validator is the open Solana network everyone shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainKind {
    /// A chain the operator runs, serving the operator's own clients
    Own,
    /// The open Solana network
    Shared,
}

impl ChainKind {
    /// Whether the operator runs this chain
    pub fn is_own(self) -> bool {
        matches!(self, Self::Own)
    }

    /// A chain the operator runs executes each call of a batch before it
    /// answers the next, so a run of sends goes out as one batch.
    pub fn executes_batches_in_order(self) -> bool {
        self.is_own()
    }
}

/// One window of produced slots, answered by one member.
#[derive(Debug, Clone, Default)]
pub struct BlockWindow {
    /// Slots the member holds a block for, ascending
    pub produced: Vec<u64>,

    /// The answering member's own tip, filled in when it listed nothing
    pub tip: Option<u64>,
}

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

    /// Which chain this backend answers for
    ///
    /// The backend decides, which may cost it a round trip the first time it
    /// is asked. Every default that depends on who runs the chain reads this
    /// one answer, so two of them can never disagree.
    async fn chain_kind(&self) -> ChainKind {
        ChainKind::Shared
    }

    /// Whether a JSON-RPC batch runs strictly in array order on this backend
    ///
    /// A backend that executes each call of a batch before it answers the
    /// next takes transactions whose proofs were built in order as one batch.
    /// An agave validator answers a send before it executes, so a caller
    /// there keeps sending one transaction at a time.
    async fn executes_batches_in_order(&self) -> bool {
        self.chain_kind().await.executes_batches_in_order()
    }

    // ========================================================================
    // Slot/Block Operations
    // ========================================================================

    /// Get the current slot number
    async fn get_slot(&self) -> Result<u64, RpcError>;

    /// Get the most recently finalized slot.
    async fn get_finalized_slot(&self) -> Result<u64, RpcError>;

    /// Get the most recent slot a supermajority has voted on.
    async fn get_confirmed_slot(&self) -> Result<u64, RpcError>;

    /// Get the latest blockhash for transaction signing
    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError>;

    /// Get the cluster's genesis hash.
    ///
    /// Identifies which chain we are talking to. One program key can be
    /// deployed to two clusters, so anything cached against a program id needs
    /// this to tell them apart.
    async fn get_genesis_hash(&self) -> Result<Hash, RpcError>;

    /// Get a confirmed block by slot number
    async fn get_block(&self, slot: u64) -> Result<Block, RpcError>;

    /// Slots in start..=end with a block at the configured commitment, ascending
    async fn get_blocks(&self, start: u64, end: u64) -> Result<Vec<u64>, RpcError>;

    /// The same listing, plus the tip of the member that answered it
    ///
    /// A member behind the window lists nothing for slots it lacks, and its
    /// own tip is how far a caller may skip on that answer. The tip comes
    /// back only for an empty window, which is the only time it decides
    /// anything.
    async fn get_block_window(&self, start: u64, end: u64) -> Result<BlockWindow, RpcError> {
        let produced = self.get_blocks(start, end).await?;
        let tip = match produced.is_empty() {
            true => Some(self.get_slot().await?),
            false => None,
        };

        Ok(BlockWindow { produced, tip })
    }

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
    /// With `min_slot` set, a backend that has not reached that slot refuses
    /// the read. Returns `Err(RpcError::AccountNotFound)` if the account
    /// doesn't exist.
    async fn get_account_with_commitment(
        &self,
        pubkey: &Address,
        commitment: CommitmentLevel,
        min_slot: Option<u64>,
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
    /// With `min_slot` set, a backend that has not reached that slot refuses
    /// the read. Returns `None` for accounts that don't exist.
    async fn get_multiple_accounts_with_commitment(
        &self,
        pubkeys: &[Address],
        commitment: CommitmentLevel,
        min_slot: Option<u64>,
    ) -> Result<Vec<Option<Account>>, RpcError> {
        let _ = (commitment, min_slot);
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
    async fn send_transaction(&self, transaction: &VersionedTransaction)
        -> Result<Txid, RpcError>;

    /// Send transactions in the given order, one verdict each
    ///
    /// The outer error is a failure to reach the backend at all. An entry's
    /// error is that transaction's own rejection, and later entries still
    /// answer for themselves.
    async fn send_transactions(
        &self,
        transactions: &[VersionedTransaction],
    ) -> Result<Vec<Result<Txid, RpcError>>, RpcError> {
        let mut sent = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            sent.push(self.send_transaction(transaction).await);
        }

        Ok(sent)
    }

    /// Simulate a transaction without submitting it, returning the execution
    /// outcome and compute units consumed
    async fn simulate_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<SimulationResult, RpcError>;

    /// Send a transaction and wait until it reaches the commitment.
    async fn send_and_confirm_transaction(
        &self,
        transaction: &VersionedTransaction,
        commitment: CommitmentLevel,
        skip_preflight: bool,
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

    /// Wait until a sent transaction reaches this client's commitment, returning how it executed
    async fn wait_for_signature(
        &self,
        txid: &Txid,
    ) -> Result<Result<(), TransactionError>, RpcError>;
}
