//! The single block-fetch shape used everywhere we ask Solana for a
//! confirmed block.
//!
//! Both the production RPC client (`rpc-solana`) and the caching proxy
//! (`rpc-cache`) call `getBlock` with this exact shape. Keeping it in
//! one place avoids drift between the cache key shape and what nodes
//! actually request — a mismatch would silently bypass the cache.
//!
//! Commitment is hardcoded to `Finalized` so cached blocks can never
//! be invalidated by a fork.

use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

pub const BLOCK_FETCH_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: Some(UiTransactionEncoding::Json),
    transaction_details: Some(TransactionDetails::Full),
    rewards: Some(false),
    commitment: Some(CommitmentConfig {
        commitment: CommitmentLevel::Finalized,
    }),
    max_supported_transaction_version: Some(0),
};
