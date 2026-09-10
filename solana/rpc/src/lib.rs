//! # tape-rpc
//!
//! Core RPC trait and error types for Solana RPC operations.
//!
//! This crate defines the `Rpc` trait which abstracts over different RPC implementations:
//! - `tape-rpc-solana` - Production client with retry/failover
//! - Custom test backends for simulation/integration environments
//!
//! ## Pattern
//!
//! This follows the same pattern as `store/` in `tapedrive/archive/`:
//! ```text
//! tape-rpc (trait)  →  tape-rpc-solana | test backends
//!                           ↓
//!                     tape-client<R: Rpc>
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use tape_rpc::{Rpc, RpcError};
//!
//! async fn fetch_slot<R: Rpc>(rpc: &R) -> Result<u64, RpcError> {
//!     rpc.get_slot().await
//! }
//! ```

mod error;
mod rpc;

// Core exports
pub use error::{RpcError, looks_like_transaction_error};
pub use rpc::{Rpc, SimulationResult};

// Re-export async_trait for implementors
pub use async_trait::async_trait;

// Re-export commonly used Solana types for convenience
pub use solana_client::rpc_config::RpcProgramAccountsConfig;
pub use solana_commitment_config::CommitmentLevel;
pub use solana_instruction_error::InstructionError;
pub use solana_transaction_error::TransactionError;
pub use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::error::RpcError;
    pub use crate::rpc::Rpc;
    pub use crate::CommitmentLevel;
}

/// Slots a leader holds in a row, which bounds how long a real skip run is.
pub use solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS;
