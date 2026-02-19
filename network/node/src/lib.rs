//! Tapedrive storage node runtime.
//!
//! This crate implements the storage node's core runtime loop:
//!
//! - **`core`**: Shared utilities (backoff, config, context, metrics guards)
//! - **`ingestor`**: Sequential Solana block fetching and parsing
//! - **`fsm`**: Finite state machine that applies parsed instructions to local state
//! - **`reconciler`**: Diffs desired vs running tasks from FSM state changes
//! - **`supervisor`**: Centralized task scheduler with retry, cancellation, and concurrency limits
//! - **`http`**: Axum-based HTTP server for node-to-node and public APIs

pub mod core;
pub mod fsm;
pub mod http;
pub mod ingestor;
pub mod peers;
pub mod pipeline;
pub mod reconciler;
pub mod state;
pub mod supervisor;
pub mod tasks;

#[cfg(test)]
pub(crate) mod test_util;
