//! Tapedrive storage node runtime.
//!
//! This crate implements the storage node's core runtime loop:
//!
//! - **`core`**: Shared utilities (backoff, config, context, metrics guards)
//! - **`core`**: Runtime state/actors (`NodeContext`, managed tasks, peer service, stats)
//! - **`ingestor`**: Sequential Solana block fetching and parsing
//! - **`fsm`**: Finite state machine that applies parsed instructions to local state
//! - **`scheduler`**: Diffs desired vs running tasks from FSM state changes
//! - **`supervisor`**: Centralized task runner with retry, cancellation, and concurrency limits
//! - **`http`**: Axum-based HTTP server for node-to-node and public APIs

pub mod core;
pub mod task;
pub mod chain;
pub mod fsm;
pub mod http;
pub mod ingestor;
pub mod pipeline;
pub mod scheduler;
pub mod snapshot;
pub mod supervisor;
pub mod tasks;

pub use task::{Task, TaskCategory, TaskOutcome, TaskResult};
