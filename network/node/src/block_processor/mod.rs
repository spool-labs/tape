//! Block processor for tapedrive on-chain events.
//!
//! This module handles the processing of Solana blocks to keep local node state
//! synchronized with on-chain tapedrive instructions. It consists of:
//!
//! - **parser**: Parses Solana blocks into tapedrive-specific instructions
//! - **handlers**: Processes parsed instructions (storage updates, GC scheduling)
//! - **worker**: The main polling loop that drives block processing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌──────────┐     ┌──────────┐
//! │ Solana RPC  │────▶│  Parser  │────▶│ Handlers │────▶ Storage
//! └─────────────┘     └──────────┘     └──────────┘
//!        ▲                                   │
//!        │                                   ▼
//!        └─────────── Worker Loop ◀───── Events
//! ```
//!
//! # Usage
//!
//! The block processor is typically run as a background task:
//!
//! ```ignore
//! use tape_node::block_processor;
//!
//! let handle = tokio::spawn(block_processor::run(ctx, event_tx, cancel));
//! ```

mod handlers;
mod parser;
#[cfg(test)]
mod test_utils;
mod worker;

// Re-export the main entry point
pub use worker::{run, BlockProcessorError};

// Re-export parser types for use by other modules
pub use parser::{parse_block, ParsedBlock, ParsedInstruction, ParseError, TapedriveEvent};

// Re-export handlers for direct use (e.g., in tests or CLI tools)
pub use handlers::{
    get_cluster_hash, get_cursor, handle_certify_track, handle_delete_track,
    handle_destroy_tape, handle_invalidate_track, handle_register_track,
    run_epoch_gc, set_cluster_hash, set_cursor,
};
