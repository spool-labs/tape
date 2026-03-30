//! Block parsing library for Tapedrive.
//!
//! This crate provides shared block parsing functionality for extracting
//! Tapedrive instructions and events from Solana blocks.
//!
//! # Usage
//!
//! ## Event-only parsing (for UI/monitoring)
//! ```ignore
//! let parsed = tape_blocks::parse(&block)?;
//! for event in parsed.events {
//!     // Process events directly
//! }
//! ```
//!
//! ## Instruction parsing with event merging (for node operation)
//! ```ignore
//! let parsed = tape_blocks::parse(&block)?;
//! let merged = tape_blocks::merge(parsed.raw_instructions, parsed.events)?;
//! for instruction in merged {
//!     // Process instructions with embedded events
//! }
//! ```
//!
//! Or call the transaction-safe helper:
//! ```ignore
//! let merged = tape_blocks::parse_and_merge(&block)?;
//! ```

mod block;
mod error;
mod event;
mod helpers;
mod instruction;
mod merge;

// Re-export main types and functions
pub use block::{parse, parse_and_merge, ParsedBlock, ParsedTransaction};
pub use error::ParseError;
pub use event::{parse_event_data, TapedriveEvent};
pub use instruction::{ParsedInstruction, RawInstruction};
pub use merge::merge;
