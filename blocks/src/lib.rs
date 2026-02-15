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

mod block;
mod error;
mod event;
mod helpers;
mod instruction;
mod merge;

// Re-export main types and functions
pub use block::{parse, ParsedBlock, ParsedTransaction};
pub use error::ParseError;
pub use event::TapedriveEvent;
pub use instruction::{ParsedInstruction, RawInstruction};
pub use merge::merge;

// Re-export event types for convenience
pub use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, TapeDestroyed, TapeReserved,
    TrackCertified, TrackDeleted, TrackInvalidated, TrackRegistered,
};
