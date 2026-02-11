//! Block processor for tapedrive on-chain events.

pub mod events;
mod handler;
mod parser;
#[cfg(test)]
mod test;
mod worker;

// Re-export the main entry point
pub use worker::{run, BlockProcessorError};

// Re-export parser types for use by other modules
pub use parser::{parse_block, ParsedBlock, ParsedInstruction, ParseError, TapedriveEvent};

// Re-export handlers for direct use (e.g., in tests or CLI tools)
pub use handler::{
    get_cluster_hash, get_sync_cursor, handle_advance_epoch, handle_certify_track,
    handle_delete_track, handle_destroy_tape, handle_invalidate_track,
    handle_register_track, handle_reserve_tape, set_cluster_hash, set_sync_cursor,
};

// Re-export events
pub use events::NodeEvent;
