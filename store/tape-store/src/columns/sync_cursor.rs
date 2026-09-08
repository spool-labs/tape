//! Sync cursor column family for tracking the last processed slot

use crate::types::UnitKey;
use store::Column;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Where durable state stands: the last slot processed and the block it chains from
#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
pub struct SyncCursor {
    pub slot: SlotNumber,
    pub parent: Option<Hash>,
}

/// Singleton column for the sync cursor
///
/// Key: UnitKey (0 bytes - singleton)
/// Value: SyncCursor
pub struct SyncCursorCol;

impl Column for SyncCursorCol {
    const CF_NAME: &'static str = "sync_cursor";
    type Key = UnitKey;
    type Value = SyncCursor;
}
