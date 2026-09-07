//! Sync cursor column family for tracking last processed slot

use crate::types::UnitKey;
use tape_core::types::SlotNumber;
use store::Column;

/// Singleton column for sync cursor
///
/// Key: UnitKey (0 bytes - singleton)
/// Value: SlotNumber (last processed slot)
pub struct SyncCursorCol;

impl Column for SyncCursorCol {
    const CF_NAME: &'static str = "sync_cursor";
    type Key = UnitKey;
    type Value = SlotNumber;
}
