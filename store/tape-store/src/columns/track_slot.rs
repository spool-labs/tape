//! Slot each track's registration finalized at
//!
//! Key structure: track address

use store::Column;
use tape_core::types::SlotNumber;
use tape_crypto::address::Address;

/// The finalized slot a track was registered in
///
/// Chain-derived, so every node that ingested the registration holds the same
/// value. The challenge sample set is cut at a round window's base slot with
/// it, so a write landing mid-round cannot split the set between observers.
///
/// Key: track address (32 bytes)
/// Value: slot number
pub struct TrackSlotCol;

impl Column for TrackSlotCol {
    const CF_NAME: &'static str = "track_slot";
    type Key = Address;
    type Value = SlotNumber;
}
