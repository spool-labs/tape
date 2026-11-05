//! Spool column families, kept by stored spool (NOT epoch-namespaced)
//!
//! - SpoolMissingTrackCol: (position, handover, virtual group, track) -> entry
//! - SpoolStateCol: (position, virtual group) -> spool_state
//! - SpoolOwnerHistoryCol: (epoch, stored spool) -> owner
//!
//! Position comes before the virtual group, so the stored spools a live
//! spool carries sit in a few runs of keys.

use tape_core::system::PackedSpoolState;
use tape_crypto::address::Address;
use tape_store_api::Column;

use crate::types::missing::{PackedEntry, PackedEntryKey};

/// Bytes a spool state key takes: position, then virtual group big endian
pub const SPOOL_STATE_KEY_LEN: usize = 3;

/// Bytes an owner history key takes: epoch big endian, then stored spool
pub const OWNER_KEY_LEN: usize = 10;

/// One missing_list per stored spool, its handover entries kept together
///
/// Key: EntryKey (36 bytes)
/// Value: Entry (28 bytes)
pub struct SpoolMissingTrackCol;

impl Column for SpoolMissingTrackCol {
    const CF_NAME: &'static str = "spool_missing_track";
    type Key = PackedEntryKey;
    type Value = PackedEntry;
}

/// One spool_state per stored spool owned now or in the epochs slices are kept
///
/// Key: position (1 byte), virtual group BE (2 bytes)
/// Value: SpoolState (41 bytes)
pub struct SpoolStateCol;

impl Column for SpoolStateCol {
    const CF_NAME: &'static str = "spool_state";
    type Key = [u8; SPOOL_STATE_KEY_LEN];
    type Value = PackedSpoolState;
}

/// Who owned each live group's spools in each recorded epoch
///
/// Key: epoch BE (8 bytes), the live group's own stored spool BE (2 bytes)
/// Value: the owner's address, zero for a position nobody holds
pub struct SpoolOwnerHistoryCol;

impl Column for SpoolOwnerHistoryCol {
    const CF_NAME: &'static str = "spool_owner_history";
    type Key = [u8; OWNER_KEY_LEN];
    type Value = Address;
}
