//! Spool column families, kept by stored spool (NOT epoch-namespaced)
//!
//! - SpoolMissingTrackCol: (position, origin, virtual group, track) -> missing track
//! - SpoolStateCol: (position, virtual group) -> spool_state
//! - SpoolOwnerHistoryCol: (epoch, stored spool) -> owner
//!
//! Position comes before the virtual group, so the stored spools a live
//! spool carries sit in a few runs of keys.

use tape_crypto::address::Address;
use tape_recover::state::SpoolState;
use tape_recover::track::MissingTrack;
use tape_store_api::Column;

use crate::types::keys::{MissingTrackKey, OwnerKey, SpoolStateKey};

/// One missing_list per stored spool, its handover tracks kept together
///
/// Key: MissingTrackKey (36 bytes)
/// Value: MissingTrack's facts (36 bytes)
pub struct SpoolMissingTrackCol;

impl Column for SpoolMissingTrackCol {
    const CF_NAME: &'static str = "spool_missing_track";
    type Key = [u8; MissingTrackKey::SIZE];
    type Value = [u8; MissingTrack::SIZE];
}

/// One spool_state per stored spool owned now or in the epochs slices are kept
///
/// Key: SpoolStateKey (3 bytes)
/// Value: SpoolState (41 bytes)
pub struct SpoolStateCol;

impl Column for SpoolStateCol {
    const CF_NAME: &'static str = "spool_state";
    type Key = [u8; SpoolStateKey::SIZE];
    type Value = [u8; SpoolState::SIZE];
}

/// Who owned each live group's spools in each recorded epoch
///
/// Key: OwnerKey (10 bytes)
/// Value: the owner's address, zero for a position nobody holds
pub struct SpoolOwnerHistoryCol;

impl Column for SpoolOwnerHistoryCol {
    const CF_NAME: &'static str = "spool_owner_history";
    type Key = [u8; OwnerKey::SIZE];
    type Value = Address;
}
