//! Tape column families

use crate::types::{StoredPubkey, TapeData, TapeKey, TapeNumber};
use store::Column;

/// Tapes indexed by ID
pub struct TapesById;

impl Column for TapesById {
    const CF_NAME: &'static str = "tapes/by_id";
    type Key = TapeKey;
    type Value = TapeData;
}

/// Tapes indexed by on-chain address
pub struct TapesByAddress;

impl Column for TapesByAddress {
    const CF_NAME: &'static str = "tapes/by_address";
    type Key = StoredPubkey;
    type Value = TapeNumber;
}

/// Active tapes index (for iteration)
/// Key: TapeNumber (BE encoded via TapeKey)
/// Value: unit (empty, presence indicates active)
pub struct TapesActiveIndex;

impl Column for TapesActiveIndex {
    const CF_NAME: &'static str = "tapes/active_index";
    type Key = TapeKey;
    type Value = (); // Unit type for presence-only index
}
