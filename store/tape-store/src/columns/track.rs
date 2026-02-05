//! Track column family for track metadata

use crate::types::{Pubkey, TrackInfo};
use store::Column;

/// Track info indexed by track address
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: TrackInfo (tape, spool allocation, encoding, commitments)
pub struct TrackCol;

impl Column for TrackCol {
    const CF_NAME: &'static str = "track";
    type Key = Pubkey;
    type Value = TrackInfo;
}
