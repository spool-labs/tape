//! TrackInfo column family for track metadata
//!
//! Stores information about individual blobs (tracks).

use crate::types::{Pubkey, TrackInfo};
use store::Column;

/// Track info indexed by track address
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: TrackInfo (tape association, certification status, signature)
pub struct TrackInfoCol;

impl Column for TrackInfoCol {
    const CF_NAME: &'static str = "track_info";
    type Key = Pubkey;
    type Value = TrackInfo;
}
