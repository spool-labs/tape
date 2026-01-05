//! Track column family for minimal track information
//!
//! Nodes only need to know:
//! - The commitment hash to verify incoming slices
//! - Whether it's certified (for GC decisions)
//! - How many slices they've stored (for certification readiness)

use crate::types::Pubkey;
use store::Column;

use crate::ops::TrackInfo;

/// Tracks indexed by on-chain address (Pubkey)
/// Key: Pubkey (track_address, 32 bytes)
/// Value: TrackInfo
pub struct Tracks;

impl Column for Tracks {
    const CF_NAME: &'static str = "tracks";
    type Key = Pubkey;
    type Value = TrackInfo;
}
