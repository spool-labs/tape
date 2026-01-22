//! SliceInfo column family for blob erasure coding metadata
//!
//! Stores the hashes needed to verify slices for each track.

use crate::types::{Pubkey, SliceInfo};
use store::Column;

/// Slice info indexed by track address
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: SliceInfo (encoding type, hashes for verification)
pub struct SliceInfoCol;

impl Column for SliceInfoCol {
    const CF_NAME: &'static str = "slice_info";
    type Key = Pubkey;
    type Value = SliceInfo;
}
