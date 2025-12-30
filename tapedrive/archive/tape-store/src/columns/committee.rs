//! Committee column family for epoch-based committee caching

use crate::ops::CommitteeCache;
use crate::types::EpochNumber;
use store::Column;

/// Committee cache indexed by epoch
/// Key: EpochNumber
/// Value: CommitteeCache
pub struct Committee;

impl Column for Committee {
    const CF_NAME: &'static str = "committee";
    type Key = EpochNumber;
    type Value = CommitteeCache;
}
