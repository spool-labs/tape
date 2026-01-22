//! Committee column family for epoch-based committee caching

use crate::types::{CommitteeCache, EpochKey};
use store::Column;

/// Committee cache indexed by epoch
///
/// Key: EpochKey (8 bytes: epoch BE)
/// Value: CommitteeCache (members, spool assignments, local node info)
pub struct Committee;

impl Column for Committee {
    const CF_NAME: &'static str = "committee";
    type Key = EpochKey;
    type Value = CommitteeCache;
}
