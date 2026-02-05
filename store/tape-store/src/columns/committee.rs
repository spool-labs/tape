//! Committee column family for epoch-based committee storage

use crate::types::{EpochKey, NodeInfo};
use store::Column;

/// Committee indexed by epoch
///
/// Key: EpochKey (8 bytes: epoch BE)
/// Value: Vec<NodeInfo> (committee members)
pub struct CommitteeCol;

impl Column for CommitteeCol {
    const CF_NAME: &'static str = "committee";
    type Key = EpochKey;
    type Value = Vec<NodeInfo>;
}
