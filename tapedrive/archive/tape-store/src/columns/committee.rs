//! Committee column families for epoch-based member tracking

use crate::types::{CommitteeData, EpochNumber};
use store::Column;

/// Committee data indexed by epoch
/// Key: EpochNumber
/// Value: CommitteeData
pub struct CommitteeByEpoch;

impl Column for CommitteeByEpoch {
    const CF_NAME: &'static str = "committee/by_epoch";
    type Key = EpochNumber;
    type Value = CommitteeData;
}
