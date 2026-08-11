use store::Column;
use tape_core::challenge::PeerRecord;

use crate::types::PeerRecordKey;

/// Stores this node's local challenge history for each peer and spool.
pub struct ChallengeRecordCol;

impl Column for ChallengeRecordCol {
    const CF_NAME: &'static str = "challenge_record";
    type Key = PeerRecordKey;
    type Value = PeerRecord;
}
