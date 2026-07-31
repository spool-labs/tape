//! Per-peer challenge history
//!
//! Key structure: peer address

use store::Column;
use tape_core::challenge::PeerRecord;
use tape_crypto::address::Address;

/// What this node remembers about each peer's answers to storage challenges
///
/// Private to the node that wrote it: a record is one owner's own observations,
/// never something the network agrees on. Kept out of the slice volume because it
/// is read on the challenge path and rewritten every round.
///
/// Key: peer address (32 bytes)
/// Value: opportunities, successes, and the current miss run
pub struct ChallengeRecordCol;

impl Column for ChallengeRecordCol {
    const CF_NAME: &'static str = "challenge_record";
    type Key = Address;
    type Value = PeerRecord;
}
