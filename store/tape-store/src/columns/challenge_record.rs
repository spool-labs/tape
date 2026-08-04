//! Per-spool challenge history
//!
//! Key structure: peer address then spool

use store::Column;
use tape_core::challenge::PeerRecord;

use crate::types::PeerRecordKey;

/// What this node remembers about each peer's answers to storage challenges
///
/// Private to the node that wrote it: a record is one owner's own observations,
/// never something the network agrees on. Kept out of the slice volume because it
/// is read on the challenge path and rewritten every round.
///
/// One record per spool a peer owns, not one per peer: a certificate is issued
/// per spool, so a peer holding several owes an answer for each and each keeps
/// its own history. The node-level rule reads across a peer's spools.
///
/// Key: peer address then spool (34 bytes)
/// Value: opportunities, successes, and the current miss run
pub struct ChallengeRecordCol;

impl Column for ChallengeRecordCol {
    const CF_NAME: &'static str = "challenge_record";
    type Key = PeerRecordKey;
    type Value = PeerRecord;
}
