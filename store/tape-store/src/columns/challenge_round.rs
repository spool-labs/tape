//! Per-round challenge outcomes, one entry per peer per round
//!
//! Key structure: peer address, epoch, round

use store::Column;

use crate::types::ChallengeRoundKey;

/// Which rounds a peer answered and which it missed
///
/// `PeerRecord` carries counters and a short strip, which answers "how often" but
/// not "which round". This answers the second: a row per peer per round, so a
/// report can name the rounds a node failed rather than only how many.
///
/// Pruned by epoch, since a round nobody can still dispute is only history.
///
/// Key: ChallengeRoundKey (48 bytes: peer + epoch BE + round BE)
/// Value: whether a certificate formed for that peer in that round
pub struct ChallengeRoundCol;

impl Column for ChallengeRoundCol {
    const CF_NAME: &'static str = "challenge_round";
    type Key = ChallengeRoundKey;
    type Value = bool;
}
