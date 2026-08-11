use store::Column;

use crate::types::ChallengeRoundKey;

/// Stores whether a peer certified each challenge round.
pub struct ChallengeRoundCol;

impl Column for ChallengeRoundCol {
    const CF_NAME: &'static str = "challenge_round";
    type Key = ChallengeRoundKey;
    type Value = bool;
}
