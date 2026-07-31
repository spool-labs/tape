//! Choosing who splices a spool that failed its round.
//!
//! Every observer that settled the miss derives the same name from the round's
//! entropy block, with no election and no messages, the same trick that places
//! the round grid. One splicer per miss is what keeps repair from being a
//! thundering herd of nineteen identical reconstructions.

use tape_crypto::hash::{Hash, hashv};

use crate::types::SpoolIndex;

const SPLICER_DOMAIN: &[u8] = b"tape-splicer-v1";

/// Index of the elected splicer among the candidates, or None when there are
/// none. Candidates must arrive in the same order on every observer, which
/// spool order gives for free.
pub fn elect_splicer(block: &Hash, failed: SpoolIndex, candidates: usize) -> Option<usize> {
    if candidates == 0 {
        return None;
    }

    let failed_bytes = u64::from(failed.0).to_le_bytes();
    let seed = hashv(&[SPLICER_DOMAIN, block.as_ref(), &failed_bytes]);
    let mut head = [0u8; 8];
    head.copy_from_slice(&seed.as_ref()[..8]);
    Some((u64::from_le_bytes(head) % candidates as u64) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_choice_is_deterministic_and_in_range() {
        let block = Hash([7u8; 32]);
        for candidates in 1..40usize {
            let first = elect_splicer(&block, SpoolIndex(4), candidates).unwrap();
            let again = elect_splicer(&block, SpoolIndex(4), candidates).unwrap();
            assert_eq!(first, again);
            assert!(first < candidates);
        }
    }

    #[test]
    fn different_rounds_pick_different_splicers() {
        // Not a uniformity proof, only that the draw actually moves: over many
        // blocks every candidate position gets picked at least once.
        let mut seen = [false; 19];
        for byte in 0..=255u8 {
            let block = Hash([byte; 32]);
            let pick = elect_splicer(&block, SpoolIndex(4), seen.len()).unwrap();
            seen[pick] = true;
        }
        assert!(seen.iter().all(|hit| *hit), "some candidate was never picked");
    }

    #[test]
    fn no_candidates_elects_nobody() {
        assert_eq!(elect_splicer(&Hash([1u8; 32]), SpoolIndex(0), 0), None);
    }
}
