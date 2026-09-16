//! Telling a skipped slot from a block a backend has not served, by getBlocks' listing

use solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS;

/// Longest absent run taken as a skip on getBlocks' word; a pruned ledger reads far longer
///
/// A leader holds its slots in a row, so two missing leaders stay within this.
pub const TRUSTED_SKIP_RUN: u64 = 2 * NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;

/// Whether an absent run this short is a skip rather than a backend missing its history
pub fn trusted_skip(slot: u64, first_listed: u64) -> bool {
    first_listed > slot && first_listed - slot <= TRUSTED_SKIP_RUN
}

/// The listed slots, plus any absent run too long to trust, which must be fetched slot by slot
pub fn slots_to_fetch(from: u64, produced: &[u64]) -> Vec<u64> {
    let mut slots = Vec::with_capacity(produced.len());
    let mut cursor = from;
    for &listed in produced {
        if listed < cursor {
            continue;
        }
        if listed != cursor && !trusted_skip(cursor, listed) {
            slots.extend(cursor..listed);
        }
        slots.push(listed);
        cursor = listed + 1;
    }
    slots
}

#[cfg(test)]
mod tests {
    use super::*;

    // short absent runs are skipped on the list's word, long ones are fetched
    #[test]
    fn skip_runs() {
        assert_eq!(slots_to_fetch(10, &[10, 11, 12]), vec![10, 11, 12]);
        assert_eq!(slots_to_fetch(10, &[13, 14]), vec![13, 14]);

        let long = 10 + TRUSTED_SKIP_RUN + 1;
        let mut expected: Vec<u64> = (10..long).collect();
        expected.push(long);
        assert_eq!(slots_to_fetch(10, &[long]), expected);

        // A hole in the middle is judged the same way as one at the front.
        let hole = 12 + TRUSTED_SKIP_RUN + 1;
        let mut expected = vec![10, 11];
        expected.extend(12..hole);
        expected.push(hole);
        assert_eq!(slots_to_fetch(10, &[10, 11, hole]), expected);
    }

    // nothing listed, or a list behind the cursor, fetches only from the cursor on
    #[test]
    fn empty_list() {
        assert!(slots_to_fetch(10, &[]).is_empty());
        // A list below the cursor is a backend answering a stale question.
        assert_eq!(slots_to_fetch(10, &[8, 9, 10]), vec![10]);
    }

    // the serial check and the list walk share one reach
    #[test]
    fn skip_reach() {
        assert!(!trusted_skip(10, 10));
        assert!(trusted_skip(10, 11));
        assert!(trusted_skip(10, 10 + TRUSTED_SKIP_RUN));
        assert!(!trusted_skip(10, 10 + TRUSTED_SKIP_RUN + 1));
    }
}
