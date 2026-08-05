/// A type alias for a (value, weight) pair.
pub type ValueAndWeight = (u64, u64);

/// Returns the BFT tolerance bound floor((n − 1)/3) for a non-zero participant count.
pub const fn max_faulty(total: u64) -> u64 {
    debug_assert!(total > 0);

    let n = total;
    (n - 1) / 3
}

/// Returns the minimum number of correct participants, computed as n minus floor((n − 1)/3).
pub const fn min_correct(total: u64) -> u64 {
    debug_assert!(total > 0);

    let n = total;
    let f = max_faulty(total);
    let result = n - f;

    debug_assert!(result > 0, "result must be non-zero");

    result
}

/// Returns true when weight is at least two thirds of total.
pub const fn is_supermajority(weight: u64, total: u64) -> bool {
    3 * weight >= 2 * total + 1
}

/// Returns true when weight guarantees at least one honest signer under the
/// fault bound implied by `is_supermajority` (f + 1, i.e. 8 of 20). Only
/// sound for attesting deterministic, independently recomputable values —
/// never for subjective votes.
pub const fn has_honest_signer(weight: u64, total: u64) -> bool {
    let quorum = (2 * total + 3) / 3;
    let tolerated = 2 * quorum - total - 1;
    weight > tolerated
}

/// Owners that must report ready before a group completes its Sync phase.
///
/// Section 11's max(k, f+1): k so the owners that did report can reconstruct
/// every coded track between them, f+1 so at least one of them is honest.
///
/// Deliberately low, and not `has_honest_signer`. That one derives f from the
/// intersecting-quorum bound, which is a looser f than the parameter set uses,
/// and it lands a position higher. A higher gate is the wrong direction here:
/// owners that left can never report, so it lets the owners a handoff is
/// replacing block the handoff itself.
pub const fn sync_ready_threshold(total: u64, reconstruct_k: u64) -> u64 {
    let honest = max_faulty(total) + 1;
    if reconstruct_k > honest {
        reconstruct_k
    } else {
        honest
    }
}

/// Finds the highest value where the cumulative weight of all votes for that value and higher
/// achieves a supermajority. If no such value exists, it returns 0.
///
/// Input is (value, weight). 
/// Returns highest value such that a quorum voted >= this.
pub fn quorum_above(pairs: &[ValueAndWeight], total: u64) -> u64 {
    debug_assert!(total > 0);

    let mut items: Vec<(u64, u64)> = pairs.to_vec();
    items.sort_by(|a, b| b.0.cmp(&a.0)); // descending by value

    let mut sum: u64 = 0;
    for (value, weight) in items {
        sum = sum.saturating_add(weight);
        if is_supermajority(sum, total) {
            return value;
        }
    }
    0
}

/// Returns the smallest value below which the remaining weight loses the supermajority, 
/// or 0 if it never fails.
///
/// Input is (value, weight).
/// Returns lowest value such that a quorum voted < this.
pub fn quorum_below(pairs: &[ValueAndWeight], total: u64) -> u64 {
    debug_assert!(total > 0);

    let mut items: Vec<(u64, u64)> = pairs.to_vec();
    items.sort_by(|a, b| b.0.cmp(&a.0)); // descending by value

    let mut sum: u64 = total;
    for (value, weight) in items {
        sum = sum.saturating_sub(weight);
        if !is_supermajority(sum, total) {
            return value;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bft_bounds() {
        for n in 1..=100u64 {
            let f = max_faulty(n);
            let m = min_correct(n);

            assert_eq!(f, (n - 1) / 3);
            assert_eq!(m, n - f);
            assert!(3 * f < n);
            assert!(m > 2 * f);
            assert_eq!(m + f, n);
            assert!(m > 0);
        }
    }

    #[test]
    fn supermajority_edges() {
        for n in 1..=50u64 {
            let thr = (2 * n + 1).div_ceil(3);
            if thr > 0 {
                assert!(!is_supermajority(thr - 1, n));
            }
            assert!(is_supermajority(thr, n));
            assert!(is_supermajority(n, n));
        }
    }

    #[test]
    fn honest_signer_edges() {
        assert!(!has_honest_signer(7, 20));
        assert!(has_honest_signer(8, 20));
        assert!(has_honest_signer(20, 20));

        for n in 1..=50u64 {
            let quorum = (2 * n + 1).div_ceil(3);
            let thr = 2 * quorum - n;
            if thr > 0 {
                assert!(!has_honest_signer(thr - 1, n));
            }
            assert!(has_honest_signer(thr, n));
            assert!(thr <= quorum);
            assert!(has_honest_signer(quorum, n));
        }
    }

    #[test]
    fn high_pass() {
        let ps = vec![(10, 2), (20, 3), (15, 1), (8, 4)];
        assert_eq!(quorum_above(&ps, 10), 8);

        let ps2 = vec![(30, 5), (20, 2), (10, 2)];
        assert_eq!(quorum_above(&ps2, 9), 20);

        let ps3 = vec![(100, 2), (50, 2), (25, 2)];
        assert_eq!(quorum_above(&ps3, 10), 0);

        let empty: Vec<(u64, u64)> = vec![];
        assert_eq!(quorum_above(&empty, 10), 0);
    }

    #[test]
    fn low_fail() {
        let ps = vec![(10, 2), (20, 3), (15, 1), (8, 4)];
        assert_eq!(quorum_below(&ps, 10), 15);

        let ps2 = vec![(100, 4), (50, 3), (25, 3)];
        assert_eq!(quorum_below(&ps2, 10), 100);

        let ps3 = vec![(100, 1), (50, 1)];
        assert_eq!(quorum_below(&ps3, 10), 0);

        let empty: Vec<(u64, u64)> = vec![];
        assert_eq!(quorum_below(&empty, 7), 0);
    }

    #[test]
    fn sat_sub() {
        let ps = vec![(999, 12)];
        assert_eq!(quorum_below(&ps, 10), 999);
    }

    #[test]
    fn sort_order() {
        let ps = vec![(5, 1), (8, 2), (7, 1), (9, 2), (7, 1)];
        assert_eq!(quorum_above(&ps, 7), 7);
        assert_eq!(quorum_below(&ps, 7), 8);
    }

    #[test]
    fn hp_lf_rel() {
        let ps = vec![(10, 2), (20, 3), (15, 1), (8, 4)];
        let hp = quorum_above(&ps, 10);
        let lf = quorum_below(&ps, 10);
        assert!(hp > 0);
        assert!(lf > 0);
        assert!(lf >= hp);
    }

    // the gate the paper puts a group's Sync completion behind, max(k, f+1),
    // which sits one position below the intersecting-quorum reading of f
    #[test]
    fn sync_gate() {
        let k = 7;
        assert_eq!(sync_ready_threshold(20, k), 7);
        assert!(has_honest_signer(8, 20));
        assert!(!has_honest_signer(7, 20), "the old gate needed one more");

        // k carries the gate when reconstruction needs more than one honest.
        assert_eq!(sync_ready_threshold(20, 9), 9);
        assert_eq!(sync_ready_threshold(4, 2), 2);
    }
}
