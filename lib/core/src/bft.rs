/// A type alias for a (value, weight) pair.
pub type ValueAndWeight = (u64, u64);

/// Returns true when weight is at least two thirds of total.
pub const fn is_supermajority(weight: u64, total: u64) -> bool {
    3 * weight >= 2 * total + 1
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
}
