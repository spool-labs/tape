use core::num::NonZeroU64;

/// Returns the BFT tolerance bound floor((n − 1)/3) for a non-zero participant count.
pub fn max_faulty(total: NonZeroU64) -> u64 {
    let n = total.get();
    (n - 1) / 3
}

/// Returns the minimum number of correct participants, computed as n minus floor((n − 1)/3).
pub fn min_correct(total: NonZeroU64) -> u64 {
    let n = total.get();
    let f = max_faulty(total);
    let result = n - f;
    debug_assert!(result > 0, "result must be non-zero");
    result
}

/// Returns true when weight is at least two thirds of total.
pub fn is_supermajority(weight: u64, total: u64) -> bool {
    3 * weight >= 2 * total + 1
}

/// Returns the greatest value whose top-down cumulative weight first reaches the supermajority, or 0 if none.
pub fn highest_passing_value(pairs: &[(u64, u16)], total: NonZeroU64) -> u64 {
    let mut items: Vec<(u64, u16)> = pairs.to_vec();
    items.sort_by(|a, b| b.0.cmp(&a.0)); // descending by value

    let mut sum: u64 = 0;
    for (value, weight) in items {
        sum = sum.saturating_add(weight as u64);
        if is_supermajority(sum, total.get()) {
            return value;
        }
    }
    0
}

/// Returns the smallest value below which the remaining weight loses the supermajority, or 0 if it never fails.
pub fn lowest_failing_value(pairs: &[(u64, u16)], total: NonZeroU64) -> u64 {
    let mut items: Vec<(u64, u16)> = pairs.to_vec();
    items.sort_by(|a, b| b.0.cmp(&a.0)); // descending by value

    let mut sum: u64 = total.get();
    for (value, weight) in items {
        sum = sum.saturating_sub(weight as u64);
        if !is_supermajority(sum, total.get()) {
            return value;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap()
    }

    #[test]
    fn bft_bounds() {
        for n in 1..=200u64 {
            let t = nz(n);
            let f = max_faulty(t);
            let m = min_correct(t);

            assert_eq!(f, (n - 1) / 3);
            assert_eq!(m, n - f);
            assert!(3 * f <= n - 1);
            assert!(m >= 2 * f + 1);
            assert_eq!(m + f, n);
            assert!(m > 0);
        }
    }

    #[test]
    fn smj_edges() {
        for n in 1..=50u64 {
            let thr = (2 * n + 1 + 2) / 3;
            if thr > 0 {
                assert!(!is_supermajority(thr - 1, n));
            }
            assert!(is_supermajority(thr, n));
            assert!(is_supermajority(n, n));
        }
    }

    #[test]
    fn high_pass() {
        let t = nz(10);
        let ps = vec![(10, 2), (20, 3), (15, 1), (8, 4)];
        assert_eq!(highest_passing_value(&ps, t), 8);

        let t2 = nz(9);
        let ps2 = vec![(30, 5), (20, 2), (10, 2)];
        assert_eq!(highest_passing_value(&ps2, t2), 20);

        let t3 = nz(10);
        let ps3 = vec![(100, 2), (50, 2), (25, 2)];
        assert_eq!(highest_passing_value(&ps3, t3), 0);

        let t4 = nz(10);
        let empty: Vec<(u64, u16)> = vec![];
        assert_eq!(highest_passing_value(&empty, t4), 0);
    }

    #[test]
    fn low_fail() {
        let t = nz(10);
        let ps = vec![(10, 2), (20, 3), (15, 1), (8, 4)];
        assert_eq!(lowest_failing_value(&ps, t), 15);

        let t2 = nz(10);
        let ps2 = vec![(100, 4), (50, 3), (25, 3)];
        assert_eq!(lowest_failing_value(&ps2, t2), 100);

        let t3 = nz(10);
        let ps3 = vec![(100, 1), (50, 1)];
        assert_eq!(lowest_failing_value(&ps3, t3), 0);

        let t4 = nz(7);
        let empty: Vec<(u64, u16)> = vec![];
        assert_eq!(lowest_failing_value(&empty, t4), 0);
    }

    #[test]
    fn sat_sub() {
        let t = nz(10);
        let ps = vec![(999, 12)];
        assert_eq!(lowest_failing_value(&ps, t), 999);
    }

    #[test]
    fn sort_order() {
        let t = nz(7);
        let ps = vec![(5, 1), (8, 2), (7, 1), (9, 2), (7, 1)];
        assert_eq!(highest_passing_value(&ps, t), 7);
        assert_eq!(lowest_failing_value(&ps, t), 8);
    }

    #[test]
    fn hp_lf_rel() {
        let t = nz(10);
        let ps = vec![(10, 2), (20, 3), (15, 1), (8, 4)];
        let hp = highest_passing_value(&ps, t);
        let lf = lowest_failing_value(&ps, t);
        assert!(hp > 0);
        assert!(lf > 0);
        assert!(lf >= hp);
    }
}
