///! Definitions and computations related to Byzantine Fault Tolerance (BFT).

use core::num::NonZeroU16;

/// Computes the maximum number of Byzantine (faulty) nodes a system of size n can tolerate.
/// This is the classic BFT bound f = (n − 1)/3, ensuring 3f < n.
#[inline]
pub fn max_byzantine_nodes(n: NonZeroU16) -> u16 {
    (n.get() - 1) / 3
}

/// Computes the minimum number of correct (non-faulty) nodes required, n − f.
/// When n = 3f + 1, this is exactly 2f + 1; otherwise, it may be slightly higher.
#[inline]
pub fn min_correct_nodes(n: NonZeroU16) -> NonZeroU16 {
    let f = max_byzantine_nodes(n);
    let result = n.get() - f;
    NonZeroU16::new(result)
        .expect("Invariant broken: minimum correct nodes must be non-zero due to 3f < n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bft_calculations() {
        let test_cases = [
            (1, 0, 1),
            (3, 0, 3),
            (4, 1, 3),
            (5, 1, 4),
            (6, 1, 5),
            (100, 33, 67),
            (300, 99, 201),
        ];

        for (n, expected_f, expected_min_correct) in test_cases.iter() {
            let n = NonZeroU16::new(*n).unwrap();
            let f = max_byzantine_nodes(n);
            let min_correct = min_correct_nodes(n).get();

            assert_eq!(f, *expected_f, "Failed max_byzantine_nodes for n={}", n);
            assert_eq!(
                min_correct,
                *expected_min_correct,
                "Failed min_correct_nodes for n={}",
                n
            );
            assert!(
                3 * f < n.get(),
                "Invariant 3f < n violated for n={}",
                n
            );
        }
    }
}
