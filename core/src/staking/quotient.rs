use core::cmp::Ordering;

/// A struct representing a quotient as a fraction (numerator/denominator).
#[derive(Clone, Debug)]
pub struct Quotient {
    pub numer: u128,
    pub denom: u128,
}

impl Quotient {
    pub fn from_quot(numer: u128, denom: u128) -> Self {
        assert!(denom > 0, "Denominator must be > 0");
        Self { numer, denom }
    }
}

pub fn compare_quotients(a: &Quotient, b: &Quotient) -> Ordering {
    let left = a.numer.saturating_mul(b.denom);
    let right = b.numer.saturating_mul(a.denom);
    left.cmp(&right)
}

pub fn tie_break(t1: u64, i1: usize, t2: u64, i2: usize) -> Ordering {
    match t1.cmp(&t2) {
        Ordering::Greater => Ordering::Greater,
        Ordering::Less => Ordering::Less,
        Ordering::Equal => i2.cmp(&i1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quotient() {
        let q1 = Quotient::from_quot(1, 2); // 0.5
        let q2 = Quotient::from_quot(2, 3); // ~0.6667
        let q3 = Quotient::from_quot(3, 4); // 0.75
        let q4 = Quotient::from_quot(4, 5); // 0.8
        let q5 = Quotient::from_quot(1, 2); // 0.5 (same as q1)

        assert_eq!(compare_quotients(&q1, &q2), Ordering::Less);
        assert_eq!(compare_quotients(&q2, &q1), Ordering::Greater);
        assert_eq!(compare_quotients(&q1, &q5), Ordering::Equal);
        assert_eq!(compare_quotients(&q3, &q4), Ordering::Less);
        assert_eq!(compare_quotients(&q4, &q3), Ordering::Greater);
    }

    #[test]
    fn test_tie_break() {
        assert_eq!(tie_break(5, 1, 3, 2), Ordering::Greater);
        assert_eq!(tie_break(3, 1, 5, 2), Ordering::Less);
        assert_eq!(tie_break(4, 1, 4, 2), Ordering::Greater); // i2 > i1
        assert_eq!(tie_break(4, 2, 4, 1), Ordering::Less);    // i1 > i2
        assert_eq!(tie_break(4, 1, 4, 1), Ordering::Equal);   // same
    }
}
