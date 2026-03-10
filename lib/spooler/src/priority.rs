use core::cmp::Ordering;

/// A struct representing a priority score (numerator/denominator) for spool allocation.
#[derive(Clone, Debug)]
pub struct SpoolPriority {
    pub n: u64,
    pub d: u64,
}

impl SpoolPriority {
    pub fn new(n: u64, d: u64) -> Self {
        Self { n, d: d.max(1) }
    }
}

/// A priority queue entry for a node's spool allocation.
#[derive(Clone, Debug)]
pub struct NodePriority {
    pub priority: SpoolPriority,
    pub tie_breaker: u64,
    pub index: usize,
}

impl PartialEq for NodePriority {
    fn eq(&self, other: &Self) -> bool {
        (self.priority.n as u128) * (other.priority.d as u128)
            == (other.priority.n as u128) * (self.priority.d as u128)
            && self.tie_breaker == other.tie_breaker
            && self.index == other.index
    }
}

impl Eq for NodePriority {}

impl PartialOrd for NodePriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodePriority {
    fn cmp(&self, other: &Self) -> Ordering {
        match compare_spool_priorities(&self.priority, &other.priority) {
            Ordering::Equal => tie_break(self.tie_breaker, self.index, other.tie_breaker, other.index),
            ord => ord,
        }
    }
}

pub fn compare_spool_priorities(a: &SpoolPriority, b: &SpoolPriority) -> Ordering {
    let left = (a.n as u128) * (b.d as u128);
    let right = (b.n as u128) * (a.d as u128);
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
    fn test_spool_priority() {
        let q1 = SpoolPriority::new(1, 2);
        let q2 = SpoolPriority::new(2, 3);
        let q3 = SpoolPriority::new(3, 4);
        let q4 = SpoolPriority::new(4, 5);
        let q5 = SpoolPriority::new(1, 2);

        assert_eq!(compare_spool_priorities(&q1, &q2), Ordering::Less);
        assert_eq!(compare_spool_priorities(&q2, &q1), Ordering::Greater);
        assert_eq!(compare_spool_priorities(&q1, &q5), Ordering::Equal);
        assert_eq!(compare_spool_priorities(&q3, &q4), Ordering::Less);
        assert_eq!(compare_spool_priorities(&q4, &q3), Ordering::Greater);
    }

    #[test]
    fn test_tie_break() {
        assert_eq!(tie_break(5, 1, 3, 2), Ordering::Greater);
        assert_eq!(tie_break(3, 1, 5, 2), Ordering::Less);
        assert_eq!(tie_break(4, 1, 4, 2), Ordering::Greater);
        assert_eq!(tie_break(4, 2, 4, 1), Ordering::Less);
        assert_eq!(tie_break(4, 1, 4, 1), Ordering::Equal);
    }
}
