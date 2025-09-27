use core::cmp::Ordering;

/// A struct representing a priority score (numerator/denominator) for shard allocation.
#[derive(Clone, Debug)]
pub struct ShardPriority {
    pub n: u128,
    pub d: u128,
}

impl ShardPriority {
    pub fn from(n: u128, d: u128) -> Self {
        assert!(d > 0, "Denominator must be > 0");
        Self { n, d }
    }
}

pub fn compare_shard_priorities(a: &ShardPriority, b: &ShardPriority) -> Ordering {
    let left = a.n.saturating_mul(b.d);
    let right = b.n.saturating_mul(a.d);
    left.cmp(&right)
}

pub fn tie_break(t1: u64, i1: usize, t2: u64, i2: usize) -> Ordering {
    match t1.cmp(&t2) {
        Ordering::Greater => Ordering::Greater,
        Ordering::Less => Ordering::Less,
        Ordering::Equal => i2.cmp(&i1),
    }
}

/// A priority queue entry for a node's shard allocation
#[derive(Clone, Debug)]
pub struct NodePriority {
    pub priority: ShardPriority,
    pub tie_breaker: u64,
    pub index: usize,
}

impl PartialEq for NodePriority {
    fn eq(&self, other: &Self) -> bool {
        self.priority.n * other.priority.d == other.priority.n * self.priority.d
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
        match compare_shard_priorities(&self.priority, &other.priority) {
            Ordering::Equal => tie_break(self.tie_breaker, self.index, other.tie_breaker, other.index),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_priority() {
        let q1 = ShardPriority::from(1, 2); // 0.5
        let q2 = ShardPriority::from(2, 3); // ~0.6667
        let q3 = ShardPriority::from(3, 4); // 0.75
        let q4 = ShardPriority::from(4, 5); // 0.8
        let q5 = ShardPriority::from(1, 2); // 0.5 (same as q1)

        assert_eq!(compare_shard_priorities(&q1, &q2), Ordering::Less);
        assert_eq!(compare_shard_priorities(&q2, &q1), Ordering::Greater);
        assert_eq!(compare_shard_priorities(&q1, &q5), Ordering::Equal);
        assert_eq!(compare_shard_priorities(&q3, &q4), Ordering::Less);
        assert_eq!(compare_shard_priorities(&q4, &q3), Ordering::Greater);
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
