use tape_crypto::hash::{hashv, Hash};

/// Deterministic per-epoch leader ordering using Fisher-Yates shuffle
/// seeded by epoch nonce.
pub struct LeaderSchedule {
    order: Vec<usize>,
    committee_size: usize,
}

impl LeaderSchedule {
    /// Create a new leader schedule for the given committee size and nonce.
    ///
    /// Uses Fisher-Yates shuffle seeded by `blake3(nonce || "leader_schedule")`.
    /// Per-position entropy via `blake3(seed || i)` ensures full 256-bit mixing.
    pub fn new(committee_size: usize, nonce: Hash) -> Self {
        let mut order: Vec<usize> = (0..committee_size).collect();

        if committee_size > 1 {
            let seed = hashv(&[nonce.as_ref(), b"leader_schedule"]);
            for i in (1..committee_size).rev() {
                let h = hashv(&[seed.as_ref(), &i.to_le_bytes()]);
                let j = u64::from_le_bytes(h.0[..8].try_into().unwrap()) as usize % (i + 1);
                order.swap(i, j);
            }
        }

        Self {
            order,
            committee_size,
        }
    }

    /// Which member index is leader for the given slot.
    pub fn leader_for(&self, slot: usize) -> usize {
        if self.committee_size == 0 {
            return 0;
        }
        self.order[slot % self.committee_size]
    }

    /// What position does the given member hold for the given slot.
    /// Position 0 = primary leader, 1 = first backup, etc.
    pub fn position_for(&self, member_index: usize, slot: usize) -> usize {
        if self.committee_size == 0 {
            return 0;
        }
        let start = slot % self.committee_size;
        for offset in 0..self.committee_size {
            let idx = (start + offset) % self.committee_size;
            if self.order[idx] == member_index {
                return offset;
            }
        }
        self.committee_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonce(val: u8) -> Hash {
        Hash([val; 32])
    }

    #[test]
    fn deterministic() {
        let s1 = LeaderSchedule::new(10, nonce(42));
        let s2 = LeaderSchedule::new(10, nonce(42));
        assert_eq!(s1.order, s2.order);
    }

    #[test]
    fn different_nonce() {
        let s1 = LeaderSchedule::new(10, nonce(1));
        let s2 = LeaderSchedule::new(10, nonce(2));
        assert_ne!(s1.order, s2.order);
    }

    #[test]
    fn permutation() {
        let s = LeaderSchedule::new(20, nonce(99));
        let mut sorted = s.order.clone();
        sorted.sort();
        let expected: Vec<usize> = (0..20).collect();
        assert_eq!(sorted, expected);
    }

    #[test]
    fn leader_wraps() {
        let s = LeaderSchedule::new(5, nonce(1));
        assert_eq!(s.leader_for(0), s.leader_for(5));
        assert_eq!(s.leader_for(1), s.leader_for(6));
    }

    #[test]
    fn position_inverse() {
        let s = LeaderSchedule::new(10, nonce(7));
        let leader = s.leader_for(0);
        assert_eq!(s.position_for(leader, 0), 0);
    }

    #[test]
    fn all_positions_unique() {
        let s = LeaderSchedule::new(5, nonce(3));
        let positions: Vec<usize> = (0..5).map(|m| s.position_for(m, 0)).collect();
        let mut sorted = positions.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 5);
    }

    #[test]
    fn single_member() {
        let s = LeaderSchedule::new(1, nonce(1));
        assert_eq!(s.leader_for(0), 0);
        assert_eq!(s.position_for(0, 0), 0);
    }

    #[test]
    fn empty_committee() {
        let s = LeaderSchedule::new(0, nonce(1));
        assert_eq!(s.leader_for(0), 0);
        assert_eq!(s.position_for(0, 0), 0);
    }
}
