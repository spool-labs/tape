use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::features::challenge::refusal::RefusalReason;

#[derive(Default)]
pub struct ChallengeCounters {
    pub opened: AtomicU64,
    pub settled_certified: AtomicU64,
    pub settled_missed: AtomicU64,
    pub refusals: RefusalCounters,
    pub voided: AtomicU64,
    pub discarded: AtomicU64,
    pub own_certified: AtomicU64,
    pub own_missed: AtomicU64,
}

/// Refused answers, split by why they were refused.
pub struct RefusalCounters {
    counts: [AtomicU64; RefusalReason::COUNT],
}

impl RefusalCounters {
    pub fn record(&self, reason: RefusalReason) {
        self.counts[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self, reason: RefusalReason) -> u64 {
        self.counts[reason.index()].load(Ordering::Relaxed)
    }

    pub fn total(&self) -> u64 {
        let mut total = 0;
        for count in &self.counts {
            total += count.load(Ordering::Relaxed);
        }
        total
    }

    /// Counts keyed by reason label, for the stats route
    pub fn by_reason(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        for reason in RefusalReason::ALL {
            counts.insert(reason.label().to_string(), self.get(reason));
        }
        counts
    }
}

impl Default for RefusalCounters {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a reason lands on its own row, and the total is the sum of the rows
    #[test]
    fn records_per_reason() {
        let counters = RefusalCounters::default();
        counters.record(RefusalReason::BadProof);
        counters.record(RefusalReason::BadProof);
        counters.record(RefusalReason::NoLocalQuestion);

        assert_eq!(counters.get(RefusalReason::BadProof), 2);
        assert_eq!(counters.get(RefusalReason::NoLocalQuestion), 1);
        assert_eq!(counters.get(RefusalReason::Late), 0);
        assert_eq!(counters.total(), 3);

        let by_reason = counters.by_reason();
        assert_eq!(by_reason.len(), RefusalReason::COUNT);
        assert_eq!(by_reason.get("bad_proof"), Some(&2));
    }
}
