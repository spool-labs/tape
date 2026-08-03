//! Certificate-threshold margin under one slow honest peer.
//!
//! Whirlwind is a signal, not consensus: a certificate is monotonic positive
//! evidence that the possession signal was witnessed, not an agreement. That
//! only needs one honest witness (f+1). Reusing the storage-safety threshold
//! q = n-f leaves zero margin, so one slow honest peer voids the round.

/// One certificate-threshold scenario.
pub struct QuorumRow {
    pub label: String,
    pub threshold: usize,
    pub slow_honest: usize,
    pub reachable_honest: usize,
    pub can_certify: bool,
    pub margin: i64,
}

/// Certificate formation under asynchrony, with the challenged owner self-signing.
pub struct QuorumReport {
    pub group_size: usize,
    pub byzantine: usize,
    pub honest: usize,
    pub rows: Vec<QuorumRow>,
}

impl QuorumReport {
    pub fn analyze(group_size: usize, byzantine: usize) -> Self {
        let honest = group_size.saturating_sub(byzantine);
        // The "one honest witness" threshold q = f+1 versus reusing the storage
        // safety threshold q = n-f, each with zero and one slow honest peer.
        let thresholds = [
            ("q = f+1 (one honest witness)", byzantine + 1),
            ("q = n-f (storage safety reuse)", group_size - byzantine),
        ];
        let mut rows = Vec::new();
        for (label, threshold) in thresholds {
            for slow_honest in [0usize, 1] {
                let reachable_honest = honest.saturating_sub(slow_honest);
                let margin = reachable_honest as i64 - threshold as i64;
                rows.push(QuorumRow {
                    label: label.to_string(),
                    threshold,
                    slow_honest,
                    reachable_honest,
                    can_certify: margin >= 0,
                    margin,
                });
            }
        }

        Self {
            group_size,
            byzantine,
            honest,
            rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // reusing the storage threshold voids a round on one slow honest peer
    #[test]
    fn slow_peer() {
        let report = QuorumReport::analyze(20, 6);
        let safety_slow = report
            .rows
            .iter()
            .find(|row| row.threshold == 14 && row.slow_honest == 1)
            .expect("the n-f row");
        assert!(!safety_slow.can_certify);
        let one_honest_slow = report
            .rows
            .iter()
            .find(|row| row.threshold == 7 && row.slow_honest == 1)
            .expect("the f+1 row");
        assert!(one_honest_slow.can_certify);
    }
}
