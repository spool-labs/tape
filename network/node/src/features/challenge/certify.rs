use tape_core::bft::min_correct;

/// Signatures a certificate needs, given how many positions the group holds.
///
/// The mechanism's `q` at a full group, scaled down so a partially filled group
/// still certifies rather than stalling every round.
pub fn agreement_threshold(members: usize) -> usize {
    min_correct(members.max(1) as u64) as usize
}

#[cfg(test)]
mod tests {
    use tape_core::bft::is_supermajority;
    use tape_core::erasure::GROUP_SIZE;

    use super::*;

    // At a full group the threshold is the mechanism's q, and it never drops to
    // a simple majority where two Byzantine signers could carry a round.
    #[test]
    fn supermajority() {
        assert_eq!(agreement_threshold(GROUP_SIZE), 14);
        assert!(agreement_threshold(GROUP_SIZE) > GROUP_SIZE / 2);

        // A partially filled group still certifies rather than stalling.
        assert_eq!(agreement_threshold(3), 3);
        assert_eq!(agreement_threshold(1), 1);
        assert_eq!(agreement_threshold(0), 1);
    }

    // The count a certificate is checked against is the one the mechanism
    // defines, at every group size a partially filled group can reach.
    #[test]
    fn threshold_is_the_bft_quorum() {
        for members in 0..=GROUP_SIZE {
            let threshold = agreement_threshold(members);
            assert!(is_supermajority(threshold as u64, members as u64));
            assert!(threshold > 0);
            if threshold > 1 {
                assert!(!is_supermajority(threshold as u64 - 1, members as u64));
            }
        }
    }
}
