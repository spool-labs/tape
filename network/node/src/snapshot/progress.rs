use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::types::EpochNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GroupState {
    Pending,
    Certified,
    Registered,
    CertifiedOnchain,
}

pub struct SnapshotProgress {
    epoch: EpochNumber,
    groups: [GroupState; SPOOL_GROUP_COUNT],
}

impl SnapshotProgress {
    pub fn new(epoch: EpochNumber) -> Self {
        Self {
            epoch,
            groups: [GroupState::Pending; SPOOL_GROUP_COUNT],
        }
    }

    pub fn reset(&mut self, epoch: EpochNumber) {
        self.epoch = epoch;
        self.groups = [GroupState::Pending; SPOOL_GROUP_COUNT];
    }

    pub fn epoch(&self) -> EpochNumber {
        self.epoch
    }

    pub fn advance(&mut self, group: usize, state: GroupState) {
        if group < SPOOL_GROUP_COUNT && state > self.groups[group] {
            self.groups[group] = state;
        }
    }

    pub fn get(&self, group: usize) -> GroupState {
        if group < SPOOL_GROUP_COUNT {
            self.groups[group]
        } else {
            GroupState::Pending
        }
    }

    pub fn has_local_cert(&self, group: usize) -> bool {
        matches!(
            self.get(group),
            GroupState::Certified | GroupState::CertifiedOnchain
        )
    }

    pub fn is_registered(&self, group: usize) -> bool {
        matches!(
            self.get(group),
            GroupState::Registered | GroupState::CertifiedOnchain
        )
    }

    pub fn is_done_onchain(&self, group: usize) -> bool {
        matches!(self.get(group), GroupState::CertifiedOnchain)
    }

    pub fn any_local_cert(&self, owned_groups: &[usize]) -> bool {
        owned_groups.iter().any(|&g| self.has_local_cert(g))
    }

    pub fn all_local_cert(&self, owned_groups: &[usize]) -> bool {
        !owned_groups.is_empty() && owned_groups.iter().all(|&g| self.has_local_cert(g))
    }

    pub fn all_done_onchain(&self, owned_groups: &[usize]) -> bool {
        !owned_groups.is_empty() && owned_groups.iter().all(|&g| self.is_done_onchain(g))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_pending() {
        let sp = SnapshotProgress::new(EpochNumber(5));
        assert_eq!(sp.epoch(), EpochNumber(5));
        assert_eq!(sp.get(0), GroupState::Pending);
        assert_eq!(sp.get(49), GroupState::Pending);
    }

    #[test]
    fn advance_monotonic() {
        let mut sp = SnapshotProgress::new(EpochNumber(1));
        sp.advance(0, GroupState::Certified);
        assert_eq!(sp.get(0), GroupState::Certified);

        // Can't go backwards
        sp.advance(0, GroupState::Pending);
        assert_eq!(sp.get(0), GroupState::Certified);

        sp.advance(0, GroupState::CertifiedOnchain);
        assert_eq!(sp.get(0), GroupState::CertifiedOnchain);
    }

    #[test]
    fn reset_clears() {
        let mut sp = SnapshotProgress::new(EpochNumber(1));
        sp.advance(0, GroupState::CertifiedOnchain);
        sp.reset(EpochNumber(2));
        assert_eq!(sp.epoch(), EpochNumber(2));
        assert_eq!(sp.get(0), GroupState::Pending);
    }

    #[test]
    fn semantic_group_checks() {
        let mut sp = SnapshotProgress::new(EpochNumber(1));
        sp.advance(0, GroupState::Certified);
        sp.advance(1, GroupState::Registered);
        sp.advance(2, GroupState::CertifiedOnchain);

        assert!(sp.has_local_cert(0));
        assert!(!sp.is_registered(0));
        assert!(!sp.is_done_onchain(0));

        assert!(!sp.has_local_cert(1));
        assert!(sp.is_registered(1));
        assert!(!sp.is_done_onchain(1));

        assert!(sp.has_local_cert(2));
        assert!(sp.is_registered(2));
        assert!(sp.is_done_onchain(2));
    }

    #[test]
    fn semantic_batch_checks() {
        let mut sp = SnapshotProgress::new(EpochNumber(1));
        sp.advance(0, GroupState::Certified);
        sp.advance(1, GroupState::Registered);
        sp.advance(2, GroupState::CertifiedOnchain);

        assert!(sp.any_local_cert(&[0, 1, 2]));
        assert!(!sp.all_local_cert(&[0, 1, 2]));
        assert!(!sp.all_done_onchain(&[0, 1, 2]));

        assert!(sp.all_local_cert(&[0, 2]));
        assert!(sp.all_done_onchain(&[2]));

        // Empty owned_groups returns false
        assert!(!sp.any_local_cert(&[]));
        assert!(!sp.all_local_cert(&[]));
        assert!(!sp.all_done_onchain(&[]));
    }

    #[test]
    fn out_of_bounds() {
        let mut sp = SnapshotProgress::new(EpochNumber(1));
        sp.advance(999, GroupState::Certified); // no-op
        assert_eq!(sp.get(999), GroupState::Pending);
    }
}
