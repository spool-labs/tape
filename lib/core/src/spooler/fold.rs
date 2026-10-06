/// The fold: where each virtual group lives at a given spool group count.

use core::ops::Range;

use crate::erasure::MAX_GROUP_SIZE;
use crate::types::SpoolIndex;

use super::{GroupIndex, VirtualGroup, VIRTUAL_GROUP_BITS, VIRTUAL_GROUP_COUNT};

/// Where each virtual group lives while `live` spool groups run.
///
/// A virtual group below the live count is live as itself. One at or past it
/// lives in its nearest ancestor below the count. An id's parent is the same
/// at every count, so a lower count never moves data between groups that stay
/// live, and a higher count only splits groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupFold {
    live: u64,
}

impl GroupFold {
    /// The fold for `live` spool groups. None while no group is live, and for
    /// more live groups than there are virtual groups.
    pub fn new(live: u64) -> Option<Self> {
        (1..=VIRTUAL_GROUP_COUNT)
            .contains(&live)
            .then_some(Self { live })
    }

    /// How many spool groups are live under this fold.
    #[inline]
    pub fn live(&self) -> u64 {
        self.live
    }

    /// The live group that carries a virtual group.
    #[inline]
    pub fn live_group(&self, group: VirtualGroup) -> GroupIndex {
        let mut id = group.0;
        while id >= self.live {
            id = parent(id);
        }

        GroupIndex(id)
    }

    /// A live group carries the virtual group with its own id. None for a
    /// group that isn't live under this fold.
    #[inline]
    pub fn virtual_group(&self, group: GroupIndex) -> Option<VirtualGroup> {
        (group.0 < self.live).then_some(VirtualGroup(group.0))
    }

    /// The live spool that carries a stored spool.
    #[inline]
    pub fn live_spool(&self, spool: SpoolIndex) -> SpoolIndex {
        let position = spool.as_usize() % MAX_GROUP_SIZE;

        self.live_group(VirtualGroup::containing(spool)).spool_at(position)
    }

    /// How many virtual groups a live group carries, its own included.
    pub fn carried_count(&self, group: GroupIndex) -> u64 {
        if group.0 >= self.live {
            return 0;
        }

        let child = self.folded_child(group);
        if child >= VIRTUAL_GROUP_COUNT {
            return 1;
        }

        1 << (VIRTUAL_GROUP_BITS + 1 - bit_length(child))
    }

    /// The virtual group at `index` among the ones a live group carries, its
    /// own first and the rest ascending. Works the answer out with no walk
    /// over the virtual groups, so the program can call it.
    pub fn carried_at(&self, group: GroupIndex, index: u64) -> Option<VirtualGroup> {
        if index >= self.carried_count(group) {
            return None;
        }
        if index == 0 {
            return self.virtual_group(group);
        }

        // The bits under the folded child count up one length at a time: one
        // id with no extra bits, two with one, four with two.
        let extra_bits = index.ilog2();
        let low_bits = index - (1 << extra_bits);

        Some(VirtualGroup((self.folded_child(group) << extra_bits) | low_bits))
    }

    /// List the virtual groups a live group carries, its own first and the rest ascending.
    pub fn carried(&self, group: GroupIndex) -> Vec<VirtualGroup> {
        (0..self.carried_count(group))
            .filter_map(|index| self.carried_at(group, index))
            .collect()
    }

    /// The virtual groups a live group carries as ascending runs of ids: its
    /// own, then one run for each bit length under its folded child. That is
    /// at most a dozen runs, so a store reads a live group's rows with a few
    /// range scans in place of one lookup per virtual group.
    pub fn carried_ranges(&self, group: GroupIndex) -> Vec<Range<VirtualGroup>> {
        if group.0 >= self.live {
            return Vec::new();
        }

        let mut ranges = vec![VirtualGroup(group.0)..VirtualGroup(group.0 + 1)];
        let mut start = self.folded_child(group);
        let mut width = 1;
        while start < VIRTUAL_GROUP_COUNT {
            ranges.push(VirtualGroup(start)..VirtualGroup(start + width));
            start *= 2;
            width *= 2;
        }

        ranges
    }

    /// List the stored spools a live spool carries, its own first and the rest ascending.
    pub fn carried_spools(&self, spool: SpoolIndex) -> Vec<SpoolIndex> {
        let position = spool.as_usize() % MAX_GROUP_SIZE;

        self.carried(GroupIndex::containing(spool))
            .into_iter()
            .map(|group| group.spool_at(position))
            .collect()
    }

    // The first child of a live group that is not live itself. Past its own
    // id, the group carries exactly the ids whose bits start with this child's.
    #[inline]
    fn folded_child(&self, group: GroupIndex) -> u64 {
        let mut child = 2 * group.0 + 1;
        while child < self.live {
            child *= 2;
        }

        child
    }
}

// Drops the id's lowest set bit and every bit under it. Zero has no parent.
#[inline]
fn parent(id: u64) -> u64 {
    id >> (id.trailing_zeros() + 1)
}

#[inline]
fn bit_length(id: u64) -> u32 {
    u64::BITS - id.leading_zeros()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spooler::MAX_LIVE_GROUPS;

    // Live counts the pairwise tests sweep.
    const BOUND: u64 = 64;

    fn fold(live: u64) -> GroupFold {
        GroupFold::new(live).expect("live groups")
    }

    #[test]
    fn parents() {
        assert_eq!(parent(5), 2);
        assert_eq!(parent(6), 1);
        assert_eq!(parent(7), 3);
        assert_eq!(parent(8), 0);
        assert_eq!(parent(9), 4);
    }

    // no fold without a live group, or with more live groups than virtual ones
    #[test]
    fn live_bounds() {
        assert_eq!(GroupFold::new(0), None);
        assert!(GroupFold::new(VIRTUAL_GROUP_COUNT).is_some());
        assert_eq!(GroupFold::new(VIRTUAL_GROUP_COUNT + 1), None);
    }

    // a group below the live count is live as itself
    #[test]
    fn live_as_itself() {
        for live in 1..=BOUND {
            for id in 0..live {
                assert_eq!(fold(live).live_group(VirtualGroup(id)), GroupIndex(id));
            }
        }
    }

    #[test]
    fn ten_groups() {
        let at = |live: u64| -> Vec<u64> {
            (5..10).map(|id| fold(live).live_group(VirtualGroup(id)).0).collect()
        };

        assert_eq!(at(10), vec![5, 6, 7, 8, 9]);
        assert_eq!(at(7), vec![5, 6, 3, 0, 4]);
        assert_eq!(at(5), vec![2, 1, 3, 0, 4]);
        assert_eq!(at(3), vec![2, 1, 1, 0, 0]);
    }

    // a lower count leaves every virtual group in a surviving group where it was
    #[test]
    fn shrink_keeps_survivors() {
        for from in 1..=BOUND {
            for to in 1..from {
                for id in 0..VIRTUAL_GROUP_COUNT {
                    let before = fold(from).live_group(VirtualGroup(id));
                    if before.0 < to {
                        assert_eq!(fold(to).live_group(VirtualGroup(id)), before);
                    }
                }
            }
        }
    }

    // a higher count only splits: what one group carried stays under that
    // group or moves out to a group that was not live
    #[test]
    fn grow_only_splits() {
        for from in 1..=BOUND {
            for to in from + 1..=BOUND {
                for id in 0..VIRTUAL_GROUP_COUNT {
                    let before = fold(from).live_group(VirtualGroup(id));
                    let after = fold(to).live_group(VirtualGroup(id));

                    assert!(after == before || after.0 >= from);
                    let own = fold(to).virtual_group(after).expect("live at the higher count");
                    assert_eq!(fold(from).live_group(own), before);
                }
            }
        }
    }

    // the closed forms agree with the walk at every live count: each live
    // group's list starts with itself, ascends, walks back to that group, and
    // the lists together hold every virtual group once
    #[test]
    fn carried_inverts_live_group() {
        for live in 1..=VIRTUAL_GROUP_COUNT {
            let fold = fold(live);
            let mut seen = 0;

            for group in 0..live {
                let carried = fold.carried(GroupIndex(group));

                assert_eq!(carried.len() as u64, fold.carried_count(GroupIndex(group)));
                assert_eq!(carried[0], VirtualGroup(group));
                assert!(carried.windows(2).all(|pair| pair[0] < pair[1]));
                for virtual_group in &carried {
                    assert!(virtual_group.0 < VIRTUAL_GROUP_COUNT);
                    assert_eq!(fold.live_group(*virtual_group), GroupIndex(group));
                }
                seen += carried.len() as u64;
            }

            assert_eq!(seen, VIRTUAL_GROUP_COUNT);
            assert!(fold.carried(GroupIndex(live)).is_empty());
        }
    }

    // the runs hold exactly the listed virtual groups, in the same order, in
    // no more than a dozen runs
    #[test]
    fn ranges_match_the_list() {
        for live in 1..=VIRTUAL_GROUP_COUNT {
            let fold = fold(live);

            for group in 0..live {
                let ranges = fold.carried_ranges(GroupIndex(group));
                let listed: Vec<VirtualGroup> = ranges
                    .iter()
                    .flat_map(|range| (range.start.0..range.end.0).map(VirtualGroup))
                    .collect();

                assert!(ranges.len() <= 1 + VIRTUAL_GROUP_BITS as usize);
                assert_eq!(listed, fold.carried(GroupIndex(group)));
            }

            assert!(fold.carried_ranges(GroupIndex(live)).is_empty());
        }
    }

    // nothing past the end of a group's list, and nothing for a group that is not live
    #[test]
    fn carried_at_bounds() {
        let fold = fold(5);
        let count = fold.carried_count(GroupIndex(1));

        assert_eq!(fold.carried_at(GroupIndex(1), 0), Some(VirtualGroup(1)));
        assert_eq!(fold.carried_at(GroupIndex(1), 1), Some(VirtualGroup(6)));
        assert_eq!(fold.carried_at(GroupIndex(1), 2), Some(VirtualGroup(12)));
        assert_eq!(fold.carried_at(GroupIndex(1), 3), Some(VirtualGroup(13)));
        assert!(fold.carried_at(GroupIndex(1), count - 1).is_some());
        assert_eq!(fold.carried_at(GroupIndex(1), count), None);
        assert_eq!(fold.carried_count(GroupIndex(5)), 0);
        assert_eq!(fold.carried_at(GroupIndex(5), 0), None);
    }

    // only a live group has a virtual group of its own
    #[test]
    fn virtual_group_bounds() {
        let fold = fold(5);

        assert_eq!(fold.virtual_group(GroupIndex(0)), Some(VirtualGroup(0)));
        assert_eq!(fold.virtual_group(GroupIndex(4)), Some(VirtualGroup(4)));
        assert_eq!(fold.virtual_group(GroupIndex(5)), None);
        assert_eq!(fold.virtual_group(GroupIndex(VIRTUAL_GROUP_COUNT)), None);
    }

    // at a power of two every live group carries the same number
    #[test]
    fn even_at_powers_of_two() {
        for bits in 0..=VIRTUAL_GROUP_BITS {
            let live = 1u64 << bits;

            for group in 0..live {
                assert_eq!(fold(live).carried_count(GroupIndex(group)), VIRTUAL_GROUP_COUNT / live);
            }
        }
    }

    // doubling the count halves every group: it keeps half of what it
    // carried and one new group takes the other half
    #[test]
    fn doubling_halves_every_group() {
        for live in 1..=MAX_LIVE_GROUPS {
            let before = fold(live);
            let after = fold(2 * live);

            for group in 0..live {
                let carried = before.carried(GroupIndex(group));
                let mut kept = 0;
                let mut takers = Vec::new();

                for virtual_group in &carried {
                    let now = after.live_group(*virtual_group);
                    if now == GroupIndex(group) {
                        kept += 1;
                    } else if !takers.contains(&now) {
                        takers.push(now);
                    }
                }

                assert_eq!(kept * 2, carried.len());
                assert_eq!(takers.len(), 1);
                assert!(takers[0].0 >= live);
            }
        }
    }

    // one more group halves exactly one group, and the new group takes that half
    #[test]
    fn one_step_splits_one_group() {
        for live in 1..VIRTUAL_GROUP_COUNT {
            let before = fold(live);
            let after = fold(live + 1);
            let mut split = Vec::new();

            for group in 0..live {
                let was = before.carried_count(GroupIndex(group));
                let now = after.carried_count(GroupIndex(group));
                if was != now {
                    assert_eq!(now * 2, was);
                    split.push(group);
                }
            }

            assert_eq!(split.len(), 1);
            assert_eq!(
                after.carried_count(GroupIndex(live)),
                after.carried_count(GroupIndex(split[0])),
            );
        }
    }

    #[test]
    fn spools() {
        let fold = fold(5);

        assert_eq!(fold.live_spool(VirtualGroup(7).spool_at(3)), GroupIndex(3).spool_at(3));
        assert_eq!(fold.live_spool(VirtualGroup(2).spool_at(19)), GroupIndex(2).spool_at(19));

        let carried = fold.carried_spools(GroupIndex(2).spool_at(4));

        assert_eq!(carried.len(), 512);
        assert_eq!(
            carried[..4],
            [
                VirtualGroup(2).spool_at(4),
                VirtualGroup(5).spool_at(4),
                VirtualGroup(10).spool_at(4),
                VirtualGroup(11).spool_at(4),
            ],
        );
    }
}
