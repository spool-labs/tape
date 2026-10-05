/// The fold: where each track group lives at a given spool group count.

use crate::erasure::MAX_GROUP_SIZE;
use crate::types::SpoolIndex;

use super::{GroupIndex, TrackGroup};

/// Where each track group lives while `live` spool groups run.
///
/// A track group below the live count is live as itself. One at or past it
/// lives in its nearest ancestor below the count. An id's parent is the same
/// at every count, so a lower count never moves data between groups that stay
/// live, and a higher count only splits groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupFold {
    live: u64,
    track_groups: u64,
}

impl GroupFold {
    /// The fold for `live` spool groups, with `track_groups` ids ever written to.
    /// None while no group is live.
    pub fn new(live: u64, track_groups: u64) -> Option<Self> {
        (live > 0).then_some(Self {
            live,
            track_groups: track_groups.max(live),
        })
    }

    /// How many spool groups are live under this fold.
    #[inline]
    pub fn live(&self) -> u64 {
        self.live
    }

    /// How many track groups a leaf may carry: the most spool groups ever live.
    #[inline]
    pub fn track_groups(&self) -> u64 {
        self.track_groups
    }

    /// Whether a track group exists under this fold. One at or past the track
    /// group count was never written to, so it has no data and no holder.
    #[inline]
    pub fn contains(&self, group: TrackGroup) -> bool {
        group.0 < self.track_groups
    }

    /// The live group that carries a track group.
    #[inline]
    pub fn live_group(&self, group: TrackGroup) -> GroupIndex {
        let mut id = group.0;
        while id >= self.live {
            id = parent(id);
        }

        GroupIndex(id)
    }

    /// The live spool that carries a stored spool.
    #[inline]
    pub fn live_spool(&self, spool: SpoolIndex) -> SpoolIndex {
        let position = spool.as_usize() % MAX_GROUP_SIZE;

        self.live_group(TrackGroup::containing(spool)).spool_at(position)
    }

    /// List the track groups a live group carries, its own first and the rest ascending.
    pub fn carried(&self, group: GroupIndex) -> Vec<TrackGroup> {
        if group.0 >= self.live {
            return Vec::new();
        }

        let folded = (self.live..self.track_groups)
            .map(TrackGroup)
            .filter(|candidate| self.live_group(*candidate) == group);

        core::iter::once(TrackGroup::from(group)).chain(folded).collect()
    }

    /// List the stored spools a live spool carries, its own first and the rest ascending.
    pub fn carried_spools(&self, spool: SpoolIndex) -> Vec<SpoolIndex> {
        let position = spool.as_usize() % MAX_GROUP_SIZE;

        self.carried(GroupIndex::containing(spool))
            .into_iter()
            .map(|group| group.spool_at(position))
            .collect()
    }
}

// Drops the id's lowest set bit and every bit under it. Zero has no parent.
#[inline]
fn parent(id: u64) -> u64 {
    id >> (id.trailing_zeros() + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUND: u64 = 64;

    fn fold(live: u64) -> GroupFold {
        GroupFold::new(live, BOUND).expect("live groups")
    }

    #[test]
    fn parents() {
        assert_eq!(parent(5), 2);
        assert_eq!(parent(6), 1);
        assert_eq!(parent(7), 3);
        assert_eq!(parent(8), 0);
        assert_eq!(parent(9), 4);
    }

    #[test]
    fn no_live_groups() {
        assert_eq!(GroupFold::new(0, 5), None);
    }

    // a group below the live count is live as itself
    #[test]
    fn live_as_itself() {
        for live in 1..=BOUND {
            for id in 0..live {
                assert_eq!(fold(live).live_group(TrackGroup(id)), GroupIndex(id));
            }
        }
    }

    #[test]
    fn ten_groups() {
        let at = |live: u64| -> Vec<u64> {
            (5..10).map(|id| fold(live).live_group(TrackGroup(id)).0).collect()
        };

        assert_eq!(at(10), vec![5, 6, 7, 8, 9]);
        assert_eq!(at(7), vec![5, 6, 3, 0, 4]);
        assert_eq!(at(5), vec![2, 1, 3, 0, 4]);
        assert_eq!(at(3), vec![2, 1, 1, 0, 0]);
    }

    // a lower count leaves every track group in a surviving group where it was
    #[test]
    fn shrink_keeps_survivors() {
        for from in 1..=BOUND {
            for to in 1..from {
                for id in 0..BOUND {
                    let before = fold(from).live_group(TrackGroup(id));
                    if before.0 < to {
                        assert_eq!(fold(to).live_group(TrackGroup(id)), before);
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
                for id in 0..BOUND {
                    let before = fold(from).live_group(TrackGroup(id));
                    let after = fold(to).live_group(TrackGroup(id));

                    assert!(after == before || after.0 >= from);
                    assert_eq!(fold(from).live_group(TrackGroup::from(after)), before);
                }
            }
        }
    }

    // halving puts exactly one retired group on each survivor
    #[test]
    fn halving_is_even() {
        for live in 1..=512u64 {
            let fold = GroupFold::new(live, 2 * live).expect("live groups");

            for group in 0..live {
                assert_eq!(fold.carried(GroupIndex(group)).len(), 2);
            }
        }
    }

    #[test]
    fn carried_inverts_live_group() {
        for live in 1..=BOUND {
            let fold = fold(live);
            let mut seen = 0;

            for group in 0..live {
                let carried = fold.carried(GroupIndex(group));

                assert_eq!(carried[0], TrackGroup(group));
                assert!(carried.windows(2).all(|pair| pair[0] < pair[1]));
                for track_group in &carried {
                    assert_eq!(fold.live_group(*track_group), GroupIndex(group));
                }
                seen += carried.len() as u64;
            }

            assert_eq!(seen, BOUND);
            assert!(fold.carried(GroupIndex(live)).is_empty());
        }
    }

    #[test]
    fn spools() {
        let fold = GroupFold::new(5, 10).expect("live groups");

        assert_eq!(fold.live_spool(TrackGroup(7).spool_at(3)), GroupIndex(3).spool_at(3));
        assert_eq!(fold.live_spool(TrackGroup(2).spool_at(19)), GroupIndex(2).spool_at(19));
        assert_eq!(
            fold.carried_spools(GroupIndex(2).spool_at(4)),
            vec![TrackGroup(2).spool_at(4), TrackGroup(5).spool_at(4)],
        );
    }

    // a count that was never passed carries nothing extra
    #[test]
    fn track_groups_floor() {
        let fold = GroupFold::new(5, 3).expect("live groups");

        assert_eq!(fold.track_groups(), 5);
        assert_eq!(fold.carried(GroupIndex(4)), vec![TrackGroup(4)]);
        assert!(fold.contains(TrackGroup(4)));
        assert!(!fold.contains(TrackGroup(5)));
    }
}
