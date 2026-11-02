//! The tracks a new spool owner is missing, worked from both ends
//!
//! Sync asks the previous owner for tracks from the front of an owner's list,
//! and keeps a small window from the back in the repair queue. Each end moves
//! at its own speed, so the split between sync and repair follows from the
//! two. Nothing here reads the store or calls a peer.

use std::collections::VecDeque;

use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;

use crate::features::spool::scan::Gap;

/// Tracks sync keeps in the repair queue from the back of an owner's list
pub const WINDOW_SIZE: usize = 32;

/// The window refills once fewer than this many of its tracks lack their slice
pub const WINDOW_REFILL: usize = 16;

/// Where a track stands with sync
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    /// Not handed out yet, or handed back after the owner left it out
    Waiting,
    /// In a request to the owner
    Asked,
    /// On disk
    Stored,
    /// The owner couldn't serve it, so it's repair's or the certify hook's
    Passed,
}

/// Whether the track has a queue row, and who wrote it
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Row {
    Absent,
    /// The row was there before sync started
    Prior,
    /// Sync wrote it, so sync deletes it when it stores the track
    Ours,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    gap: Gap,
    mark: Mark,
    row: Row,
}

/// One track handed out to ask the owner for, with its place in the list
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ask {
    pub index: usize,
    pub gap: Gap,
    /// The track has a queue row, so repair may write it first
    pub is_queued: bool,
}

/// A live spool's missing tracks, split by the previous owner of each stored spool
#[derive(Debug, Default)]
pub struct MissingList {
    /// One list per previous owner, in the order the owners first appear
    pub owners: Vec<OwnerList>,
    /// Certified gaps with no other node to sync from and no queue row yet
    pub unsourced: Vec<Gap>,
}

impl MissingList {
    /// Split the gaps by the previous owner of their stored spool, and sort each
    /// owner's share by stored spool, then track
    ///
    /// `owner_of` gives a stored spool's previous owner, or nothing when there's
    /// no other node to sync from. `has_row` says whether a gap already sits in
    /// the repair or recovery queue.
    pub fn build(
        gaps: Vec<Gap>,
        mut owner_of: impl FnMut(SpoolIndex) -> Option<Address>,
        mut has_row: impl FnMut(&Gap) -> bool,
    ) -> Self {
        let mut lists: Vec<(Address, Vec<Entry>)> = Vec::new();
        let mut unsourced = Vec::new();

        for gap in gaps {
            let row = if has_row(&gap) { Row::Prior } else { Row::Absent };

            let Some(owner) = owner_of(gap.stored) else {
                if gap.is_certified && row == Row::Absent {
                    unsourced.push(gap);
                }
                continue;
            };

            let entry = Entry { gap, mark: Mark::Waiting, row };
            match lists.iter_mut().find(|(known, _)| *known == owner) {
                Some((_, entries)) => entries.push(entry),
                None => lists.push((owner, vec![entry])),
            }
        }

        let owners = lists
            .into_iter()
            .map(|(owner, mut entries)| {
                entries.sort_unstable_by_key(|entry| (entry.gap.stored, entry.gap.track));
                OwnerList::new(owner, entries)
            })
            .collect();

        Self { owners, unsourced }
    }
}

/// One previous owner's share of a live spool's missing tracks
#[derive(Debug)]
pub struct OwnerList {
    owner: Address,
    entries: Vec<Entry>,
    /// The next entry the front has never handed out
    front: usize,
    /// The window has looked at every entry from here on
    back: usize,
    /// Tracks the owner left out, asked for again before the front moves on
    again: VecDeque<usize>,
    /// Window tracks not yet known to be on disk
    window: Vec<usize>,
}

impl OwnerList {
    fn new(owner: Address, entries: Vec<Entry>) -> Self {
        let back = entries.len();
        Self {
            owner,
            entries,
            front: 0,
            back,
            again: VecDeque::new(),
            window: Vec::new(),
        }
    }

    pub fn owner(&self) -> Address {
        self.owner
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Hand out up to `max` tracks to ask the owner for, left-out ones first
    pub fn next_batch(&mut self, max: usize) -> Vec<Ask> {
        let mut batch = Vec::new();

        while batch.len() < max {
            let Some(index) = self.again.pop_front() else {
                break;
            };
            self.hand_out(index, &mut batch);
        }

        while batch.len() < max && self.front < self.entries.len() {
            let index = self.front;
            self.front += 1;
            self.hand_out(index, &mut batch);
        }

        batch
    }

    fn hand_out(&mut self, index: usize, batch: &mut Vec<Ask>) {
        let entry = &mut self.entries[index];
        if entry.mark != Mark::Waiting {
            return;
        }

        entry.mark = Mark::Asked;
        batch.push(Ask {
            index,
            gap: entry.gap,
            is_queued: entry.row != Row::Absent,
        });
    }

    /// The track is on disk. True when sync wrote its queue row, so the row goes
    pub fn stored(&mut self, index: usize) -> bool {
        let entry = &mut self.entries[index];
        entry.mark = Mark::Stored;
        self.window.retain(|queued| *queued != index);

        entry.row == Row::Ours
    }

    /// The owner left the track out to stay within the byte budget
    pub fn left_out(&mut self, index: usize) {
        let entry = &mut self.entries[index];
        if entry.mark == Mark::Asked {
            entry.mark = Mark::Waiting;
            self.again.push_back(index);
        }
    }

    /// The owner couldn't serve the track: it reported it missing, or the
    /// request failed. Gives the track back when sync should queue it
    pub fn passed(&mut self, index: usize) -> Option<Gap> {
        let entry = &mut self.entries[index];
        if entry.mark != Mark::Asked {
            return None;
        }

        entry.mark = Mark::Passed;
        if !entry.gap.is_certified || entry.row != Row::Absent {
            return None;
        }

        entry.row = Row::Ours;
        Some(entry.gap)
    }

    /// Drop window tracks now on disk, and refill the window from the back once
    /// fewer than `WINDOW_REFILL` still lack their slice
    ///
    /// Gives back the tracks to add to the repair queue. Only a slice on disk
    /// counts as progress, whichever path wrote it.
    pub fn refill(&mut self, mut is_written: impl FnMut(&Gap) -> bool) -> Vec<Gap> {
        let entries = &mut self.entries;
        self.window.retain(|index| {
            let entry = &mut entries[*index];
            if entry.mark == Mark::Stored || is_written(&entry.gap) {
                entry.mark = Mark::Stored;
                return false;
            }
            true
        });

        if self.window.len() >= WINDOW_REFILL {
            return Vec::new();
        }

        let mut queued = Vec::new();
        while self.window.len() < WINDOW_SIZE && self.back > self.front {
            self.back -= 1;

            let entry = &mut self.entries[self.back];
            if entry.mark != Mark::Waiting || entry.row != Row::Absent || !entry.gap.is_certified {
                continue;
            }

            entry.row = Row::Ours;
            self.window.push(self.back);
            queued.push(entry.gap);
        }

        queued
    }

    /// Sync has handed out every track it hasn't stored, so the ends have met
    pub fn has_met(&self) -> bool {
        self.front == self.entries.len()
            && self.again.iter().all(|index| self.entries[*index].mark != Mark::Waiting)
    }

    /// Certified tracks still in requests that have no queue row, for the repair
    /// queue once the ends have met
    pub fn meet(&mut self) -> Vec<Gap> {
        let mut queued = Vec::new();
        for entry in &mut self.entries {
            if entry.mark == Mark::Asked && entry.gap.is_certified && entry.row == Row::Absent {
                entry.row = Row::Ours;
                queued.push(entry.gap);
            }
        }

        queued
    }

    /// Certified tracks still in requests and not yet on disk. A track found on
    /// disk is marked stored, so a later answer for it changes nothing
    pub fn in_flight(&mut self, mut is_written: impl FnMut(&Gap) -> bool) -> usize {
        let mut pending = 0;
        for entry in &mut self.entries {
            if entry.mark != Mark::Asked || !entry.gap.is_certified {
                continue;
            }

            if is_written(&entry.gap) {
                entry.mark = Mark::Stored;
            } else {
                pending += 1;
            }
        }

        pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: Address = Address::new([1; 32]);
    const OTHER: Address = Address::new([2; 32]);

    fn track(n: u16) -> Address {
        let mut bytes = [0u8; 32];
        bytes[..2].copy_from_slice(&n.to_be_bytes());
        Address::new(bytes)
    }

    fn gap(n: u16) -> Gap {
        Gap {
            track: track(n),
            stored: SpoolIndex(5),
            is_certified: true,
        }
    }

    fn uncertified(n: u16) -> Gap {
        Gap {
            is_certified: false,
            ..gap(n)
        }
    }

    // one owner with `count` certified gaps and no queue rows
    fn owner_list(count: u16) -> OwnerList {
        let gaps = (0..count).map(gap).collect();
        let mut list = MissingList::build(gaps, |_| Some(OWNER), |_| false);
        list.owners.remove(0)
    }

    fn indices(batch: &[Ask]) -> Vec<usize> {
        batch.iter().map(|ask| ask.index).collect()
    }

    // each stored spool's gaps go to its own previous owner, and a spool with
    // none queues its certified gaps that have no row yet
    #[test]
    fn routes_by_owner() {
        let gaps = vec![
            Gap { stored: SpoolIndex(7), ..gap(1) },
            Gap { stored: SpoolIndex(5), ..gap(2) },
            Gap { stored: SpoolIndex(9), ..gap(3) },
            Gap { stored: SpoolIndex(9), ..uncertified(4) },
            Gap { stored: SpoolIndex(9), ..gap(5) },
        ];
        let owner_of = |stored: SpoolIndex| match stored {
            SpoolIndex(5) => Some(OWNER),
            SpoolIndex(7) => Some(OTHER),
            _ => None,
        };

        let list = MissingList::build(gaps, owner_of, |gap| gap.track == track(5));

        let owners: Vec<(Address, usize)> = list.owners.iter().map(|owner| (owner.owner(), owner.len())).collect();
        assert_eq!(owners, vec![(OTHER, 1), (OWNER, 1)]);
        assert_eq!(list.unsourced, vec![Gap { stored: SpoolIndex(9), ..gap(3) }]);
    }

    // an owner's share runs by stored spool, then track
    #[test]
    fn sorted_by_spool_then_track() {
        let gaps = vec![
            Gap { stored: SpoolIndex(9), ..gap(1) },
            Gap { stored: SpoolIndex(5), ..gap(3) },
            Gap { stored: SpoolIndex(5), ..gap(2) },
        ];
        let mut list = MissingList::build(gaps, |_| Some(OWNER), |_| false);

        let batch = list.owners[0].next_batch(10);
        let order: Vec<(SpoolIndex, Address)> = batch.iter().map(|ask| (ask.gap.stored, ask.gap.track)).collect();
        assert_eq!(order, vec![
            (SpoolIndex(5), track(2)),
            (SpoolIndex(5), track(3)),
            (SpoolIndex(9), track(1)),
        ]);
    }

    #[test]
    fn batches_from_front() {
        let mut list = owner_list(5);

        assert_eq!(indices(&list.next_batch(2)), vec![0, 1]);
        assert_eq!(indices(&list.next_batch(2)), vec![2, 3]);
        assert_eq!(indices(&list.next_batch(2)), vec![4]);
        assert!(list.next_batch(2).is_empty());
    }

    // tracks the owner left out are asked for again before the front moves on
    #[test]
    fn left_out_first() {
        let mut list = owner_list(6);
        list.next_batch(3);

        list.left_out(1);
        list.left_out(2);
        assert_eq!(indices(&list.next_batch(3)), vec![1, 2, 3]);
    }

    // the window takes certified tracks from the back that have no row yet
    #[test]
    fn window_from_back() {
        let mut gaps: Vec<Gap> = (0..40).map(gap).collect();
        gaps[39] = uncertified(39);
        let mut list = MissingList::build(gaps, |_| Some(OWNER), |gap| gap.track == track(38));
        let list = &mut list.owners[0];

        // 39 is uncertified and 38 already has a row, so the window runs 37 down to 6.
        let queued = list.refill(|_| false);
        let expected: Vec<Gap> = (6..38).rev().map(gap).collect();
        assert_eq!(queued, expected);
    }

    // the window refills only once fewer than the refill mark still lack their slice
    #[test]
    fn window_refills_by_presence() {
        let mut list = owner_list(100);
        let first = list.refill(|_| false);
        assert_eq!(first.len(), WINDOW_SIZE);

        // Ten written leaves 22 lacking, which is still enough.
        let written: Vec<Address> = first[..10].iter().map(|gap| gap.track).collect();
        assert!(list.refill(|gap| written.contains(&gap.track)).is_empty());

        // Seventeen written leaves 15, so the window tops back up to 32.
        let written: Vec<Address> = first[..17].iter().map(|gap| gap.track).collect();
        assert_eq!(list.refill(|gap| written.contains(&gap.track)).len(), 17);
    }

    // a repair that fails into recovery writes nothing, so the window doesn't move
    #[test]
    fn window_waits_on_presence() {
        let mut list = owner_list(100);
        assert_eq!(list.refill(|_| false).len(), WINDOW_SIZE);
        assert!(list.refill(|_| false).is_empty());
        assert!(list.refill(|_| false).is_empty());
    }

    // the window never takes a track the front has already handed out
    #[test]
    fn window_stops_at_front() {
        let mut list = owner_list(40);
        list.next_batch(30);

        let queued = list.refill(|_| false);
        let expected: Vec<Gap> = (30..40).rev().map(gap).collect();
        assert_eq!(queued, expected);
    }

    // the front still asks for window tracks, and storing one says its row goes
    #[test]
    fn front_takes_back_window() {
        let mut list = owner_list(40);
        list.refill(|_| false);

        let batch = list.next_batch(40);
        assert_eq!(batch.len(), 40);
        assert!(!batch[0].is_queued);
        assert!(batch[39].is_queued);

        assert!(!list.stored(0));
        assert!(list.stored(39));
    }

    // a failed or missing track is queued once, and only when certified with no row
    #[test]
    fn passed_queues_once() {
        let gaps = vec![gap(0), uncertified(1), gap(2)];
        let mut list = MissingList::build(gaps, |_| Some(OWNER), |gap| gap.track == track(2));
        let list = &mut list.owners[0];
        list.next_batch(3);

        assert_eq!(list.passed(0), Some(gap(0)));
        assert_eq!(list.passed(0), None);
        assert_eq!(list.passed(1), None);
        assert_eq!(list.passed(2), None);

        // A passed track is never handed out again.
        assert!(list.next_batch(3).is_empty());
    }

    // the ends meet once every track has been handed out
    #[test]
    fn meets_when_handed_out() {
        let mut list = owner_list(4);
        assert!(!list.has_met());

        list.next_batch(4);
        assert!(list.has_met());

        list.left_out(3);
        assert!(!list.has_met());

        list.next_batch(4);
        assert!(list.has_met());
    }

    // at the meeting, tracks still in requests are queued unless they have a row
    #[test]
    fn meet_queues_in_flight() {
        let gaps = vec![gap(0), gap(1), uncertified(2), gap(3)];
        let mut list = MissingList::build(gaps, |_| Some(OWNER), |gap| gap.track == track(3));
        let list = &mut list.owners[0];
        list.next_batch(4);
        list.stored(0);

        assert_eq!(list.meet(), vec![gap(1)]);
        assert!(list.stored(1));
    }

    // only certified tracks still in requests and not on disk count as in flight
    #[test]
    fn in_flight_by_presence() {
        let gaps = vec![gap(0), gap(1), uncertified(2)];
        let mut list = MissingList::build(gaps, |_| Some(OWNER), |_| false);
        let list = &mut list.owners[0];
        list.next_batch(3);

        assert_eq!(list.in_flight(|_| false), 2);
        assert_eq!(list.in_flight(|gap| gap.track == track(0)), 1);
        assert_eq!(list.in_flight(|_| false), 1);

        // An answer for a track found on disk asks for nothing again.
        list.left_out(0);
        assert!(list.has_met());
    }
}
