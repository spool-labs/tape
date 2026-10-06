//! Where the archive schedule's rows sit at a given epoch

use super::EpochScheduleError;
use crate::types::EpochNumber;

/// Rows in the archive schedule, across every tier
pub const SCHEDULE_ROWS: usize = 512;

/// Tiers in the archive schedule. A row in tier `t` covers `1 << t` epochs
pub const SCHEDULE_TIERS: usize = 10;

/// Rows each tier holds when it is full
pub const TIER_ROWS: [u64; SCHEDULE_TIERS] = [128, 64, 64, 64, 64, 32, 32, 32, 16, 16];

/// Every reservation of this many epochs or fewer fits in the schedule
pub const SCHEDULE_MIN_REACH: u64 = 20_993;

const LAST_TIER: usize = SCHEDULE_TIERS - 1;

/// The first slot of each tier's region
const TIER_SLOTS: [usize; SCHEDULE_TIERS] = tier_slots();

const fn tier_slots() -> [usize; SCHEDULE_TIERS] {
    let mut slots = [0; SCHEDULE_TIERS];
    let mut next = 0;
    let mut tier = 0;

    while tier < SCHEDULE_TIERS {
        slots[tier] = next;
        next += TIER_ROWS[tier] as usize;
        tier += 1;
    }

    assert!(next == SCHEDULE_ROWS);
    slots
}

/// One row of the schedule: a run of epochs with the same usage
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduleRow {
    /// The row's first epoch
    pub start: EpochNumber,

    /// The epoch after the row's last
    pub end: EpochNumber,

    slot: usize,
}

impl ScheduleRow {
    /// The row's place in the schedule's storage, always below `SCHEDULE_ROWS`
    #[inline]
    pub fn slot(&self) -> usize {
        self.slot
    }

    /// Epochs the row covers
    #[inline]
    pub fn width(&self) -> u64 {
        (self.end - self.start).as_u64()
    }
}

/// The epochs each tier covers at one moment
///
/// The first tier starts at the current epoch and each tier starts where the
/// one before it ends. The current epoch fixes the whole layout, so the
/// schedule stores none of it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduleLayout {
    now: EpochNumber,
    ends: [EpochNumber; SCHEDULE_TIERS],
}

impl ScheduleLayout {
    /// Lay the tiers out from the current epoch
    pub fn try_new(now: EpochNumber) -> Result<Self, EpochScheduleError> {
        let mut ends = [now; SCHEDULE_TIERS];
        let mut start = now.as_u64();

        for (tier, end) in ends.iter_mut().enumerate() {
            let width = row_width(tier);
            let reach = if tier == LAST_TIER {
                start.checked_add(TIER_ROWS[tier] * width)
            } else {
                // A tier holds all of its rows or one fewer, so that it ends
                // where the next tier's rows line up.
                start
                    .checked_add((TIER_ROWS[tier] - 1) * width)
                    .and_then(|reach| reach.checked_next_multiple_of(2 * width))
            };

            start = reach.ok_or(EpochScheduleError::Overflow)?;
            *end = EpochNumber(start);
        }

        Ok(Self { now, ends })
    }

    /// The epoch the first row starts at
    #[inline]
    pub fn start(&self) -> EpochNumber {
        self.now
    }

    /// The epoch after the last row
    #[inline]
    pub fn end(&self) -> EpochNumber {
        self.ends[LAST_TIER]
    }

    /// The row that covers an epoch
    pub fn row_at(&self, epoch: EpochNumber) -> Result<ScheduleRow, EpochScheduleError> {
        if epoch < self.now {
            return Err(EpochScheduleError::EpochInPast);
        }

        for (tier, end) in self.ends.iter().enumerate() {
            if epoch < *end {
                let width = row_width(tier);
                let start = EpochNumber(epoch.as_u64() / width * width);
                return Ok(row(tier, start));
            }
        }

        Err(EpochScheduleError::EpochTooFar)
    }

    /// Whether a row starts at this epoch, or the schedule ends at it
    pub fn is_boundary(&self, epoch: EpochNumber) -> bool {
        self.boundary_at_or_after(epoch) == Some(epoch)
    }

    /// The first row boundary at or after an epoch, when the schedule reaches that far
    pub fn boundary_at_or_after(&self, epoch: EpochNumber) -> Option<EpochNumber> {
        let epoch = epoch.max(self.now);

        for (tier, end) in self.ends.iter().enumerate() {
            if epoch < *end {
                // A tier ends on a multiple of its row width, so this stays inside it.
                let boundary = epoch.as_u64().checked_next_multiple_of(row_width(tier))?;
                return Some(EpochNumber(boundary));
            }
        }

        (epoch == self.end()).then_some(epoch)
    }

    /// The last row boundary at or before an epoch
    pub fn boundary_at_or_before(&self, epoch: EpochNumber) -> EpochNumber {
        match self.row_at(epoch.max(self.now)) {
            Ok(row) => row.start,
            Err(_) => self.end(),
        }
    }

    /// The latest expiry a reservation made now can have: the last row
    /// boundary within `max_epochs` of the current epoch
    pub fn latest_expiry(&self, max_epochs: u64) -> EpochNumber {
        let reach = self.now.saturating_add(EpochNumber(max_epochs));
        self.boundary_at_or_before(reach)
    }

    /// The expiry a request gets: the next row boundary, stopping at the latest expiry
    pub fn expiry_for(&self, requested: EpochNumber, max_epochs: u64) -> EpochNumber {
        let latest = self.latest_expiry(max_epochs);

        match self.boundary_at_or_after(requested) {
            Some(boundary) => boundary.min(latest),
            None => latest,
        }
    }

    /// The rows that make up `[start, end)`. Both ends must be row boundaries
    pub fn rows(
        &self,
        start: EpochNumber,
        end: EpochNumber,
    ) -> Result<ScheduleRows, EpochScheduleError> {
        if start < self.now {
            return Err(EpochScheduleError::EpochInPast);
        }
        if end <= start {
            return Err(EpochScheduleError::EndNotAfterStart);
        }
        if end > self.end() {
            return Err(EpochScheduleError::EpochTooFar);
        }
        if !self.is_boundary(start) || !self.is_boundary(end) {
            return Err(EpochScheduleError::MidRow);
        }

        Ok(ScheduleRows { layout: *self, tier: 0, next: start, end })
    }

    /// Every row, from the current epoch to the end of the schedule
    pub fn all_rows(&self) -> ScheduleRows {
        ScheduleRows { layout: *self, tier: 0, next: self.now, end: self.end() }
    }

    /// The parent rows that split when the schedule moves from this layout to
    /// the next epoch's, finest tier first. Each one becomes two rows of the
    /// tier below it.
    pub fn splits(&self, next: &ScheduleLayout) -> ScheduleSplits {
        ScheduleSplits { old: *self, new: *next, tier: 0 }
    }
}

/// Walks the rows of a run of epochs, earliest first
#[derive(Clone, Copy, Debug)]
pub struct ScheduleRows {
    layout: ScheduleLayout,
    tier: usize,
    next: EpochNumber,
    end: EpochNumber,
}

impl Iterator for ScheduleRows {
    type Item = ScheduleRow;

    fn next(&mut self) -> Option<ScheduleRow> {
        if self.next >= self.end {
            return None;
        }

        while self.tier < LAST_TIER && self.next >= self.layout.ends[self.tier] {
            self.tier += 1;
        }

        let found = row(self.tier, self.next);
        self.next = found.end;
        Some(found)
    }
}

/// One parent row splitting into two rows of the tier below it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduleSplit {
    /// The row that splits
    pub parent: ScheduleRow,

    /// The two rows that take its place
    pub children: [ScheduleRow; 2],
}

/// Walks the splits of one epoch advance, finest tier first
#[derive(Clone, Copy, Debug)]
pub struct ScheduleSplits {
    old: ScheduleLayout,
    new: ScheduleLayout,
    tier: usize,
}

impl Iterator for ScheduleSplits {
    type Item = ScheduleSplit;

    fn next(&mut self) -> Option<ScheduleSplit> {
        // A tier takes a row from the one above only when it has grown, and
        // the tier above loses a row only then, so the splits stop at the
        // first tier that kept its end.
        if self.tier >= LAST_TIER || self.new.ends[self.tier] == self.old.ends[self.tier] {
            return None;
        }

        let start = self.old.ends[self.tier];
        let first = row(self.tier, start);
        let second = row(self.tier, first.end);
        let parent = row(self.tier + 1, start);

        self.tier += 1;
        Some(ScheduleSplit { parent, children: [first, second] })
    }
}

#[inline]
fn row_width(tier: usize) -> u64 {
    1 << tier
}

/// The row of a tier that starts at an epoch. A tier's rows are consecutive
/// and never outnumber its region, so no two of them share a slot.
fn row(tier: usize, start: EpochNumber) -> ScheduleRow {
    let width = row_width(tier);
    let index = (start.as_u64() / width) % TIER_ROWS[tier];

    ScheduleRow {
        start,
        end: EpochNumber(start.as_u64().saturating_add(width)),
        slot: TIER_SLOTS[tier] + index as usize,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const SCHEDULE_MAX_REACH: u64 = 21_504;

    fn layout(now: u64) -> ScheduleLayout {
        ScheduleLayout::try_new(EpochNumber(now)).expect("layout")
    }

    fn tier_start(layout: &ScheduleLayout, tier: usize) -> u64 {
        match tier {
            0 => layout.now.as_u64(),
            _ => layout.ends[tier - 1].as_u64(),
        }
    }

    // every starting epoch gives full or one-short tiers that line up
    #[test]
    fn every_phase() {
        for now in 0..2 * SCHEDULE_ROWS as u64 {
            let layout = layout(now);

            for tier in 0..SCHEDULE_TIERS {
                let width = row_width(tier);
                let start = tier_start(&layout, tier);
                let rows = (layout.ends[tier].as_u64() - start) / width;

                assert_eq!(start % width, 0);
                assert!(rows == TIER_ROWS[tier] || rows + 1 == TIER_ROWS[tier]);
            }

            assert_eq!(
                (layout.ends[LAST_TIER].as_u64() - tier_start(&layout, LAST_TIER)) / row_width(LAST_TIER),
                TIER_ROWS[LAST_TIER],
            );

            let reach = (layout.end() - layout.start()).as_u64();
            assert!((SCHEDULE_MIN_REACH..=SCHEDULE_MAX_REACH).contains(&reach));
        }
    }

    // the shortest and the longest reach both occur
    #[test]
    fn reach_bounds() {
        let mut shortest = u64::MAX;
        let mut longest = 0;

        for now in 0..SCHEDULE_ROWS as u64 {
            let layout = layout(now);
            let reach = (layout.end() - layout.start()).as_u64();
            shortest = shortest.min(reach);
            longest = longest.max(reach);
        }

        assert_eq!(shortest, SCHEDULE_MIN_REACH);
        assert_eq!(longest, SCHEDULE_MAX_REACH);
    }

    // rows are back to back, cover the whole reach, and never share a slot
    #[test]
    fn rows_partition() {
        for now in [0, 1, 127, 128, 511, 512, 1_000, 21_503, 21_504, 1 << 40] {
            let layout = layout(now);
            let mut slots = HashSet::new();
            let mut next = layout.start();

            for row in layout.all_rows() {
                assert_eq!(row.start, next);
                assert!(row.slot() < SCHEDULE_ROWS);
                assert!(slots.insert(row.slot()));
                next = row.end;
            }

            assert_eq!(next, layout.end());
            assert!(slots.len() <= SCHEDULE_ROWS);
        }
    }

    // a boundary stays a boundary as epochs pass
    #[test]
    fn boundaries_stay() {
        let mut old = layout(0);

        for now in 1..3_000 {
            let new = layout(now);

            for row in old.all_rows() {
                assert!(row.start < new.start() || new.is_boundary(row.start));
            }
            assert!(new.is_boundary(old.end()));

            old = new;
        }
    }

    // the next epoch's layout is this one with the first row gone and each split applied
    #[test]
    fn splits_match() {
        let mut old = layout(0);

        for now in 1..3_000 {
            let new = layout(now);
            let mut expected: Vec<ScheduleRow> = old.all_rows().skip(1).collect();

            for split in old.splits(&new) {
                let at = expected
                    .iter()
                    .position(|row| *row == split.parent)
                    .expect("parent is a row");
                expected.splice(at..=at, split.children);
            }
            if expected.last().map(|row| row.end) != Some(new.end()) {
                expected.push(row(LAST_TIER, old.end()));
            }

            let rows: Vec<ScheduleRow> = new.all_rows().collect();
            assert_eq!(rows, expected);

            old = new;
        }
    }

    // rounding lands on the row boundaries either side of an epoch
    #[test]
    fn rounding() {
        let layout = layout(1_000);

        // one-epoch rows run to 1,128 and two-epoch rows to 1,256
        assert_eq!(layout.boundary_at_or_after(EpochNumber(1_127)), Some(EpochNumber(1_127)));
        assert_eq!(layout.boundary_at_or_after(EpochNumber(1_129)), Some(EpochNumber(1_130)));
        assert_eq!(layout.boundary_at_or_before(EpochNumber(1_129)), EpochNumber(1_128));
        assert_eq!(layout.boundary_at_or_after(EpochNumber(1_257)), Some(EpochNumber(1_260)));
        assert_eq!(layout.boundary_at_or_before(EpochNumber(1_259)), EpochNumber(1_256));

        assert_eq!(layout.boundary_at_or_after(EpochNumber(3)), Some(EpochNumber(1_000)));
        assert_eq!(layout.boundary_at_or_before(EpochNumber(3)), EpochNumber(1_000));
        assert_eq!(layout.boundary_at_or_after(layout.end()), Some(layout.end()));
        assert_eq!(layout.boundary_at_or_after(layout.end().next()), None);
        assert_eq!(layout.boundary_at_or_before(EpochNumber(u64::MAX)), layout.end());

        assert!(layout.is_boundary(EpochNumber(1_128)));
        assert!(!layout.is_boundary(EpochNumber(1_129)));
        assert!(!layout.is_boundary(EpochNumber(999)));
    }

    // a request rounds up, and the maximum cuts it back to a boundary
    #[test]
    fn expiry() {
        let layout = layout(1_000);

        assert_eq!(layout.expiry_for(EpochNumber(1_050), u64::MAX), EpochNumber(1_050));
        assert_eq!(layout.expiry_for(EpochNumber(1_129), u64::MAX), EpochNumber(1_130));
        assert_eq!(layout.expiry_for(EpochNumber(u64::MAX), u64::MAX), layout.end());

        assert_eq!(layout.latest_expiry(129), EpochNumber(1_128));
        assert_eq!(layout.expiry_for(EpochNumber(1_129), 129), EpochNumber(1_128));
        assert_eq!(layout.expiry_for(EpochNumber(5_000), 100), EpochNumber(1_100));
        assert_eq!(layout.expiry_for(EpochNumber(1_050), 100), EpochNumber(1_050));
        assert_eq!(layout.latest_expiry(0), EpochNumber(1_000));
    }

    // a range must sit on row boundaries inside the schedule
    #[test]
    fn range_errors() {
        let layout = layout(1_000);
        let end = layout.end();

        assert_eq!(
            layout.rows(EpochNumber(999), EpochNumber(1_010)).err(),
            Some(EpochScheduleError::EpochInPast),
        );
        assert_eq!(
            layout.rows(EpochNumber(1_010), EpochNumber(1_010)).err(),
            Some(EpochScheduleError::EndNotAfterStart),
        );
        assert_eq!(
            layout.rows(EpochNumber(1_010), EpochNumber(end.as_u64() + 512)).err(),
            Some(EpochScheduleError::EpochTooFar),
        );
        assert_eq!(
            layout.rows(EpochNumber(1_010), EpochNumber(1_129)).err(),
            Some(EpochScheduleError::MidRow),
        );
        assert_eq!(
            layout.rows(EpochNumber(1_129), EpochNumber(1_256)).err(),
            Some(EpochScheduleError::MidRow),
        );
        assert_eq!(
            layout.rows(EpochNumber(1_000), end).map(|rows| rows.count()).ok(),
            Some(layout.all_rows().count()),
        );
    }

    // an epoch too close to the largest number has no layout
    #[test]
    fn overflow() {
        assert_eq!(
            ScheduleLayout::try_new(EpochNumber(u64::MAX - 100)),
            Err(EpochScheduleError::Overflow),
        );
    }
}
