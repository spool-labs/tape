//! The last hour of transfer, folded into wall-clock minutes
//!
//! Everything the board carries is a cumulative counter, so a dashboard can
//! only chart the traffic it was open for: a page that just loaded reports an
//! idle node however busy it has been. The node keeps the minutes itself and
//! ships them, so the panel opens on what actually happened.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use tape_observe_api::{
    BandwidthMinute, BANDWIDTH_MINUTES, SPOOL_OP_RECOVER, SPOOL_OP_REPAIR, SPOOL_OP_SYNC,
    SPOOL_STAGE_FETCHED,
};

/// One reading of the counters a minute is built from
#[derive(Clone, Copy, Default)]
struct Flows {
    sync: u64,
    repair: u64,
    recover: u64,
    upload: u64,
}

impl Flows {
    fn read() -> Self {
        let m = tape_metrics::metrics();
        let fetched = |op: &str| {
            m.spool_bytes_total
                .with_label_values(&[op, SPOOL_STAGE_FETCHED])
                .get()
        };
        Self {
            sync: fetched(SPOOL_OP_SYNC),
            repair: fetched(SPOOL_OP_REPAIR),
            recover: fetched(SPOOL_OP_RECOVER),
            upload: m.bytes_uploaded.get(),
        }
    }

    /// This reading minus an earlier one, clamped so a counter that went
    /// backwards reads as idle rather than as an hour of traffic at once
    fn since(&self, earlier: &Flows) -> Flows {
        Flows {
            sync: self.sync.saturating_sub(earlier.sync),
            repair: self.repair.saturating_sub(earlier.repair),
            recover: self.recover.saturating_sub(earlier.recover),
            upload: self.upload.saturating_sub(earlier.upload),
        }
    }
}

struct History {
    /// Contiguous minutes, oldest first, the last one still filling
    minutes: Vec<BandwidthMinute>,
    /// The reading the open minute has already been credited with
    last: Flows,
}

impl History {
    fn new(minute: u64, now: Flows) -> Self {
        Self {
            minutes: vec![BandwidthMinute { minute, ..BandwidthMinute::default() }],
            last: now,
        }
    }

    fn fold(&mut self, minute: u64, now: Flows) {
        let open = self.minutes.last().map(|bucket| bucket.minute).unwrap_or(minute);
        if minute.saturating_sub(open) >= BANDWIDTH_MINUTES as u64 {
            *self = Self::new(minute, now);
            return;
        }
        for next in (open + 1)..=minute {
            self.minutes.push(BandwidthMinute { minute: next, ..BandwidthMinute::default() });
            if self.minutes.len() > BANDWIDTH_MINUTES {
                self.minutes.remove(0);
            }
        }

        let delta = now.since(&self.last);
        self.last = now;
        let Some(bucket) = self.minutes.last_mut() else { return };
        bucket.sync = bucket.sync.saturating_add(delta.sync);
        bucket.repair = bucket.repair.saturating_add(delta.repair);
        bucket.recover = bucket.recover.saturating_add(delta.recover);
        bucket.upload = bucket.upload.saturating_add(delta.upload);
    }
}

static HISTORY: Mutex<Option<History>> = Mutex::new(None);

/// Credit the open minute with everything moved since the last call
///
/// The fold is a delta, so calling more often only sharpens which minute a
/// transfer lands in. It has to run at least once a minute for the window to
/// hold anything, which is why the observe clock drives it rather than a board
/// nobody may ask for.
pub fn sample() {
    let now = Flows::read();
    let minute = unix_minute();
    let Ok(mut history) = HISTORY.lock() else { return };
    match history.as_mut() {
        Some(history) => history.fold(minute, now),
        None => *history = Some(History::new(minute, now)),
    }
}

/// The window as the board carries it, oldest minute first
pub fn history() -> Vec<BandwidthMinute> {
    match HISTORY.lock() {
        Ok(history) => history.as_ref().map(|h| h.minutes.clone()).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

fn unix_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() / 60)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{Flows, History, BANDWIDTH_MINUTES};

    fn moved(upload: u64) -> Flows {
        Flows { upload, ..Flows::default() }
    }

    // everything moved since the last fold lands in the minute still open
    #[test]
    fn credits_open_minute() {
        let mut history = History::new(100, moved(0));

        history.fold(100, moved(4_096));

        assert_eq!(history.minutes.len(), 1);
        assert_eq!(history.minutes[0].upload, 4_096);
    }

    // a minute nothing moved in still gets a bucket, so the window reads as a
    // timeline rather than as a list of busy minutes
    #[test]
    fn fills_quiet_minutes() {
        let mut history = History::new(100, moved(0));

        history.fold(103, moved(512));

        let minutes: Vec<u64> = history.minutes.iter().map(|bucket| bucket.minute).collect();
        assert_eq!(minutes, vec![100, 101, 102, 103]);
        assert_eq!(history.minutes[3].upload, 512);
    }

    // nothing is left to keep after a gap past the window, and crediting one
    // minute with an hour of traffic would draw a spike that never happened
    #[test]
    fn restarts_after_a_gap() {
        let mut history = History::new(100, moved(0));
        history.fold(100, moved(4_096));

        history.fold(100 + BANDWIDTH_MINUTES as u64, moved(9_000));

        assert_eq!(history.minutes.len(), 1);
        assert_eq!(history.minutes[0].upload, 0);
    }

    // the window never grows past the hour it covers
    #[test]
    fn bounded_window() {
        let span = BANDWIDTH_MINUTES as u64;
        let newest = span + 19;
        let mut history = History::new(0, moved(0));

        for minute in 1..=newest {
            history.fold(minute, moved(minute));
        }

        assert_eq!(history.minutes.len(), BANDWIDTH_MINUTES);
        assert_eq!(history.minutes[0].minute, newest - span + 1);
    }
}
