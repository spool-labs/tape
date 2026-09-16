//! Following one list: the tapes its rack reaches, and the entries present on its blacklist tapes

use std::collections::{HashMap, HashSet};

use tape_core::system::BlacklistEntry;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_rack::reach::{Reach, TrackRef, reach};
use tape_rack::source::{TapeSource, Track};

use crate::batch::{parse, parse_first};
use crate::entry::Entry;
use crate::error::BlacklistError;
use crate::extract::Change;

const FIRST_TRACK: TrackNumber = TrackNumber(0);

/// A track that names nothing because it does not parse
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Skipped {
    pub track: TrackRef,
    pub reason: String,
}

/// What changes did to what one list denies
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListChanges {
    pub denied: Vec<BlacklistEntry>,
    pub allowed: Vec<BlacklistEntry>,
}

/// What a list holds from its blacklist tapes
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Contents {
    blacklist_tapes: HashSet<Address>,
    /// Targets each present, readable track names, by track address
    tracks: HashMap<Address, Vec<BlacklistEntry>>,
    missing: HashMap<Address, TrackRef>,
    skipped: HashMap<Address, Skipped>,
}

/// One followed list
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct List {
    root: Address,
    reach: Reach,
    contents: Contents,
    /// How many present tracks name each denied target
    counts: HashMap<BlacklistEntry, usize>,
}

impl List {
    /// A list with nothing read yet
    pub fn new(root: Address) -> Self {
        Self {
            root,
            reach: Reach::default(),
            contents: Contents::default(),
            counts: HashMap::new(),
        }
    }

    pub fn root(&self) -> Address {
        self.root
    }

    /// Whether a change to a tape can matter to this list
    pub fn watches(&self, tape: Address) -> bool {
        tape == self.root || self.reach.tapes.contains(&tape) || self.reach.named.contains(&tape)
    }

    /// Live tapes the list's rack reaches, the rack included
    pub fn reached(&self) -> &HashSet<Address> {
        &self.reach.tapes
    }

    pub fn is_denied(&self, target: &BlacklistEntry) -> bool {
        self.counts.contains_key(target)
    }

    pub fn denied(&self) -> impl Iterator<Item = &BlacklistEntry> {
        self.counts.keys()
    }

    /// Present tracks whose bytes the list still needs
    pub fn missing(&self) -> Vec<TrackRef> {
        let mut missing = self.contents.missing.clone();
        for track in &self.reach.missing {
            missing.insert(track.address, *track);
        }
        missing.into_values().collect()
    }

    /// Tracks that name nothing because they do not parse
    pub fn skipped(&self) -> Vec<Skipped> {
        let mut skipped: Vec<Skipped> = self.contents.skipped.values().cloned().collect();
        for track in &self.reach.skipped {
            skipped.push(Skipped {
                track: track.track,
                reason: track.reason.clone(),
            });
        }
        skipped
    }

    /// Read the whole list again from the source
    pub fn rebuild<S: TapeSource>(&mut self, source: &S) -> Result<ListChanges, S::Error> {
        let reach = reach(self.root, source)?;
        let mut contents = Contents::default();
        for tape in &reach.tapes {
            if !reach.racks.contains(tape) {
                read_tape(*tape, source, &mut contents)?;
            }
        }

        let counts = count(&contents.tracks);
        let changes = compare(&self.counts, &counts);
        self.reach = reach;
        self.contents = contents;
        self.counts = counts;
        Ok(changes)
    }

    /// Apply changes the source already reflects
    ///
    /// A change to what the rack reaches, or to which tapes are blacklist tapes,
    /// reads the list again; a change to a blacklist tape's later track reads
    /// just that track.
    pub fn apply<S: TapeSource>(&mut self, changes: &[Change], source: &S) -> Result<ListChanges, S::Error> {
        if self.reshapes(changes, source)? {
            return self.rebuild(source);
        }

        let mut touched = Vec::new();
        for change in changes {
            match change {
                Change::Written { tape, track_number } => {
                    if self.contents.blacklist_tapes.contains(tape) {
                        let track = source.track(*tape, *track_number)?;
                        self.write_track(*tape, *track_number, track, &mut touched);
                    }
                }
                Change::Removed { track } => {
                    self.contents.missing.remove(track);
                    self.drop_track(*track, &mut touched);
                }
                Change::Tape { .. } | Change::Epoch { .. } => {}
            }
        }
        Ok(self.settle(touched))
    }

    fn reshapes<S: TapeSource>(&self, changes: &[Change], source: &S) -> Result<bool, S::Error> {
        for change in changes {
            let does = match change {
                Change::Written { tape, track_number } => {
                    self.reach.racks.contains(tape)
                        || (*track_number == FIRST_TRACK && self.reach.tapes.contains(tape))
                        || (self.watches(*tape) && !self.reach.tapes.contains(tape))
                }
                Change::Removed { track } => self.reach.tracks.contains_key(track),
                Change::Tape { tape } => self.watches(*tape),
                Change::Epoch { .. } => self.has_expired(source)?,
            };
            if does {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn has_expired<S: TapeSource>(&self, source: &S) -> Result<bool, S::Error> {
        for tape in &self.reach.tapes {
            if !source.is_live(*tape)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn write_track(
        &mut self,
        tape: Address,
        track_number: TrackNumber,
        track: Track,
        touched: &mut Vec<(BlacklistEntry, bool)>,
    ) {
        let (address, bytes) = match track {
            Track::Absent | Track::Opaque => return,
            Track::Pending { address } => (address, None),
            Track::Written { address, bytes } => (address, Some(bytes)),
        };
        self.drop_track(address, touched);
        let reference = TrackRef {
            tape,
            track_number,
            address,
        };
        let Some(bytes) = bytes else {
            self.contents.missing.insert(address, reference);
            return;
        };
        self.contents.missing.remove(&address);
        match parse(&bytes) {
            Ok(entries) => {
                let targets = targets(&entries);
                for target in &targets {
                    note(touched, &self.counts, *target);
                    *self.counts.entry(*target).or_insert(0) += 1;
                }
                self.contents.tracks.insert(address, targets);
            }
            Err(error) => {
                self.contents.skipped.insert(address, skipped(reference, &error));
            }
        }
    }

    fn drop_track(&mut self, address: Address, touched: &mut Vec<(BlacklistEntry, bool)>) {
        self.contents.skipped.remove(&address);
        let Some(targets) = self.contents.tracks.remove(&address) else {
            return;
        };
        for target in targets {
            note(touched, &self.counts, target);
            release(&mut self.counts, target);
        }
    }

    fn settle(&self, touched: Vec<(BlacklistEntry, bool)>) -> ListChanges {
        let mut changes = ListChanges::default();
        for (target, was_denied) in touched {
            match (was_denied, self.counts.contains_key(&target)) {
                (false, true) => changes.denied.push(target),
                (true, false) => changes.allowed.push(target),
                (true, true) | (false, false) => {}
            }
        }
        changes
    }
}

/// Read a reached tape that is not a rack: whether it is a blacklist tape, and what its tracks name
fn read_tape<S: TapeSource>(tape: Address, source: &S, contents: &mut Contents) -> Result<(), S::Error> {
    match source.track(tape, FIRST_TRACK)? {
        Track::Absent | Track::Opaque => return Ok(()),
        Track::Pending { address } => {
            let reference = TrackRef {
                tape,
                track_number: FIRST_TRACK,
                address,
            };
            contents.missing.insert(address, reference);
            return Ok(());
        }
        Track::Written { address, bytes } => {
            let Some(entries) = parse_first(&bytes) else {
                return Ok(());
            };
            let reference = TrackRef {
                tape,
                track_number: FIRST_TRACK,
                address,
            };
            hold(contents, reference, entries);
        }
    }
    contents.blacklist_tapes.insert(tape);

    for track_number in source.track_numbers(tape)? {
        if track_number == FIRST_TRACK {
            continue;
        }
        match source.track(tape, track_number)? {
            Track::Absent | Track::Opaque => {}
            Track::Pending { address } => {
                let reference = TrackRef {
                    tape,
                    track_number,
                    address,
                };
                contents.missing.insert(address, reference);
            }
            Track::Written { address, bytes } => {
                let reference = TrackRef {
                    tape,
                    track_number,
                    address,
                };
                hold(contents, reference, parse(&bytes));
            }
        }
    }
    Ok(())
}

fn hold(contents: &mut Contents, track: TrackRef, entries: Result<Vec<Entry>, BlacklistError>) {
    match entries {
        Ok(entries) => {
            contents.tracks.insert(track.address, targets(&entries));
        }
        Err(error) => {
            contents.skipped.insert(track.address, skipped(track, &error));
        }
    }
}

fn skipped(track: TrackRef, error: &BlacklistError) -> Skipped {
    Skipped {
        track,
        reason: error.to_string(),
    }
}

/// The distinct targets a track names
fn targets(entries: &[Entry]) -> Vec<BlacklistEntry> {
    let distinct: HashSet<BlacklistEntry> = entries.iter().map(|entry| entry.target).collect();
    distinct.into_iter().collect()
}

fn count(tracks: &HashMap<Address, Vec<BlacklistEntry>>) -> HashMap<BlacklistEntry, usize> {
    let mut counts = HashMap::new();
    for targets in tracks.values() {
        for target in targets {
            *counts.entry(*target).or_insert(0) += 1;
        }
    }
    counts
}

fn compare(before: &HashMap<BlacklistEntry, usize>, after: &HashMap<BlacklistEntry, usize>) -> ListChanges {
    let mut changes = ListChanges::default();
    for target in after.keys() {
        if !before.contains_key(target) {
            changes.denied.push(*target);
        }
    }
    for target in before.keys() {
        if !after.contains_key(target) {
            changes.allowed.push(*target);
        }
    }
    changes
}

/// Remember whether a target was denied before it was first touched
fn note(touched: &mut Vec<(BlacklistEntry, bool)>, counts: &HashMap<BlacklistEntry, usize>, target: BlacklistEntry) {
    if !touched.iter().any(|(seen, _)| *seen == target) {
        touched.push((target, counts.contains_key(&target)));
    }
}

/// Drop one naming of a target
fn release(counts: &mut HashMap<BlacklistEntry, usize>, target: BlacklistEntry) {
    let is_last = match counts.get_mut(&target) {
        Some(count) => {
            *count = count.saturating_sub(1);
            *count == 0
        }
        None => false,
    };
    if is_last {
        counts.remove(&target);
    }
}
