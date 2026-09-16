//! Which tapes a rack reaches

use std::collections::{HashMap, HashSet};

use tape_core::types::TrackNumber;
use tape_crypto::address::Address;

use crate::error::RackError;
use crate::format::{parse, parse_first};
use crate::record::Record;
use crate::source::{TapeSource, Track};

const FIRST_TRACK: TrackNumber = TrackNumber(0);

/// A track a reader read, or still needs
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TrackRef {
    pub tape: Address,
    pub track_number: TrackNumber,
    pub address: Address,
}

/// A rack track that names nothing because it does not parse
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedTrack {
    pub track: TrackRef,
    pub reason: String,
}

/// What a rack reaches, and what deciding it depended on
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Reach {
    /// Live tapes reached, the starting tape included
    pub tapes: HashSet<Address>,
    /// Reached tapes that are racks
    pub racks: HashSet<Address>,
    /// Every tape a reached rack names, live or not
    pub named: HashSet<Address>,
    /// Tracks read to decide the reach, by address: every reached tape's track 0 and every rack track
    pub tracks: HashMap<Address, TrackRef>,
    /// Present tracks whose bytes are needed before the reach is certain
    pub missing: Vec<TrackRef>,
    pub skipped: Vec<SkippedTrack>,
}

impl Reach {
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty()
    }

    fn read(&mut self, tape: Address, track_number: TrackNumber, address: Address) -> TrackRef {
        let track = TrackRef {
            tape,
            track_number,
            address,
        };
        self.tracks.insert(address, track);
        track
    }

    fn wait(&mut self, tape: Address, track_number: TrackNumber, address: Address) {
        let track = self.read(tape, track_number, address);
        self.missing.push(track);
    }

    fn names(&mut self, track: TrackRef, records: Result<Vec<Record>, RackError>) -> Vec<Address> {
        match records {
            Ok(records) => records.into_iter().map(|record| record.tape).collect(),
            Err(error) => {
                self.skipped.push(SkippedTrack {
                    track,
                    reason: error.to_string(),
                });
                Vec::new()
            }
        }
    }
}

/// Every tape `start` reaches: itself while live, and if it is a rack, what it names at any depth
pub fn reach<S: TapeSource>(start: Address, source: &S) -> Result<Reach, S::Error> {
    let mut reach = Reach::default();
    let mut visited = HashSet::new();
    let mut pending = vec![start];
    while let Some(tape) = pending.pop() {
        if !visited.insert(tape) || !source.is_live(tape)? {
            continue;
        }
        reach.tapes.insert(tape);

        let mut names = match source.track(tape, FIRST_TRACK)? {
            Track::Absent | Track::Opaque => continue,
            Track::Pending { address } => {
                reach.wait(tape, FIRST_TRACK, address);
                continue;
            }
            Track::Written { address, bytes } => {
                let track = reach.read(tape, FIRST_TRACK, address);
                match parse_first(&bytes) {
                    Some(records) => reach.names(track, records),
                    None => continue,
                }
            }
        };
        reach.racks.insert(tape);

        for track_number in source.track_numbers(tape)? {
            if track_number == FIRST_TRACK {
                continue;
            }
            match source.track(tape, track_number)? {
                Track::Absent | Track::Opaque => {}
                Track::Pending { address } => reach.wait(tape, track_number, address),
                Track::Written { address, bytes } => {
                    let track = reach.read(tape, track_number, address);
                    names.extend(reach.names(track, parse(&bytes)));
                }
            }
        }

        for named in names {
            reach.named.insert(named);
            pending.push(named);
        }
    }
    Ok(reach)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{render, render_first};
    use crate::source::MemorySource;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn records(tapes: &[Address]) -> Vec<Record> {
        tapes.iter().map(|tape| Record::new(*tape)).collect()
    }

    /// A reserved rack whose track 0 names `tapes`
    fn rack(source: &mut MemorySource, tape: Address, tapes: &[Address]) {
        source.reserve(tape);
        source.write(tape, FIRST_TRACK, &render_first(&records(tapes)));
    }

    fn tapes(tapes: &[Address]) -> HashSet<Address> {
        tapes.iter().copied().collect()
    }

    fn reached(start: Address, source: &MemorySource) -> Reach {
        match reach(start, source) {
            Ok(reach) => reach,
            Err(never) => match never {},
        }
    }

    // a rack reaches the live tapes its tracks name, and itself
    #[test]
    fn flat() {
        let (root, a, b, c) = (address(1), address(2), address(3), address(4));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[a, b]);
        source.write(root, TrackNumber(1), &render(&records(&[c])));
        for tape in [a, b, c] {
            source.reserve(tape);
        }

        let reach = reached(root, &source);

        assert_eq!(reach.tapes, tapes(&[root, a, b, c]));
        assert_eq!(reach.racks, tapes(&[root]));
        assert!(reach.is_complete());
    }

    // a tape whose track 0 declares nothing names nothing, whatever its later tracks hold
    #[test]
    fn undeclared() {
        let (tape, named) = (address(1), address(2));
        let mut source = MemorySource::default();
        source.reserve(tape);
        source.reserve(named);
        source.write(tape, FIRST_TRACK, b"hello");
        source.write(tape, TrackNumber(1), &render_first(&records(&[named])));

        let reach = reached(tape, &source);

        assert_eq!(reach.tapes, tapes(&[tape]));
        assert!(reach.racks.is_empty());
    }

    // nested racks and loops reach the same tapes as one rack naming them all
    #[test]
    fn nested() {
        let (root, inner, leaf) = (address(1), address(2), address(3));
        let mut nested = MemorySource::default();
        rack(&mut nested, root, &[inner]);
        rack(&mut nested, inner, &[leaf, root, inner]);
        nested.reserve(leaf);
        let mut flat = MemorySource::default();
        rack(&mut flat, root, &[inner, leaf]);
        flat.reserve(inner);
        flat.reserve(leaf);

        let nested_reach = reached(root, &nested);

        assert_eq!(nested_reach.tapes, reached(root, &flat).tapes);
        assert_eq!(nested_reach.racks, tapes(&[root, inner]));
    }

    // deleting a record track drops what it named, and deleting track 0 ends the rack
    #[test]
    fn removed() {
        let (root, a, b) = (address(1), address(2), address(3));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[a]);
        source.write(root, TrackNumber(1), &render(&records(&[b])));
        source.reserve(a);
        source.reserve(b);

        source.remove(root, TrackNumber(1));
        let record_removed = reached(root, &source);
        source.remove(root, FIRST_TRACK);
        let header_removed = reached(root, &source);

        assert_eq!(record_removed.tapes, tapes(&[root, a]));
        assert_eq!(header_removed.tapes, tapes(&[root]));
        assert!(header_removed.racks.is_empty());
    }

    // an expired tape stops counting with what it reaches, unless reached another way
    #[test]
    fn expired() {
        let (root, inner, shared, only_inner) = (address(1), address(2), address(3), address(4));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[inner, shared]);
        rack(&mut source, inner, &[shared, only_inner]);
        source.reserve(shared);
        source.reserve(only_inner);

        source.expire(inner);
        let inner_expired = reached(root, &source);
        source.expire(root);
        let root_expired = reached(root, &source);

        assert_eq!(inner_expired.tapes, tapes(&[root, shared]));
        assert!(root_expired.tapes.is_empty());
    }

    // a named tape that is not reserved is named but not reached until it is
    #[test]
    fn unreserved() {
        let (root, later) = (address(1), address(2));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[later]);

        let before = reached(root, &source);
        source.reserve(later);
        let after = reached(root, &source);

        assert!(before.named.contains(&later));
        assert!(!before.tapes.contains(&later));
        assert!(after.tapes.contains(&later));
    }

    // tracks without bytes are reported as missing, and track 0 without bytes decides nothing yet
    #[test]
    fn missing() {
        let (root, other) = (address(1), address(2));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[]);
        let later = source.pend(root, TrackNumber(1));
        source.reserve(other);
        let first = source.pend(other, FIRST_TRACK);

        let root_reach = reached(root, &source);
        let other_reach = reached(other, &source);

        assert_eq!(root_reach.missing.iter().map(|track| track.address).collect::<Vec<_>>(), vec![later]);
        assert!(!root_reach.is_complete());
        assert_eq!(other_reach.missing.iter().map(|track| track.address).collect::<Vec<_>>(), vec![first]);
        assert!(other_reach.racks.is_empty());
    }

    // a track that does not parse names nothing, and the rack still counts
    #[test]
    fn skipped() {
        let (root, a) = (address(1), address(2));
        let mut source = MemorySource::default();
        source.reserve(root);
        source.reserve(a);
        source.write(root, FIRST_TRACK, b"tapedrive-rack\ntape nope\n");
        source.write(root, TrackNumber(1), b"garbage");
        source.write(root, TrackNumber(2), &render(&records(&[a])));
        source.write_opaque(root, TrackNumber(3));

        let reach = reached(root, &source);

        assert_eq!(reach.racks, tapes(&[root]));
        assert_eq!(reach.tapes, tapes(&[root, a]));
        assert_eq!(reach.skipped.len(), 2);
    }
}
