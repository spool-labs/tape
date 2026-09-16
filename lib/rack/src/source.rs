//! What a reader knows about tapes and their tracks

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;

use tape_core::types::TrackNumber;
use tape_crypto::address::Address;

/// One track as a reader holds it
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Track {
    /// Never written, or deleted or invalidated
    Absent,
    /// Present but not an inline write, so it names nothing
    Opaque,
    /// Present, with bytes the reader does not hold yet
    Pending { address: Address },
    /// Present, with its bytes
    Written { address: Address, bytes: Vec<u8> },
}

/// A reader's view of tapes and their tracks, from its store or index
pub trait TapeSource {
    type Error;

    /// Whether a tape is reserved and not expired at the reader's epoch
    fn is_live(&self, tape: Address) -> Result<bool, Self::Error>;

    /// One track of a tape
    fn track(&self, tape: Address, track_number: TrackNumber) -> Result<Track, Self::Error>;

    /// The numbers of a tape's present tracks, in any order
    fn track_numbers(&self, tape: Address) -> Result<Vec<TrackNumber>, Self::Error>;
}

/// Tapes and tracks held in memory, for tests and small tools
#[derive(Clone, Debug, Default)]
pub struct MemorySource {
    live: HashSet<Address>,
    tapes: HashMap<Address, BTreeMap<TrackNumber, Track>>,
}

impl MemorySource {
    pub fn reserve(&mut self, tape: Address) {
        self.live.insert(tape);
    }

    pub fn expire(&mut self, tape: Address) {
        self.live.remove(&tape);
    }

    /// Write a track with its bytes, returning its address
    pub fn write(&mut self, tape: Address, track_number: TrackNumber, bytes: &[u8]) -> Address {
        let address = self.address(tape, track_number);
        self.put(tape, track_number, Track::Written { address, bytes: bytes.to_vec() });
        address
    }

    /// Write a track whose bytes the reader does not hold, returning its address
    pub fn pend(&mut self, tape: Address, track_number: TrackNumber) -> Address {
        let address = self.address(tape, track_number);
        self.put(tape, track_number, Track::Pending { address });
        address
    }

    /// Write a track that is not an inline write
    pub fn write_opaque(&mut self, tape: Address, track_number: TrackNumber) {
        self.put(tape, track_number, Track::Opaque);
    }

    /// Delete or invalidate a track, returning the address it had
    pub fn remove(&mut self, tape: Address, track_number: TrackNumber) -> Option<Address> {
        let address = match self.tapes.get(&tape).and_then(|tracks| tracks.get(&track_number)) {
            Some(Track::Pending { address } | Track::Written { address, .. }) => Some(*address),
            Some(Track::Absent | Track::Opaque) | None => None,
        };
        self.put(tape, track_number, Track::Absent);
        address
    }

    fn address(&self, tape: Address, track_number: TrackNumber) -> Address {
        match self.tapes.get(&tape).and_then(|tracks| tracks.get(&track_number)) {
            Some(Track::Pending { address } | Track::Written { address, .. }) => *address,
            Some(Track::Absent | Track::Opaque) | None => Address::new_unique(),
        }
    }

    fn put(&mut self, tape: Address, track_number: TrackNumber, track: Track) {
        self.tapes.entry(tape).or_default().insert(track_number, track);
    }
}

impl TapeSource for MemorySource {
    type Error = Infallible;

    fn is_live(&self, tape: Address) -> Result<bool, Self::Error> {
        Ok(self.live.contains(&tape))
    }

    fn track(&self, tape: Address, track_number: TrackNumber) -> Result<Track, Self::Error> {
        let track = self
            .tapes
            .get(&tape)
            .and_then(|tracks| tracks.get(&track_number))
            .cloned()
            .unwrap_or(Track::Absent);
        Ok(track)
    }

    fn track_numbers(&self, tape: Address) -> Result<Vec<TrackNumber>, Self::Error> {
        let mut numbers = Vec::new();
        if let Some(tracks) = self.tapes.get(&tape) {
            for (number, track) in tracks {
                if *track != Track::Absent {
                    numbers.push(*number);
                }
            }
        }
        Ok(numbers)
    }
}
