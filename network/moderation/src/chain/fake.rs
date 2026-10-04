//! An in-memory chain for tests, with faults that lose, refuse or double writes and deletes

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Mutex, MutexGuard};

use tape_api::program::tapedrive::TrackAddress;
use tape_blacklist::list::List;
use tape_core::spooler::GroupIndex;
use tape_core::system::BlacklistEntry;
use tape_core::track::data::TrackMeta;
use tape_core::track::types::{CompressedTrack, TrackFlags};
use tape_core::types::coin::{SOL, TAPE};
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_rack::source::MemorySource;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::stream::manifest::MAX_TRACKS_PER_TAPE;

use crate::chain::error::ChainError;
use crate::chain::tape_chain::{PayerBalances, TapeChain, TapeStatus};
use crate::publisher::writer::{inline_key, TrackWriter, WrittenTrack};
use crate::records::rows::ListTape;
use crate::types::ids::TrackCount;

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

/// What a write does
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFault {
    /// Lands and reports where
    Lands,
    /// Lands, then reports a failure, as when the service stops before recording it
    Lost,
    /// Fails without landing
    Refused,
    /// Lands twice and reports the second, as when a resent transaction lands beside the first
    Doubled,
}

/// What a delete does
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteFault {
    /// Lands and reports it
    Lands,
    /// Lands, then reports a failure
    Lost,
    /// Fails without landing
    Refused,
}

/// A write or delete that landed
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Landed {
    Write { tape: Address, track_number: TrackNumber },
    Delete { tape: Address, track_number: TrackNumber },
}

/// A tape as the chain holds it
#[derive(Clone, Debug)]
pub struct FakeTape {
    pub capacity: StorageUnits,
    pub used: StorageUnits,
    pub expiry_epoch: EpochNumber,
    pub next_track: TrackNumber,
    /// Present tracks and their bytes
    pub tracks: BTreeMap<TrackNumber, Vec<u8>>,
}

/// Everything the fake chain holds
pub struct FakeState {
    pub epoch: EpochNumber,
    pub balances: PayerBalances,
    pub tapes: HashMap<Address, FakeTape>,
    pub reserved: Vec<Address>,
    pub expiry_extensions: Vec<Address>,
    pub capacity_extensions: Vec<Address>,
    /// Writes and deletes in the order they landed
    pub landed: Vec<Landed>,
    /// Faults for the next writes in order; every write past them lands
    pub write_faults: VecDeque<WriteFault>,
    pub delete_faults: VecDeque<DeleteFault>,
    pub is_extension_failing: bool,
}

/// A chain in memory
pub struct FakeChain {
    state: Mutex<FakeState>,
}

impl FakeTape {
    pub fn new(capacity: StorageUnits, expiry_epoch: EpochNumber) -> Self {
        Self {
            capacity,
            used: StorageUnits(0),
            expiry_epoch,
            next_track: TrackNumber(0),
            tracks: BTreeMap::new(),
        }
    }
}

impl FakeChain {
    pub fn new(epoch: EpochNumber) -> Self {
        Self {
            state: Mutex::new(FakeState {
                epoch,
                balances: PayerBalances {
                    sol: SOL(10 * LAMPORTS_PER_SOL),
                    tape: TAPE(1_000 * TAPE::SCALE),
                },
                tapes: HashMap::new(),
                reserved: Vec::new(),
                expiry_extensions: Vec::new(),
                capacity_extensions: Vec::new(),
                landed: Vec::new(),
                write_faults: VecDeque::new(),
                delete_faults: VecDeque::new(),
                is_extension_failing: false,
            }),
        }
    }

    /// The chain's state; never held across an await
    pub fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().expect("fake chain lock")
    }

    /// A tape's present tracks as text, lowest number first
    pub fn tracks(&self, tape: Address) -> Vec<(TrackNumber, String)> {
        let state = self.state();
        let mut tracks = Vec::new();
        if let Some(fake) = state.tapes.get(&tape) {
            for (number, bytes) in &fake.tracks {
                tracks.push((*number, String::from_utf8_lossy(bytes).into_owned()));
            }
        }
        tracks
    }

    /// What a consumer following `rack` denies at the chain's epoch
    pub fn denied(&self, rack: Address) -> HashSet<BlacklistEntry> {
        self.list(rack).denied().copied().collect()
    }

    /// The tapes a consumer following `rack` reaches at the chain's epoch
    pub fn reached(&self, rack: Address) -> HashSet<Address> {
        self.list(rack).reached().clone()
    }

    fn list(&self, rack: Address) -> List {
        let source = self.source();
        let mut list = List::new(rack);
        match list.rebuild(&source) {
            Ok(_) => list,
            Err(never) => match never {},
        }
    }

    fn source(&self) -> MemorySource {
        let state = self.state();
        let mut source = MemorySource::default();
        for (address, fake) in &state.tapes {
            if fake.expiry_epoch > state.epoch {
                source.reserve(*address);
            }
            for (number, bytes) in &fake.tracks {
                source.write(*address, *number, bytes);
            }
        }
        source
    }
}

impl TapeChain for FakeChain {
    async fn tape_status(&self, tape: Address) -> Result<Option<TapeStatus>, ChainError> {
        let state = self.state();
        let Some(fake) = state.tapes.get(&tape) else {
            return Ok(None);
        };
        Ok(Some(TapeStatus {
            capacity: fake.capacity,
            used: fake.used,
            expiry_epoch: fake.expiry_epoch,
            next_track: fake.next_track,
            present_tracks: TrackCount(fake.tracks.len() as u64),
        }))
    }

    async fn current_epoch(&self) -> Result<EpochNumber, ChainError> {
        Ok(self.state().epoch)
    }

    async fn payer_balances(&self) -> Result<PayerBalances, ChainError> {
        Ok(self.state().balances)
    }

    async fn extend_expiry(&self, tape: Address, epochs: EpochNumber) -> Result<(), ChainError> {
        let mut state = self.state();
        if state.is_extension_failing {
            return Err(ChainError::Sdk("the payer cannot pay for the extension".to_string()));
        }
        state.expiry_extensions.push(tape);
        let fake = state.tapes.get_mut(&tape).expect("tape exists");
        fake.expiry_epoch = fake.expiry_epoch + epochs;
        Ok(())
    }

    async fn extend_capacity(&self, tape: Address, extra: StorageUnits) -> Result<(), ChainError> {
        let mut state = self.state();
        state.capacity_extensions.push(tape);
        let fake = state.tapes.get_mut(&tape).expect("tape exists");
        fake.capacity = fake.capacity + extra;
        Ok(())
    }

    async fn reserve(&self, key: &TapeKey, capacity: StorageUnits, epochs: EpochNumber) -> Result<(), ChainError> {
        let mut state = self.state();
        let expiry_epoch = state.epoch + epochs;
        state.reserved.push(key.address());
        state.tapes.insert(key.address(), FakeTape::new(capacity, expiry_epoch));
        Ok(())
    }
}

impl TrackWriter for FakeChain {
    async fn write_inline(&self, tape: &ListTape, bytes: &[u8]) -> Result<WrittenTrack, ChainError> {
        let mut state = self.state();
        let fault = state.write_faults.pop_front().unwrap_or(WriteFault::Lands);
        if fault == WriteFault::Refused {
            return Err(ChainError::Sdk("the write was refused".to_string()));
        }
        let first = land_write(&mut state, tape.address, bytes)?;
        match fault {
            WriteFault::Lands | WriteFault::Refused => Ok(first),
            WriteFault::Lost => Err(ChainError::Sdk("connection lost after the write landed".to_string())),
            WriteFault::Doubled => land_write(&mut state, tape.address, bytes),
        }
    }

    async fn delete_track(
        &self,
        tape: &ListTape,
        track: Address,
        track_number: TrackNumber,
    ) -> Result<(), ChainError> {
        let mut state = self.state();
        let fault = state.delete_faults.pop_front().unwrap_or(DeleteFault::Lands);
        if fault == DeleteFault::Refused {
            return Err(ChainError::Sdk("the delete was refused".to_string()));
        }
        let fake = state.tapes.get_mut(&tape.address).ok_or(ChainError::TrackNotFound(track))?;
        let is_held = fake
            .tracks
            .get(&track_number)
            .is_some_and(|bytes| inline_track(tape.address, track_number, bytes).address() == track);
        if !is_held {
            return Err(ChainError::TrackNotFound(track));
        }
        fake.tracks.remove(&track_number);
        state.landed.push(Landed::Delete {
            tape: tape.address,
            track_number,
        });
        match fault {
            DeleteFault::Lands | DeleteFault::Refused => Ok(()),
            DeleteFault::Lost => Err(ChainError::Sdk("connection lost after the delete landed".to_string())),
        }
    }

    async fn landed_track(
        &self,
        tape: Address,
        track_number: TrackNumber,
    ) -> Result<Option<CompressedTrack>, ChainError> {
        let state = self.state();
        let bytes = state
            .tapes
            .get(&tape)
            .and_then(|fake| fake.tracks.get(&track_number));
        Ok(bytes.map(|bytes| inline_track(tape, track_number, bytes)))
    }
}

/// The leaf the chain holds for an unnamed inline track of these bytes
fn inline_track(tape: Address, track_number: TrackNumber, bytes: &[u8]) -> CompressedTrack {
    let meta = TrackMeta::inline(bytes);
    CompressedTrack {
        tape,
        track_number,
        key: inline_key(bytes),
        kind: meta.kind as u64,
        state: meta.state as u64,
        flags: TrackFlags::NONE,
        size: meta.size,
        group: GroupIndex(0),
        value_hash: meta.value_hash,
    }
}

fn land_write(state: &mut FakeState, tape: Address, bytes: &[u8]) -> Result<WrittenTrack, ChainError> {
    let epoch = state.epoch;
    let fake = state
        .tapes
        .get_mut(&tape)
        .ok_or_else(|| ChainError::Sdk(format!("tape {tape} does not exist")))?;
    if fake.expiry_epoch <= epoch {
        return Err(ChainError::Sdk(format!("tape {tape} expired")));
    }
    if fake.next_track.0 >= MAX_TRACKS_PER_TAPE {
        return Err(ChainError::Sdk(format!("tape {tape} is full")));
    }
    let track_number = fake.next_track;
    fake.next_track = TrackNumber(track_number.0 + 1);
    fake.used = fake.used + StorageUnits(bytes.len() as u64);
    fake.tracks.insert(track_number, bytes.to_vec());
    state.landed.push(Landed::Write { tape, track_number });
    Ok(WrittenTrack {
        track: inline_track(tape, track_number, bytes).address(),
        track_number,
    })
}
