//! Stores for the challenge tests, filled through the node's own apply path

use tape_api::program::tapedrive::TrackAddress;
use tape_core::encoding::{ErasureCode, TAPENET_PROFILE};
use tape_core::erasure::{slice_root, SLICE_TREE_HEIGHT};
use tape_core::snapshot::replay::{ReplayTrack, ReplayableEvent};
use tape_core::spooler::VirtualGroup;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
use tape_core::types::coin::TAPE;
use tape_core::types::{
    EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
};
use tape_crypto::hash::hash;
use tape_crypto::merkle::root_from_leaf_hashes;
use tape_crypto::{Address, Hash};
use tape_slicer::{num_stripes, ErasureCoder, SliceMetadata, Slicer};
use tape_store::ops::{ObjectInfoOps, SliceOps, TapeOps, TrackDataOps};
use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};
use tape_store::TapeStore;
use tape_store_memory::MemoryStore;

use crate::features::store::apply::{apply_event, DeletePolicy};

/// The chain's current epoch in these tests
pub const EPOCH: EpochNumber = EpochNumber(10);

/// The epoch before it
pub const PREVIOUS: EpochNumber = EpochNumber(9);

/// Tracks of the set were written and certified in this epoch
pub const WRITTEN: EpochNumber = EpochNumber(8);

pub const PROFILE: ErasureCode = TAPENET_PROFILE;

/// Virtual groups the fixture tracks are spread over
pub const VIRTUAL_GROUPS: u64 = 4;

const SLOT: SlotNumber = SlotNumber(1);
const LIVE_UNTIL: EpochNumber = EpochNumber(20);
const CODED_BYTES: usize = 40_000;
const PAYLOAD_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// One track of a fixture store
#[derive(Clone, Debug)]
pub struct Case {
    pub name: &'static str,
    pub address: Address,
    pub track: CompressedTrack,
    pub is_in_set: bool,
    pub footprint: StorageUnits,
}

/// A coded track with the slices its encoding commits to
#[derive(Clone, Debug)]
pub struct Coded {
    pub track: CompressedTrack,
    pub blob: BlobEncoding,
    pub slices: Vec<Vec<u8>>,
}

pub fn test_store() -> TapeStore<MemoryStore> {
    TapeStore::new(MemoryStore::new())
}

/// Apply one chain event the way a storage node's ingest does
pub fn apply(store: &TapeStore<MemoryStore>, event: ReplayableEvent) {
    apply_event(store, SLOT, None, &event, PROFILE, DeletePolicy::NextEpoch).expect("event applies");
}

pub fn tape_at(seed: u8) -> Address {
    Address::from([seed; 32])
}

pub fn tape_number(seed: u8) -> TapeNumber {
    TapeNumber(u64::from(seed))
}

/// Store the row of the tape at `seed`
pub fn put_tape(
    store: &TapeStore<MemoryStore>,
    seed: u8,
    end_epoch: EpochNumber,
    tree_start: TrackNumber,
) -> Address {
    let row = TapeInfo {
        id: tape_number(seed),
        flags: 0,
        end_epoch,
        next_track_number: tree_start,
        tree_start,
        authority: Address::default(),
        delegate: Address::default(),
        nonce: Hash::default(),
    };
    store.put_tape(tape_at(seed), row).expect("tape stored");

    tape_at(seed)
}

/// Bytes that differ from one track to the next and never repeat, so no two
/// slices of a track come out the same
pub fn payload(seed: u8, number: u64, len: usize) -> Vec<u8> {
    let mut state = PAYLOAD_SEED ^ (u64::from(seed) << 32) ^ number;
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push((state >> 24) as u8);
    }
    bytes
}

fn track_row(seed: u8, number: u64, group: VirtualGroup, state: TrackState) -> CompressedTrack {
    CompressedTrack {
        tape: tape_at(seed),
        tape_number: tape_number(seed),
        track_number: TrackNumber(number),
        key: hash(&[seed, number as u8]),
        kind: TrackKind::Inline as u64,
        state: state as u64,
        flags: TrackFlags::NONE,
        size: StorageUnits::zero(),
        group,
        value_hash: Hash::default(),
    }
}

/// Write an inline track on the tape at `seed`, with its payload
pub fn write_inline(
    store: &TapeStore<MemoryStore>,
    seed: u8,
    number: u64,
    group: VirtualGroup,
    state: TrackState,
    epoch: EpochNumber,
) -> Case {
    let bytes = payload(seed, number, 100 + number as usize);
    let size = StorageUnits::from_bytes(bytes.len() as u64);
    let track = CompressedTrack {
        size,
        value_hash: hash(&bytes),
        ..track_row(seed, number, group, state)
    };
    let address = track.address();

    let replay = ReplayTrack { state: track, epoch, blob: None, object: None };
    apply(store, ReplayableEvent::Track(replay));
    store
        .put_track_data(address, BlobData::Inline(bytes))
        .expect("payload stored");

    Case { name: "", address, track, is_in_set: false, footprint: size }
}

/// Encode a payload the way a writer on `profile` does
pub fn coded(
    profile: ErasureCode,
    seed: u8,
    number: u64,
    group: VirtualGroup,
    bytes: &[u8],
) -> Coded {
    let mut slicer = Slicer::try_from_profile(profile).expect("valid profile");
    let slices = slicer.encode(bytes).expect("payload encodes");
    let stripe_size = SliceMetadata::parse(&slices[0])
        .expect("slice metadata")
        .stripe_size();

    let leaves = core::array::from_fn(|position| match slices.get(position) {
        Some(slice) => slice_root(slice).expect("slice within capacity"),
        None => Hash::default(),
    });
    let blob = BlobEncoding {
        size: StorageUnits::from_bytes(bytes.len() as u64),
        commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves[..slices.len()]),
        stripe_size: StorageUnits::from_bytes(stripe_size as u64),
        stripe_count: StripeCount(num_stripes(bytes.len(), stripe_size) as u64),
        leaves,
    };
    let track = CompressedTrack {
        kind: TrackKind::Coded as u64,
        size: blob.size,
        value_hash: blob.get_hash(),
        ..track_row(seed, number, group, TrackState::Certified)
    };

    Coded { track, blob, slices }
}

/// Write a coded track with every slice of it
pub fn write_coded(store: &TapeStore<MemoryStore>, coded: &Coded, epoch: EpochNumber) -> Case {
    let address = coded.track.address();

    let replay = ReplayTrack { state: coded.track, epoch, blob: Some(coded.blob), object: None };
    apply(store, ReplayableEvent::Track(replay));
    for (position, slice) in coded.slices.iter().enumerate() {
        store
            .put_slice(coded.track.group.spool_at(position), address, slice.clone())
            .expect("slice stored");
    }

    Case {
        name: "",
        address,
        track: coded.track,
        is_in_set: false,
        footprint: StorageUnits::from_bytes(coded.slices[0].len() as u64),
    }
}

fn certified(store: &TapeStore<MemoryStore>, seed: u8, number: u64) -> Case {
    let group = VirtualGroup(u64::from(seed) % VIRTUAL_GROUPS);
    write_inline(store, seed, number, group, TrackState::Certified, WRITTEN)
}

fn named(name: &'static str, is_in_set: bool, case: Case) -> Case {
    Case { name, is_in_set, ..case }
}

/// Fill a store with one track for every way a track sits in or out of the set
pub fn every_case(store: &TapeStore<MemoryStore>) -> Vec<Case> {
    let mut cases = Vec::new();

    put_tape(store, 1, LIVE_UNTIL, TrackNumber(0));
    cases.push(named("inline", true, certified(store, 1, 0)));

    put_tape(store, 2, LIVE_UNTIL, TrackNumber(0));
    let blob = coded(PROFILE, 2, 0, VirtualGroup(2), &payload(2, 0, CODED_BYTES));
    cases.push(named("coded", true, write_coded(store, &blob, WRITTEN)));

    put_tape(store, 3, LIVE_UNTIL, TrackNumber(0));
    let system = certified(store, 3, 0);
    let info = ObjectInfo::System {
        kind: SystemObjectKind::Blacklist,
        track_address: system.address,
        registered_epoch: WRITTEN,
        certified_epoch: Some(WRITTEN),
        slot: SLOT,
    };
    store.put_object_info(system.address, info).expect("object info stored");
    cases.push(named("system", false, system));

    put_tape(store, 4, LIVE_UNTIL, TrackNumber(0));
    let registered = write_inline(store, 4, 0, VirtualGroup(0), TrackState::Registered, WRITTEN);
    cases.push(named("registered only", false, registered));

    put_tape(store, 5, LIVE_UNTIL, TrackNumber(0));
    let late = write_inline(store, 5, 0, VirtualGroup(1), TrackState::Registered, PREVIOUS);
    apply(store, ReplayableEvent::CertifyTrack { track: late.address, epoch: EPOCH });
    cases.push(named("certified this epoch", false, late));

    put_tape(store, 6, LIVE_UNTIL, TrackNumber(0));
    let new = write_inline(store, 6, 0, VirtualGroup(2), TrackState::Certified, EPOCH);
    cases.push(named("written this epoch", false, new));

    put_tape(store, 7, LIVE_UNTIL, TrackNumber(0));
    let deleted = certified(store, 7, 0);
    apply(store, ReplayableEvent::DeleteTrack { track: deleted.address, epoch: PREVIOUS });
    cases.push(named("deleted last epoch", false, deleted));

    put_tape(store, 8, LIVE_UNTIL, TrackNumber(0));
    let deleted = certified(store, 8, 0);
    apply(store, ReplayableEvent::DeleteTrack { track: deleted.address, epoch: EPOCH });
    cases.push(named("deleted this epoch", true, deleted));

    put_tape(store, 9, LIVE_UNTIL, TrackNumber(0));
    cases.push(named("closed last epoch", false, certified(store, 9, 0)));
    apply(store, ReplayableEvent::CloseTape { tape: tape_at(9), id: tape_number(9), epoch: PREVIOUS });

    put_tape(store, 10, LIVE_UNTIL, TrackNumber(0));
    cases.push(named("closed this epoch", true, certified(store, 10, 0)));
    apply(store, ReplayableEvent::CloseTape { tape: tape_at(10), id: tape_number(10), epoch: EPOCH });

    // An earlier rewind moved this tape's start to 1, and this epoch's moves it to 2.
    put_tape(store, 11, LIVE_UNTIL, TrackNumber(1));
    cases.push(named("rewound before", false, certified(store, 11, 0)));
    cases.push(named("rewound this epoch", true, certified(store, 11, 1)));
    apply(store, rewind(11, 2));

    put_tape(store, 12, PREVIOUS, TrackNumber(0));
    cases.push(named("renewed", false, certified(store, 12, 0)));
    apply(store, renew(12, 1));

    put_tape(store, 13, EPOCH, TrackNumber(0));
    cases.push(named("expired tape", false, certified(store, 13, 0)));

    put_tape(store, 14, EPOCH.next(), TrackNumber(0));
    cases.push(named("tape in its last epoch", true, certified(store, 14, 0)));

    put_tape(store, 15, LIVE_UNTIL, TrackNumber(0));
    let blacklisted = certified(store, 15, 0);
    store
        .put_object_info(blacklisted.address, ObjectInfo::Blacklisted)
        .expect("object info stored");
    cases.push(named("blacklisted", false, blacklisted));

    put_tape(store, 16, LIVE_UNTIL, TrackNumber(0));
    let invalidated = certified(store, 16, 0);
    apply(store, ReplayableEvent::InvalidateTrack { track: invalidated.address, epoch: PREVIOUS });
    cases.push(named("invalidated", false, invalidated));

    cases
}

/// Add `count` tracks of the set on one live tape, spread over the virtual groups
pub fn fill(store: &TapeStore<MemoryStore>, seed: u8, count: u64) -> Vec<Case> {
    put_tape(store, seed, LIVE_UNTIL, TrackNumber(0));

    let mut cases = Vec::new();
    for number in 0..count {
        let group = VirtualGroup(number % VIRTUAL_GROUPS);
        let case = write_inline(store, seed, number, group, TrackState::Certified, WRITTEN);
        cases.push(named("filler", true, case));
    }
    cases
}

/// A rewind of the tape at `seed` in the current epoch
pub fn rewind(seed: u8, tree_start: u64) -> ReplayableEvent {
    ReplayableEvent::RewindTape {
        tape: tape_at(seed),
        id: tape_number(seed),
        epoch: EPOCH,
        tree_start: TrackNumber(tree_start),
        nonce: Hash::default(),
    }
}

/// A renew of the expired tape at `seed`
pub fn renew(seed: u8, tree_start: u64) -> ReplayableEvent {
    ReplayableEvent::RenewTape {
        tape: tape_at(seed),
        id: tape_number(seed),
        capacity: StorageUnits::zero(),
        active_epoch: EPOCH,
        expiry_epoch: LIVE_UNTIL,
        cost: TAPE(0),
        burned: TAPE(0),
        scheduled: TAPE(0),
        tree_start: TrackNumber(tree_start),
        nonce: Hash::default(),
    }
}

#[cfg(test)]
mod tests {
    use tape_core::spooler::GroupFold;

    use crate::features::assignment::size::{group_weights, GroupWeights};

    use super::*;

    // the sizing counts the tracks the fixture marks as in the set, at their footprints
    #[test]
    fn sizing_weights() {
        let store = test_store();
        let cases = every_case(&store);
        let fold = GroupFold::new(2).expect("live groups");

        let mut expected = vec![StorageUnits::zero(); 2];
        let mut counted = Vec::new();
        for case in &cases {
            if case.is_in_set {
                let group = fold.live_group(case.track.group).as_usize();
                expected[group] = expected[group] + case.footprint;
                counted.push(case.address);
            }
        }
        counted.sort();

        let weights = match group_weights(&store, EPOCH, EPOCH.next(), fold, PROFILE)
            .expect("weights build")
        {
            GroupWeights::Ready(weights) => Some(weights),
            GroupWeights::Pending { .. } => None,
        };
        let weights = weights.expect("weights ready");
        let mut tracks: Vec<Address> = weights
            .tracks
            .iter()
            .map(|active| active.track)
            .collect();
        tracks.sort();

        assert_eq!(tracks, counted);
        assert_eq!(weights.sizes, expected);
        assert!(expected[0] > StorageUnits::zero() && expected[1] > StorageUnits::zero());
    }
}
