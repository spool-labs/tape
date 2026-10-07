//! In-memory pending-track state from confirmed-but-not-yet-finalized slots.
//!
//! The block ingestor appends events here as soon as a confirmed block enters
//! the pending queue, so SDK reads, peer queries, `put_slice`, and `certify`
//! see new tracks at confirmed latency rather than waiting for finalization.
//!
//! Only `Reserve`, `Register`, `Certify` and a tape's `Close`, `Rewind` and
//! `Renew` flow through pending state. `Invalidate` and `Delete` are
//! destructive and remain on the finalized path — exposing them from confirmed
//! state risks showing a track as gone when the block that removed it gets
//! reorged out.
//!
//! Reservations are here because a track without its tape is not servable: a
//! proof needs the tape's track set, so a fresh tape's first track would be
//! not-found on every peer for a whole finality window while the overlay
//! resolved the track and the tape read fell through to disk.
//!
//! Close, rewind and renew are here for the same reason. A track written after
//! one of them sits in the tape's new tree, and its proof needs the tape number
//! and tree start the tape has now. Reads by tape stop listing the earlier
//! tracks at confirmed latency, and nothing is deleted until the block is
//! finalized: if it is reorged out, `drop_slot` drops the change and the tracks
//! list again.
//!
//! Read paths fold pending events on top of the disk-backed `TapeStore` via
//! `apply_to_track`. Once a slot is finalized (and the corresponding events
//! have been applied to disk by `StoreManager`) or rolled back, `drop_slot`
//! removes the events for that slot.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use tape_blocks::ParsedInstruction;
use tape_core::encoding::ErasureCode;
use tape_core::object::object_etag;
use tape_core::snapshot::replay::ReplayTrackObject;
use tape_core::track::data::BlobData;
use tape_core::track::types::{CompressedTrack, TrackState};
use tape_core::types::{ContentType, EpochNumber, SlotNumber, TapeNumber, TrackNumber};
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_store::types::TapeInfo;

use crate::features::block::ingestor::ParsedBlock;

#[derive(Debug)]
enum EventKind {
    Register {
        state: CompressedTrack,
        data: Box<BlobData>,
        object: Option<ReplayTrackObject>,
        block_time: Option<i64>,
    },
    Certify,
}

#[derive(Debug)]
struct Event {
    slot: SlotNumber,
    kind: EventKind,
}

/// A change to a tape seen at confirmed commitment
#[derive(Debug)]
enum TapeChange {
    /// The tape's whole row, from a reservation or from an update to a pending one
    Row(TapeInfo),
    Close {
        id: TapeNumber,
    },
    Rewind {
        id: TapeNumber,
        tree_start: TrackNumber,
        nonce: Hash,
    },
    Renew {
        id: TapeNumber,
        end_epoch: EpochNumber,
        tree_start: TrackNumber,
        nonce: Hash,
    },
}

#[derive(Debug)]
struct TapeEvent {
    slot: SlotNumber,
    change: TapeChange,
}

/// A certified name-addressed object visible in confirmed pending state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNamedObject {
    pub track_address: Address,
    pub track_number: TrackNumber,
    pub size: u64,
    pub etag: Hash,
    pub block_time: Option<i64>,
    pub content_type: ContentType,
}

#[derive(Debug, Default)]
pub struct PendingTracks {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {

    /// Per-track event log, in append order (which is slot-monotonic because
    /// the ingestor only appends after chain validation).
    events_by_track: HashMap<Address, Vec<Event>>,

    /// Reverse index used to bulk-drop events when a slot is rolled back or
    /// promoted.
    addresses_by_slot: BTreeMap<SlotNumber, Vec<Address>>,

    /// Name lookup index over pending registrations, ordered by tape track
    /// number so replacement reads do not scan every pending track.
    objects_by_tape: HashMap<Address, HashMap<Vec<u8>, BTreeMap<u64, Address>>>,

    /// Reverse index for removing name registrations on promotion or rollback.
    objects_by_slot: BTreeMap<SlotNumber, Vec<(Address, Vec<u8>, u64)>>,

    /// Per-tape log of pending changes, in append order.
    tape_changes: HashMap<Address, Vec<TapeEvent>>,

    /// Reverse index for dropping a slot's tape changes.
    tapes_by_slot: BTreeMap<SlotNumber, Vec<Address>>,
}

impl PendingTracks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_register(
        &self,
        slot: SlotNumber,
        track: Address,
        state: CompressedTrack,
        data: BlobData,
    ) {
        self.apply_register_with_object(slot, track, state, data, None, None);
    }

    /// Record a registration together with its optional named-object metadata.
    pub fn apply_register_with_object(
        &self,
        slot: SlotNumber,
        track: Address,
        state: CompressedTrack,
        data: BlobData,
        object: Option<ReplayTrackObject>,
        block_time: Option<i64>,
    ) {
        self.append(
            track,
            Event {
                slot,
                kind: EventKind::Register {
                    state,
                    data: Box::new(data),
                    object,
                    block_time,
                },
            },
        );
    }

    /// Record a tape reservation seen at confirmed commitment.
    pub fn apply_reserve(&self, slot: SlotNumber, tape: Address, info: TapeInfo) {
        self.apply_tape_change(slot, tape, TapeChange::Row(info));
    }

    /// Record a tape closed at confirmed commitment.
    pub fn apply_close(&self, slot: SlotNumber, tape: Address, id: TapeNumber) {
        self.apply_tape_change(slot, tape, TapeChange::Close { id });
    }

    /// Record a tape rewound at confirmed commitment.
    pub fn apply_rewind(
        &self,
        slot: SlotNumber,
        tape: Address,
        id: TapeNumber,
        tree_start: TrackNumber,
        nonce: Hash,
    ) {
        self.apply_tape_change(slot, tape, TapeChange::Rewind { id, tree_start, nonce });
    }

    /// Record an expired tape renewed at confirmed commitment.
    pub fn apply_renew(
        &self,
        slot: SlotNumber,
        tape: Address,
        id: TapeNumber,
        end_epoch: EpochNumber,
        tree_start: TrackNumber,
        nonce: Hash,
    ) {
        let change = TapeChange::Renew { id, end_epoch, tree_start, nonce };
        self.apply_tape_change(slot, tape, change);
    }

    fn apply_tape_change(&self, slot: SlotNumber, tape: Address, change: TapeChange) {
        let mut inner = self.inner.write().expect("pending-tracks lock poisoned");
        inner
            .tape_changes
            .entry(tape)
            .or_default()
            .push(TapeEvent { slot, change });
        inner.tapes_by_slot.entry(slot).or_default().push(tape);
    }

    /// Push an updated snapshot of a pending tape so last-wins reads see the change
    fn apply_tape_update(
        &self,
        slot: SlotNumber,
        tape: Address,
        update: impl FnOnce(&mut TapeInfo),
    ) {
        // Only a tape whose reservation is still pending has a row here.
        let Some(mut info) = self.apply_to_tape(tape, None) else {
            return;
        };
        update(&mut info);
        self.apply_tape_change(slot, tape, TapeChange::Row(info));
    }

    pub fn apply_certify(&self, slot: SlotNumber, track: Address) {
        self.append(
            track,
            Event {
                slot,
                kind: EventKind::Certify,
            },
        );
    }

    /// Apply every track-relevant instruction in `block` to pending state.
    /// Mirrors the registration and certification subset of
    /// `features/store/apply.rs`. Invalidate and Delete are intentionally
    /// not applied here — those are destructive and remain on the finalized
    /// path. The network's profile sets how wide a coded commitment is.
    pub fn apply_block(&self, block: &ParsedBlock, profile: ErasureCode) {
        for instruction in &block.instructions {
            match instruction {
                ParsedInstruction::TrackWrite {
                    track,
                    key,
                    object,
                    value,
                    event,
                    ..
                } => {
                    let Some(meta) = value.meta(profile) else {
                        continue;
                    };
                    let state = CompressedTrack {
                        tape: event.tape,
                        tape_number: event.tape_number,
                        track_number: event.track_number,
                        key: *key,
                        kind: meta.kind as u64,
                        state: meta.state as u64,
                        flags: event.flags,
                        size: meta.size,
                        group: event.group,
                        value_hash: meta.value_hash,
                    };
                    self.apply_register_with_object(
                        block.slot,
                        *track,
                        state,
                        value.clone(),
                        object.clone(),
                        block.block_time,
                    );
                }
                ParsedInstruction::CertifyTrack { track, .. } => {
                    self.apply_certify(block.slot, *track);
                }
                ParsedInstruction::ReserveTape { tape, event, .. } => {
                    self.apply_reserve(
                        block.slot,
                        *tape,
                        TapeInfo {
                            id: event.id,
                            flags: event.flags,
                            end_epoch: event.expiry_epoch,
                            next_track_number: TrackNumber(0),
                            tree_start: TrackNumber(0),
                            authority: event.authority,
                            delegate: Address::default(),
                            nonce: event.nonce,
                        },
                    );
                }
                ParsedInstruction::SetTapeDelegate { tape, delegate } => {
                    self.apply_tape_update(block.slot, *tape, |info| info.delegate = *delegate);
                }
                ParsedInstruction::AdvanceTapeNonce { tape, event } => {
                    self.apply_tape_update(block.slot, *tape, |info| info.nonce = event.nonce);
                }
                ParsedInstruction::CloseTape { tape, event, .. } => {
                    self.apply_close(block.slot, *tape, event.id);
                }
                ParsedInstruction::RewindTape { tape, event, .. } => {
                    self.apply_rewind(
                        block.slot,
                        *tape,
                        event.id,
                        event.tree_start,
                        event.nonce,
                    );
                }
                ParsedInstruction::RenewTape { tape, event, .. } => {
                    self.apply_renew(
                        block.slot,
                        *tape,
                        event.id,
                        event.expiry_epoch,
                        event.tree_start,
                        event.nonce,
                    );
                }
                _ => {}
            }
        }
    }

    /// Drop every event recorded at `slot`. Called when the queue rolls a
    /// slot back after a chain break, or when the slot has been promoted and
    /// its effects are now on disk.
    pub fn drop_slot(&self, slot: SlotNumber) {
        let mut inner = self.inner.write().expect("pending-tracks lock poisoned");

        if let Some(objects) = inner.objects_by_slot.remove(&slot) {
            for (tape, name, track_number) in objects {
                let mut remove_tape = false;
                if let Some(by_name) = inner.objects_by_tape.get_mut(&tape) {
                    let mut remove_name = false;
                    if let Some(versions) = by_name.get_mut(&name) {
                        versions.remove(&track_number);
                        remove_name = versions.is_empty();
                    }
                    if remove_name {
                        by_name.remove(&name);
                    }
                    remove_tape = by_name.is_empty();
                }
                if remove_tape {
                    inner.objects_by_tape.remove(&tape);
                }
            }
        }

        if let Some(tapes) = inner.tapes_by_slot.remove(&slot) {
            for tape in tapes {
                let drained_empty = match inner.tape_changes.get_mut(&tape) {
                    Some(events) => {
                        events.retain(|event| event.slot != slot);
                        events.is_empty()
                    }
                    None => false,
                };
                if drained_empty {
                    inner.tape_changes.remove(&tape);
                }
            }
        }

        let Some(addresses) = inner.addresses_by_slot.remove(&slot) else {
            return;
        };

        for addr in addresses {
            let drained_empty = match inner.events_by_track.get_mut(&addr) {
                Some(events) => {
                    events.retain(|event| event.slot != slot);
                    events.is_empty()
                }
                None => false,
            };
            if drained_empty {
                inner.events_by_track.remove(&addr);
            }
        }
    }

    /// Fold the pending changes to `tape` on top of the row the store holds.
    ///
    /// A pending reservation is the tape's whole row. The store's row for the
    /// same tape number wins over it, since the store has then applied that
    /// reservation and whatever followed. A close, a rewind and a renew change
    /// the row of the tape number they were made on.
    pub fn apply_to_tape(&self, tape: Address, in_store: Option<TapeInfo>) -> Option<TapeInfo> {
        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        let Some(events) = inner.tape_changes.get(&tape) else {
            return in_store;
        };

        let mut is_stored = in_store.is_some();
        let mut row = in_store;
        for event in events {
            match &event.change {
                TapeChange::Row(info) => {
                    let is_applied = is_stored && row.as_ref().is_some_and(|row| row.id == info.id);
                    if !is_applied {
                        row = Some(info.clone());
                        is_stored = false;
                    }
                }
                TapeChange::Close { id } => {
                    if row.as_ref().is_some_and(|row| row.id == *id) {
                        row = None;
                        is_stored = false;
                    }
                }
                TapeChange::Rewind { id, tree_start, nonce } => {
                    if let Some(row) = row.as_mut().filter(|row| row.id == *id) {
                        row.tree_start = row.tree_start.max(*tree_start);
                        row.next_track_number = row.next_track_number.max(*tree_start);
                        row.nonce = *nonce;
                    }
                }
                TapeChange::Renew { id, end_epoch, tree_start, nonce } => {
                    if let Some(row) = row.as_mut().filter(|row| row.id == *id) {
                        row.end_epoch = *end_epoch;
                        row.tree_start = row.tree_start.max(*tree_start);
                        row.next_track_number = row.next_track_number.max(*tree_start);
                        row.nonce = *nonce;
                    }
                }
            }
        }

        row
    }

    /// Fold pending events for `track` on top of `in_store` (the value
    /// observed in the disk-backed store). Returns the resulting state, or
    /// `None` if the track is unknown to both pending state and the store.
    pub fn apply_to_track(
        &self,
        track: Address,
        in_store: Option<CompressedTrack>,
    ) -> Option<CompressedTrack> {
        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        let Some(events) = inner.events_by_track.get(&track) else {
            return in_store;
        };

        Self::apply_events(events, in_store)
    }

    /// Return the pending-state view of the track's payload, if a `Register`
    /// event for `track` is currently held. Disk state is not consulted —
    /// callers fall back to `store.get_track_data` themselves when this
    /// returns `None`.
    pub fn track_data(&self, track: Address) -> Option<BlobData> {
        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        let events = inner.events_by_track.get(&track)?;
        for event in events.iter().rev() {
            if let EventKind::Register { data, .. } = &event.kind {
                return Some(data.as_ref().clone());
            }
        }
        None
    }

    /// Return pending registered tracks for `tape` after applying any later
    /// pending certify events. Disk-only tracks are intentionally absent;
    /// callers that need a complete tape view merge this over store results.
    pub fn registered_tracks_by_tape(&self, tape: Address) -> Vec<(Address, CompressedTrack)> {
        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        let mut tracks = inner
            .events_by_track
            .iter()
            .filter_map(|(addr, events)| {
                let track = Self::apply_events(events, None)?;
                (track.tape == tape).then_some((*addr, track))
            })
            .collect::<Vec<_>>();

        tracks.sort_by_key(|(_, track)| track.track_number.0);
        tracks
    }

    /// Return the address of the pending registered track at one number on
    /// the tape numbered `tape_number` at `tape`.
    pub fn track_address_by_number(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        track_number: TrackNumber,
    ) -> Option<Address> {
        self.registered_tracks_by_tape(tape)
            .into_iter()
            .find(|(_, track)| {
                track.tape_number == tape_number && track.track_number == track_number
            })
            .map(|(address, _)| address)
    }

    /// Resolve the newest certified pending registration for one object name.
    ///
    /// Registered-but-uncertified coded objects remain hidden. If several
    /// confirmed writes replace the same name, the highest tape track number
    /// wins, matching finalized replay order. Dropping either its register or
    /// certify slot automatically removes it from this derived view.
    pub fn named_object(&self, tape: Address, name: &[u8]) -> Option<PendingNamedObject> {
        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        let versions = inner.objects_by_tape.get(&tape)?.get(name)?;

        for (_, track_address) in versions.iter().rev() {
            let events = inner.events_by_track.get(track_address)?;
            let Some(state) = Self::apply_events(events, None) else {
                continue;
            };
            if state.tape != tape || !state.is_certified() {
                continue;
            }

            let registration = events.iter().rev().find_map(|event| match &event.kind {
                EventKind::Register {
                    data,
                    object: Some(object),
                    block_time,
                    ..
                } if object.name == name => Some((data, object, *block_time)),
                _ => None,
            });
            let Some((data, object, block_time)) = registration else {
                continue;
            };
            let blob = match data.as_ref() {
                BlobData::Coded(blob) => Some(blob),
                BlobData::Inline(_) => None,
            };
            return Some(PendingNamedObject {
                track_address: *track_address,
                track_number: state.track_number,
                size: object.logical_size.to_bytes(),
                etag: object_etag(&state, blob),
                block_time,
                content_type: object.content_type,
            });
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.inner
            .read()
            .expect("pending-tracks lock poisoned")
            .events_by_track
            .is_empty()
    }

    fn append(&self, track: Address, event: Event) {
        let mut inner = self.inner.write().expect("pending-tracks lock poisoned");
        let slot = event.slot;
        if let EventKind::Register {
            state,
            object: Some(object),
            ..
        } = &event.kind
        {
            inner
                .objects_by_tape
                .entry(state.tape)
                .or_default()
                .entry(object.name.clone())
                .or_default()
                .insert(state.track_number.0, track);
            inner.objects_by_slot.entry(slot).or_default().push((
                state.tape,
                object.name.clone(),
                state.track_number.0,
            ));
        }
        inner.events_by_track.entry(track).or_default().push(event);
        let slotted = inner.addresses_by_slot.entry(slot).or_default();
        if !slotted.contains(&track) {
            slotted.push(track);
        }
    }

    fn apply_events(events: &[Event], in_store: Option<CompressedTrack>) -> Option<CompressedTrack> {
        let mut state = in_store;
        for event in events {
            match &event.kind {
                EventKind::Register {
                    state: registered, ..
                } => {
                    state = Some(*registered);
                }
                EventKind::Certify => {
                    state = state.map(|mut s| {
                        s.state = TrackState::Certified as u64;
                        s
                    });
                }
            }
        }
        state
    }
}

#[cfg(test)]
mod tests {
    use tape_core::erasure::MAX_GROUP_SIZE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{TrackFlags, TrackKind};
    use tape_core::types::{StorageUnits, StripeCount, TapeNumber, TrackNumber};
    use tape_crypto::Hash;

    use super::*;

    fn registered_blob(tape: Address) -> CompressedTrack {
        CompressedTrack {
            tape,
            tape_number: TapeNumber(1),
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1024),
            group: VirtualGroup::from(0),
            value_hash: Hash::new_unique(),
        }
    }

    fn sample_blob() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::from_bytes(1024),
            commitment: Hash::default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(1),
            leaves: [Hash::default(); MAX_GROUP_SIZE],
        }
    }

    #[test]
    fn register_visible_via_pending_state() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(
            SlotNumber(10),
            track,
            state,
            BlobData::Coded(sample_blob()),
        );

        let pending_view = pending.apply_to_track(track, None);
        assert_eq!(pending_view, Some(state));
    }

    #[test]
    fn certify_after_register_returns_certified() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(sample_blob()));
        pending.apply_certify(SlotNumber(11), track);

        let pending_view = pending
            .apply_to_track(track, None)
            .expect("track present");
        assert_eq!(pending_view.state, TrackState::Certified as u64);
    }

    #[test]
    fn certify_promotes_disk_state() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let mut in_store = registered_blob(tape);
        in_store.state = TrackState::Registered as u64;

        // Register has already promoted, only the certify event is pending.
        pending.apply_certify(SlotNumber(20), track);

        let pending_view = pending
            .apply_to_track(track, Some(in_store))
            .expect("track present");
        assert_eq!(pending_view.state, TrackState::Certified as u64);
        assert_eq!(pending_view.tape, in_store.tape);
    }

    #[test]
    fn certify_without_disk_or_register_does_nothing() {
        let pending = PendingTracks::new();
        let track = Address::new_unique();

        pending.apply_certify(SlotNumber(20), track);

        assert!(pending.apply_to_track(track, None).is_none());
    }

    #[test]
    fn drop_slot_removes_register() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(sample_blob()));
        pending.drop_slot(SlotNumber(10));

        assert!(pending.apply_to_track(track, None).is_none());
        assert!(pending.is_empty());
    }

    #[test]
    fn drop_slot_keeps_other_slots_for_same_track() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(sample_blob()));
        pending.apply_certify(SlotNumber(11), track);
        pending.drop_slot(SlotNumber(10));

        // Register dropped, but certify remains. With in_store=Some(disk),
        // the certify still upgrades the in-store state to Certified.
        let pending_view = pending
            .apply_to_track(track, Some(state))
            .expect("certify still applied");
        assert_eq!(pending_view.state, TrackState::Certified as u64);
    }

    #[test]
    fn drop_slot_unrelated_is_noop() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(sample_blob()));
        pending.drop_slot(SlotNumber(99));

        assert_eq!(pending.apply_to_track(track, None), Some(state));
    }

    #[test]
    fn track_data_returns_blob_payload() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);
        let blob = sample_blob();

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(blob));

        assert_eq!(
            pending.track_data(track),
            Some(BlobData::Coded(blob))
        );
    }

    #[test]
    fn track_data_exposes_raw_payload() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let mut state = registered_blob(tape);
        state.kind = TrackKind::Inline as u64;
        state.state = TrackState::Certified as u64;
        let bytes = vec![0xAB; 16];

        pending.apply_register(
            SlotNumber(10),
            track,
            state,
            BlobData::Inline(bytes.clone()),
        );

        assert_eq!(
            pending.track_data(track),
            Some(BlobData::Inline(bytes))
        );
    }

    #[test]
    fn track_data_returns_none_after_drop() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(
            SlotNumber(10),
            track,
            state,
            BlobData::Coded(sample_blob()),
        );
        pending.drop_slot(SlotNumber(10));

        assert!(pending.track_data(track).is_none());
    }

    #[test]
    fn registered_tracks_by_tape_returns_pending_only() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let other_tape = Address::new_unique();
        let track = Address::new_unique();
        let other_track = Address::new_unique();
        let state = registered_blob(tape);
        let mut other = registered_blob(other_tape);
        other.track_number = TrackNumber(2);

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(sample_blob()));
        pending.apply_register(
            SlotNumber(11),
            other_track,
            other,
            BlobData::Coded(sample_blob()),
        );
        pending.apply_certify(SlotNumber(12), track);

        let tracks = pending.registered_tracks_by_tape(tape);
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].0, track);
        assert_eq!(tracks[0].1.state, TrackState::Certified as u64);
    }

    fn named(name: &[u8], size: u64, content_type: ContentType) -> ReplayTrackObject {
        ReplayTrackObject {
            name: name.to_vec(),
            content_type,
            logical_size: StorageUnits::from_bytes(size),
        }
    }

    #[test]
    fn named_object_waits_for_certification_and_rolls_back_with_it() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);
        let blob = sample_blob();

        pending.apply_register_with_object(
            SlotNumber(10),
            track,
            state,
            BlobData::Coded(blob),
            Some(named(b"index.html", 321, ContentType::TextHtml)),
            Some(1_700_000_000),
        );
        assert!(pending.named_object(tape, b"index.html").is_none());

        pending.apply_certify(SlotNumber(11), track);
        let object = pending
            .named_object(tape, b"index.html")
            .expect("certified pending object");
        assert_eq!(object.track_address, track);
        assert_eq!(object.track_number, TrackNumber(0));
        assert_eq!(object.size, 321);
        assert_eq!(object.content_type, ContentType::TextHtml);
        assert_eq!(object.block_time, Some(1_700_000_000));

        pending.drop_slot(SlotNumber(11));
        assert!(pending.named_object(tape, b"index.html").is_none());
    }

    #[test]
    fn named_object_uses_newest_certified_track_and_reverts_on_rollback() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let first_track = Address::new_unique();
        let second_track = Address::new_unique();
        let first = registered_blob(tape);
        let mut second = registered_blob(tape);
        second.track_number = TrackNumber(1);

        pending.apply_register_with_object(
            SlotNumber(10),
            first_track,
            first,
            BlobData::Coded(sample_blob()),
            Some(named(b"app.js", 10, ContentType::TextJavascript)),
            None,
        );
        pending.apply_certify(SlotNumber(11), first_track);
        pending.apply_register_with_object(
            SlotNumber(12),
            second_track,
            second,
            BlobData::Coded(sample_blob()),
            Some(named(b"app.js", 20, ContentType::TextJavascript)),
            None,
        );
        pending.apply_certify(SlotNumber(13), second_track);

        let newest = pending
            .named_object(tape, b"app.js")
            .expect("replacement visible");
        assert_eq!(newest.track_address, second_track);
        assert_eq!(newest.size, 20);

        pending.drop_slot(SlotNumber(13));
        let reverted = pending
            .named_object(tape, b"app.js")
            .expect("older certified object visible");
        assert_eq!(reverted.track_address, first_track);
        assert_eq!(reverted.size, 10);
    }

    #[test]
    fn certified_inline_named_object_is_immediately_visible() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let bytes = b"console.log('ready')".to_vec();
        let mut state = registered_blob(tape);
        state.kind = TrackKind::Inline as u64;
        state.state = TrackState::Certified as u64;
        state.size = StorageUnits::from_bytes(bytes.len() as u64);
        state.value_hash = tape_crypto::hash::hash(&bytes);

        pending.apply_register_with_object(
            SlotNumber(20),
            track,
            state,
            BlobData::Inline(bytes.clone()),
            Some(named(
                b"assets/app.js",
                bytes.len() as u64,
                ContentType::TextJavascript,
            )),
            None,
        );

        let object = pending
            .named_object(tape, b"assets/app.js")
            .expect("inline object visible at registration");
        assert_eq!(object.track_address, track);
        assert_eq!(object.size, bytes.len() as u64);
        assert_eq!(object.etag, object_etag(&state, None));
    }

    // a pending registration is found by its tape and number
    #[test]
    fn address_by_number() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let state = registered_blob(tape);

        pending.apply_register(SlotNumber(10), track, state, BlobData::Coded(sample_blob()));
        let by_number = |tape, tape_number, number| {
            pending.track_address_by_number(tape, tape_number, TrackNumber(number))
        };

        assert_eq!(by_number(tape, state.tape_number, 0), Some(track));
        assert_eq!(by_number(tape, state.tape_number, 1), None);
        assert_eq!(by_number(tape, state.tape_number.next(), 0), None);
        assert_eq!(by_number(Address::new_unique(), state.tape_number, 0), None);

        pending.drop_slot(SlotNumber(10));
        assert_eq!(by_number(tape, state.tape_number, 0), None);
    }

    fn tape_row(id: u64) -> TapeInfo {
        TapeInfo::new(TapeNumber(id), 0, EpochNumber(10), TrackNumber(3))
    }

    // a pending rewind moves the stored row's tree start and nonce, and
    // dropping its slot puts the row back
    #[test]
    fn pending_rewind() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let nonce = Hash::from([7u8; 32]);

        pending.apply_rewind(SlotNumber(10), tape, TapeNumber(1), TrackNumber(3), nonce);

        let row = pending.apply_to_tape(tape, Some(tape_row(1))).expect("tape row");
        assert_eq!(row.tree_start, TrackNumber(3));
        assert_eq!(row.nonce, nonce);

        // A rewind of another tape number at the address leaves the row alone.
        assert_eq!(pending.apply_to_tape(tape, Some(tape_row(2))), Some(tape_row(2)));
        assert_eq!(pending.apply_to_tape(tape, None), None);

        pending.drop_slot(SlotNumber(10));
        assert_eq!(pending.apply_to_tape(tape, Some(tape_row(1))), Some(tape_row(1)));
    }

    // a pending close hides the stored row, and a reservation pending after
    // it shows the new tape at the address
    #[test]
    fn pending_close_and_reserve() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();

        pending.apply_close(SlotNumber(10), tape, TapeNumber(1));
        assert_eq!(pending.apply_to_tape(tape, Some(tape_row(1))), None);

        pending.apply_reserve(SlotNumber(11), tape, tape_row(2));
        assert_eq!(pending.apply_to_tape(tape, Some(tape_row(1))), Some(tape_row(2)));

        // Once the store has applied both, its row for the new tape wins.
        let stored = TapeInfo { next_track_number: TrackNumber(9), ..tape_row(2) };
        assert_eq!(pending.apply_to_tape(tape, Some(stored.clone())), Some(stored));
    }

    // a pending renew gives the stored row its new expiry and tree start
    #[test]
    fn pending_renew() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let nonce = Hash::from([7u8; 32]);

        pending.apply_renew(SlotNumber(10), tape, TapeNumber(1), EpochNumber(20), TrackNumber(3), nonce);

        let row = pending.apply_to_tape(tape, Some(tape_row(1))).expect("tape row");
        assert_eq!(row.end_epoch, EpochNumber(20));
        assert_eq!(row.tree_start, TrackNumber(3));
        assert_eq!(row.nonce, nonce);
    }

    // an update to a tape whose reservation is pending keeps a rewind that is
    // pending too
    #[test]
    fn pending_update_after_rewind() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let nonce = Hash::from([7u8; 32]);
        let delegate = Address::new_unique();

        pending.apply_reserve(SlotNumber(10), tape, tape_row(1));
        pending.apply_rewind(SlotNumber(11), tape, TapeNumber(1), TrackNumber(5), nonce);
        pending.apply_tape_update(SlotNumber(12), tape, |info| info.delegate = delegate);

        let row = pending.apply_to_tape(tape, None).expect("tape row");
        assert_eq!(row.tree_start, TrackNumber(5));
        assert_eq!(row.delegate, delegate);
    }
}
