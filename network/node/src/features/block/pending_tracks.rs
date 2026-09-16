//! In-memory pending-track state from confirmed-but-not-yet-finalized slots.
//!
//! The block ingestor appends events here as soon as a confirmed block enters
//! the pending queue, so SDK reads, peer queries, `put_slice`, and `certify`
//! see new tracks at confirmed latency rather than waiting for finalization.
//!
//! Only `Reserve`, `Register` and `Certify` flow through pending state.
//! `Invalidate` and `Delete` are destructive and remain on the finalized path —
//! exposing them from confirmed state risks showing a track as gone when the
//! block that removed it gets reorged out.
//!
//! Reservations are here because a track without its tape is not servable: a
//! proof needs the tape's track set, so a fresh tape's first track would be
//! not-found on every peer for a whole finality window while the overlay
//! resolved the track and the tape read fell through to disk.
//!
//! Read paths fold pending events on top of the disk-backed `TapeStore` via
//! `apply_to_track`. Once a slot is finalized (and the corresponding events
//! have been applied to disk by `StoreManager`) or rolled back, `drop_slot`
//! removes the events for that slot.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use tape_blocks::ParsedInstruction;
use tape_core::object::object_etag;
use tape_core::snapshot::replay::ReplayTrackObject;
use tape_core::track::data::BlobData;
use tape_core::track::types::{CompressedTrack, TrackState};
use tape_core::types::{ContentType, SlotNumber, TrackNumber};
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_store::types::TapeInfo;

use crate::features::block::ingestor::ParsedBlock;

#[derive(Debug)]
enum EventKind {
    Register {
        state: CompressedTrack,
        /// Track payload — `BlobData::Coded(blob_encoding)` for blob tracks
        /// (matches what `store.get_track_data` returns) or
        /// `BlobData::Inline(bytes)` for inline raw tracks. Read paths consult
        /// this via [`PendingTracks::track_data`].
        data: Box<BlobData>,
        /// Name-addressed object metadata carried by this registration.
        object: Option<ReplayTrackObject>,
        /// Block time used for the same response metadata as the finalized
        /// object index.
        block_time: Option<i64>,
    },
    Certify,
}

#[derive(Debug)]
struct Event {
    slot: SlotNumber,
    kind: EventKind,
}

#[derive(Debug)]
struct TapeEvent {
    slot: SlotNumber,
    info: TapeInfo,
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

    /// Per-tape reservation log, in append order.
    reservations: HashMap<Address, Vec<TapeEvent>>,

    /// Reverse index for dropping a slot's reservations.
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
        let mut inner = self.inner.write().expect("pending-tracks lock poisoned");
        inner
            .reservations
            .entry(tape)
            .or_default()
            .push(TapeEvent { slot, info });
        inner.tapes_by_slot.entry(slot).or_default().push(tape);
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
    /// path.
    pub fn apply_block(&self, block: &ParsedBlock) {
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
                    let Some(meta) = value.meta() else {
                        continue;
                    };
                    let state = CompressedTrack {
                        tape: event.tape,
                        track_number: event.track_number,
                        key: *key,
                        kind: meta.kind as u64,
                        state: meta.state as u64,
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
                        },
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
                let drained_empty = match inner.reservations.get_mut(&tape) {
                    Some(events) => {
                        events.retain(|event| event.slot != slot);
                        events.is_empty()
                    }
                    None => false,
                };
                if drained_empty {
                    inner.reservations.remove(&tape);
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

    /// Fold a pending reservation for `tape` on top of what the store holds.
    ///
    /// A reservation is the whole tape record, so the newest one wins outright
    /// rather than merging field by field.
    pub fn apply_to_tape(&self, tape: Address, in_store: Option<TapeInfo>) -> Option<TapeInfo> {
        if in_store.is_some() {
            return in_store;
        }

        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        inner
            .reservations
            .get(&tape)
            .and_then(|events| events.last())
            .map(|event| event.info.clone())
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
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::spooler::GroupIndex;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::TrackKind;
    use tape_core::types::{StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::Hash;

    use super::*;

    fn registered_blob(tape: Address) -> CompressedTrack {
        CompressedTrack {
            tape,
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            size: StorageUnits::from_bytes(1024),
            group: GroupIndex::from(0),
            value_hash: Hash::new_unique(),
        }
    }

    fn sample_blob() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::from_bytes(1024),
            commitment: Hash::default(),
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(1),
            leaves: [Hash::default(); GROUP_SIZE],
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
}
