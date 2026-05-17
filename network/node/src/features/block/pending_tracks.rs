//! In-memory pending-track state from confirmed-but-not-yet-finalized slots.
//!
//! The block ingestor appends events here as soon as a confirmed block enters
//! the pending queue, so SDK reads, peer queries, `put_slice`, and `certify`
//! see new tracks at confirmed latency rather than waiting for finalization.
//!
//! Only `Register` and `Certify` flow through pending state. `Invalidate` and
//! `Delete` are destructive and remain on the finalized path — exposing them
//! from confirmed state risks showing a track as gone when the block that
//! removed it gets reorged out.
//!
//! Read paths fold pending events on top of the disk-backed `TapeStore` via
//! `apply_to_track`. Once a slot is finalized (and the corresponding events
//! have been applied to disk by `StoreManager`) or rolled back, `drop_slot`
//! removes the events for that slot.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use tape_blocks::ParsedInstruction;
use tape_core::track::data::TrackData;
use tape_core::track::types::{CompressedTrack, TrackState};
use tape_core::types::SlotNumber;
use tape_crypto::address::Address;

use crate::features::block::ingestor::ParsedBlock;

#[derive(Debug)]
enum EventKind {
    Register {
        state: CompressedTrack,
        /// Track payload — `TrackData::Blob(blob_info)` for blob tracks
        /// (matches what `store.get_track_data` returns) or
        /// `TrackData::Raw(bytes)` for inline raw tracks. Read paths consult
        /// this via [`PendingTracks::track_data`].
        data: TrackData,
    },
    Certify,
}

#[derive(Debug)]
struct Event {
    slot: SlotNumber,
    kind: EventKind,
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
        data: TrackData,
    ) {
        self.append(
            track,
            Event {
                slot,
                kind: EventKind::Register { state, data },
            },
        );
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
                    self.apply_register(block.slot, *track, state, value.clone());
                }
                ParsedInstruction::CertifyTrack { track, .. } => {
                    self.apply_certify(block.slot, *track);
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
    pub fn track_data(&self, track: Address) -> Option<TrackData> {
        let inner = self.inner.read().expect("pending-tracks lock poisoned");
        let events = inner.events_by_track.get(&track)?;
        for event in events.iter().rev() {
            if let EventKind::Register { data, .. } = &event.kind {
                return Some(data.clone());
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
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::types::TrackKind;
    use tape_core::types::{StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::Hash;

    use super::*;

    fn registered_blob(tape: Address) -> CompressedTrack {
        CompressedTrack {
            tape,
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: TrackKind::Blob as u64,
            state: TrackState::Registered as u64,
            size: StorageUnits::from_bytes(1024),
            group: GroupIndex::from(0),
            value_hash: Hash::new_unique(),
        }
    }

    fn sample_blob() -> BlobInfo {
        BlobInfo {
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
            TrackData::Blob(sample_blob()),
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

        pending.apply_register(SlotNumber(10), track, state, TrackData::Blob(sample_blob()));
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

        pending.apply_register(SlotNumber(10), track, state, TrackData::Blob(sample_blob()));
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

        pending.apply_register(SlotNumber(10), track, state, TrackData::Blob(sample_blob()));
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

        pending.apply_register(SlotNumber(10), track, state, TrackData::Blob(sample_blob()));
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

        pending.apply_register(SlotNumber(10), track, state, TrackData::Blob(blob));

        assert_eq!(
            pending.track_data(track),
            Some(TrackData::Blob(blob))
        );
    }

    #[test]
    fn track_data_exposes_raw_payload() {
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let mut state = registered_blob(tape);
        state.kind = TrackKind::Raw as u64;
        state.state = TrackState::Certified as u64;
        let bytes = vec![0xAB; 16];

        pending.apply_register(
            SlotNumber(10),
            track,
            state,
            TrackData::Raw(bytes.clone()),
        );

        assert_eq!(
            pending.track_data(track),
            Some(TrackData::Raw(bytes))
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
            TrackData::Blob(sample_blob()),
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

        pending.apply_register(SlotNumber(10), track, state, TrackData::Blob(sample_blob()));
        pending.apply_register(
            SlotNumber(11),
            other_track,
            other,
            TrackData::Blob(sample_blob()),
        );
        pending.apply_certify(SlotNumber(12), track);

        let tracks = pending.registered_tracks_by_tape(tape);
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].0, track);
        assert_eq!(tracks[0].1.state, TrackState::Certified as u64);
    }
}
