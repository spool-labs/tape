//! What a replayed record changes for the tapes lists read

use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent};
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::address::Address;

/// A change a consumer reports to its lists, once its source reflects it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Change {
    /// A track was written, or the consumer received its bytes
    Written { tape: Address, track_number: TrackNumber },
    /// A track was deleted or invalidated
    Removed { track: Address },
    /// A tape was reserved, extended, closed, rewound or renewed
    Tape { tape: Address },
    /// The epoch advanced, so tapes may have expired
    Epoch { epoch: EpochNumber },
}

/// The change a record makes, if lists could care about it
pub fn change(record: &ReplayRecord) -> Option<Change> {
    if let ReplayableEvent::Track(written) = &record.event {
        return Some(Change::Written {
            tape: written.state.tape,
            track_number: written.state.track_number,
        });
    }
    if let ReplayableEvent::DeleteTrack { track, .. } | ReplayableEvent::InvalidateTrack { track, .. } =
        &record.event
    {
        return Some(Change::Removed { track: *track });
    }
    // A close, a rewind and a renew each take every track off the tape at
    // once, so a list that reads the tape reads it again.
    if let ReplayableEvent::ReserveTape { tape, .. }
    | ReplayableEvent::ExtendTape { tape, .. }
    | ReplayableEvent::CloseTape { tape, .. }
    | ReplayableEvent::RewindTape { tape, .. }
    | ReplayableEvent::RenewTape { tape, .. } = &record.event
    {
        return Some(Change::Tape { tape: *tape });
    }
    if let ReplayableEvent::AdvanceEpoch { new_epoch, .. } = &record.event {
        return Some(Change::Epoch { epoch: *new_epoch });
    }
    None
}

#[cfg(test)]
mod tests {
    use tape_core::snapshot::replay::ReplayTrack;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{StorageUnits, TapeNumber};
    use tape_crypto::Hash;
    use tape_crypto::tx::Txid;

    use super::*;

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event,
        }
    }

    fn written(tape: Address, number: u64) -> ReplayableEvent {
        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                tape_number: TapeNumber(1),
                track_number: TrackNumber(number),
                key: Hash::new_unique(),
                kind: TrackKind::Inline as u64,
                state: TrackState::Registered as u64,
                flags: TrackFlags::NONE,
                size: StorageUnits::from_bytes(1),
                group: VirtualGroup::from(0),
                value_hash: Hash::new_unique(),
            },
            epoch: EpochNumber(1),
            blob: None,
            object: None,
        })
    }

    // writes, deletes, invalidations, reservations and epochs become changes; other events don't
    #[test]
    fn changes() {
        let (tape, track) = (Address::new_unique(), Address::new_unique());

        let write = change(&record(written(tape, 3)));
        let delete = change(&record(ReplayableEvent::DeleteTrack { track, epoch: EpochNumber(2) }));
        let invalidate = change(&record(ReplayableEvent::InvalidateTrack { track, epoch: EpochNumber(2) }));
        let certify = change(&record(ReplayableEvent::CertifyTrack { track, epoch: EpochNumber(2) }));

        assert_eq!(write, Some(Change::Written { tape, track_number: TrackNumber(3) }));
        assert_eq!(delete, Some(Change::Removed { track }));
        assert_eq!(invalidate, Some(Change::Removed { track }));
        assert_eq!(certify, None);
    }

    // a close and a rewind change the tape as a whole
    #[test]
    fn tape_cleared() {
        let tape = Address::new_unique();
        let (id, epoch) = (TapeNumber(1), EpochNumber(2));

        let close = change(&record(ReplayableEvent::CloseTape { tape, id, epoch }));
        let rewind = change(&record(ReplayableEvent::RewindTape {
            tape,
            id,
            epoch,
            tree_start: TrackNumber(4),
            nonce: Hash::new_unique(),
        }));

        assert_eq!(close, Some(Change::Tape { tape }));
        assert_eq!(rewind, Some(Change::Tape { tape }));
    }
}
