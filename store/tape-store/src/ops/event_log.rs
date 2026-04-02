//! EventLog operations for snapshot event persistence

use std::collections::BTreeMap;

use crate::columns::EventLogCol;
use crate::error::{Result, TapeStoreError};
use crate::types::keys::EventLogKey;
use crate::TapeStore;
use store::{Column, Store};
use tape_core::snapshot::types::{ReplayableEvent, SnapshotEntry};
use tape_core::types::{EpochNumber, SlotNumber};

/// Serialize an EventLogKey to raw bytes (bypassing TypedStore).
fn serialize_key(key: &EventLogKey) -> Vec<u8> {
    wincode::serialize(key).expect("EventLogKey serialization should not fail")
}

/// Operations for the event log column family.
///
/// The event log stores replayable events per-epoch, ordered by (slot, sequence).
/// After a snapshot is built and certified for an epoch, the event log for that
/// epoch can be garbage collected.
pub trait EventLogOps {
    /// Append a replayable event to the epoch's event log.
    fn append_event(
        &self,
        epoch: EpochNumber,
        slot: SlotNumber,
        event: &ReplayableEvent,
    ) -> Result<()>;

    /// Read all events for an epoch, grouped by slot and ordered by (slot, seq).
    fn get_epoch_events(&self, epoch: EpochNumber) -> Result<Vec<SnapshotEntry>>;

    /// Delete all events for an epoch (GC after snapshot built).
    fn delete_epoch_events(&self, epoch: EpochNumber) -> Result<()>;

    /// Check if any events exist for an epoch.
    fn has_epoch_events(&self, epoch: EpochNumber) -> Result<bool>;
}

impl<S: Store> EventLogOps for TapeStore<S> {
    fn append_event(
        &self,
        epoch: EpochNumber,
        slot: SlotNumber,
        event: &ReplayableEvent,
    ) -> Result<()> {
        let raw = self.inner().inner();

        // Find next sequence number for this (epoch, slot) by counting existing entries.
        let prefix = {
            let mut buf = [0u8; 16];
            buf[0..8].copy_from_slice(&epoch.0.to_be_bytes());
            buf[8..16].copy_from_slice(&slot.0.to_be_bytes());
            buf
        };

        let seq = raw.iter_prefix(EventLogCol::CF_NAME, &prefix)?.count() as u32;

        let key = serialize_key(&EventLogKey::new(epoch.0, slot.0, seq));
        let value = wincode::serialize(event)
            .map_err(|e| TapeStoreError::Serialization(format!("event: {}", e)))?;

        raw.put(EventLogCol::CF_NAME, &key, &value)?;
        Ok(())
    }

    fn get_epoch_events(&self, epoch: EpochNumber) -> Result<Vec<SnapshotEntry>> {
        let prefix = EventLogKey::epoch_prefix(epoch.0);

        let iter = self
            .inner()
            .inner()
            .iter_prefix(EventLogCol::CF_NAME, &prefix)?;

        // Group events by slot, maintaining order
        let mut slots: BTreeMap<u64, Vec<ReplayableEvent>> = BTreeMap::new();

        for (key_bytes, value_bytes) in iter {
            let key: EventLogKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("event key: {}", e)))?;
            let event: ReplayableEvent = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("event value: {}", e)))?;

            slots.entry(key.slot).or_default().push(event);
        }

        let entries = slots
            .into_iter()
            .map(|(slot, events)| SnapshotEntry {
                slot: SlotNumber(slot),
                events,
            })
            .collect();

        Ok(entries)
    }

    fn delete_epoch_events(&self, epoch: EpochNumber) -> Result<()> {
        let raw = self.inner().inner();
        let prefix = EventLogKey::epoch_prefix(epoch.0);

        let keys: Vec<Vec<u8>> = raw
            .iter_prefix(EventLogCol::CF_NAME, &prefix)?
            .map(|(k, _)| k)
            .collect();

        for key in keys {
            raw.delete(EventLogCol::CF_NAME, &key)?;
        }

        Ok(())
    }

    fn has_epoch_events(&self, epoch: EpochNumber) -> Result<bool> {
        let prefix = EventLogKey::epoch_prefix(epoch.0);

        let iter = self
            .inner()
            .inner()
            .iter_prefix(EventLogCol::CF_NAME, &prefix)?;

        Ok(iter.into_iter().next().is_some())
    }
}

#[cfg(test)]
mod tests {
    use solana_program::pubkey::Pubkey;

    use super::*;
    use store_memory::MemoryStore;
    use tape_core::snapshot::types::ReplayTrack;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_crypto::hash::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_append_and_get_events() {
        let store = test_store();
        let epoch = EpochNumber(1);

        // Append events across multiple slots
        store
            .append_event(
                epoch,
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                },
            )
            .unwrap();

        store
            .append_event(
                epoch,
                SlotNumber(150),
                &ReplayableEvent::Track(ReplayTrack {
                    state: CompressedTrack {
                        tape: Pubkey::new_from_array([2u8; 32]),
                        key: Hash::default(),
                        track_number: TrackNumber(1),
                        kind: TrackKind::Raw as u64,
                        state: TrackState::Certified as u64,
                        size: StorageUnits(100),
                        spool_group: SpoolGroup::from(7),
                        value_hash: Hash::default(),
                    },
                    epoch,
                    blob: None,
                }),
            )
            .unwrap();

        store
            .append_event(
                epoch,
                SlotNumber(150),
                &ReplayableEvent::CertifyTrack {
                    track: [1u8; 32],
                    epoch: EpochNumber(1),
                },
            )
            .unwrap();

        let entries = store.get_epoch_events(epoch).unwrap();

        assert_eq!(entries.len(), 2); // 2 distinct slots
        assert_eq!(entries[0].slot, SlotNumber(100));
        assert_eq!(entries[0].events.len(), 1);
        assert_eq!(entries[1].slot, SlotNumber(150));
        assert_eq!(entries[1].events.len(), 2);
    }

    #[test]
    fn test_has_epoch_events() {
        let store = test_store();
        let epoch = EpochNumber(5);

        assert!(!store.has_epoch_events(epoch).unwrap());

        store
            .append_event(
                epoch,
                SlotNumber(10),
                &ReplayableEvent::JoinNetwork {
                    node: [1u8; 32],
                },
            )
            .unwrap();

        assert!(store.has_epoch_events(epoch).unwrap());
    }

    #[test]
    fn test_delete_epoch_events() {
        let store = test_store();
        let epoch = EpochNumber(3);

        store
            .append_event(
                epoch,
                SlotNumber(10),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(2),
                    new_epoch: EpochNumber(3),
                },
            )
            .unwrap();

        store
            .append_event(
                epoch,
                SlotNumber(20),
                &ReplayableEvent::RegisterNode {
                    authority: [1u8; 32],
                    node: [2u8; 32],
                },
            )
            .unwrap();

        assert!(store.has_epoch_events(epoch).unwrap());

        store.delete_epoch_events(epoch).unwrap();

        assert!(!store.has_epoch_events(epoch).unwrap());
        assert!(store.get_epoch_events(epoch).unwrap().is_empty());
    }

    #[test]
    fn test_epoch_isolation() {
        let store = test_store();

        // Events in epoch 1
        store
            .append_event(
                EpochNumber(1),
                SlotNumber(10),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                },
            )
            .unwrap();

        // Events in epoch 2
        store
            .append_event(
                EpochNumber(2),
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(1),
                    new_epoch: EpochNumber(2),
                },
            )
            .unwrap();

        let epoch1_events = store.get_epoch_events(EpochNumber(1)).unwrap();
        let epoch2_events = store.get_epoch_events(EpochNumber(2)).unwrap();

        assert_eq!(epoch1_events.len(), 1);
        assert_eq!(epoch2_events.len(), 1);
        assert_eq!(epoch1_events[0].slot, SlotNumber(10));
        assert_eq!(epoch2_events[0].slot, SlotNumber(100));

        // Delete epoch 1, epoch 2 should survive
        store.delete_epoch_events(EpochNumber(1)).unwrap();
        assert!(!store.has_epoch_events(EpochNumber(1)).unwrap());
        assert!(store.has_epoch_events(EpochNumber(2)).unwrap());
    }

    #[test]
    fn test_ordering_within_slot() {
        let store = test_store();
        let epoch = EpochNumber(1);
        let slot = SlotNumber(50);

        // Append 5 events in same slot
        for i in 0..5u8 {
            store
                .append_event(
                    epoch,
                    slot,
                    &ReplayableEvent::SyncEpoch {
                        node: [i; 32],
                        node_id: tape_core::types::NodeId(i as u64),
                        epoch,
                        spools_hash: Hash::default(),
                    },
                )
                .unwrap();
        }

        let entries = store.get_epoch_events(epoch).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].events.len(), 5);

        // Verify order is preserved
        for (i, event) in entries[0].events.iter().enumerate() {
            match event {
                ReplayableEvent::SyncEpoch { node, .. } => {
                    assert_eq!(node[0], i as u8);
                }
                _ => panic!("unexpected event type"),
            }
        }
    }

    #[test]
    fn test_empty_epoch() {
        let store = test_store();
        let entries = store.get_epoch_events(EpochNumber(999)).unwrap();
        assert!(entries.is_empty());
    }
}
