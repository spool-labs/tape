//! EventLog operations for snapshot event persistence

use std::collections::BTreeMap;

use crate::columns::EventLogCol;
use crate::error::{Result, TapeStoreError};
use crate::types::keys::EventLogKey;
use crate::TapeStore;
use store::{Column, Store};
use serde::{Deserialize, Serialize};
use tape_core::snapshot::replay::{ReplayRecord, SnapshotEntry};
use tape_core::types::{EpochNumber, SlotNumber};
use wincode_derive::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct EventLogValue {
    block_time: Option<i64>,
    record: ReplayRecord,
}

#[derive(Debug, Default)]
struct SlotRecords {
    block_time: Option<i64>,
    records: Vec<ReplayRecord>,
}

/// Serialize an EventLogKey to raw bytes (bypassing TypedStore).
fn serialize_key(key: &EventLogKey) -> Vec<u8> {
    wincode::serialize(key).expect("EventLogKey serialization should not fail")
}

/// Operations for the event log column family.
///
/// The event log stores replay records per-epoch, ordered by (slot, sequence).
/// After a snapshot is built and certified for an epoch, the event log for that
/// epoch can be garbage collected.
pub trait EventLogOps {
    /// Append a replay record to the epoch's event log.
    fn append_record(
        &self,
        epoch: EpochNumber,
        slot: SlotNumber,
        block_time: Option<i64>,
        record: &ReplayRecord,
    ) -> Result<()>;

    /// Read all records for an epoch, grouped by slot and ordered by (slot, seq).
    fn get_epoch_events(&self, epoch: EpochNumber) -> Result<Vec<SnapshotEntry>>;

    /// Delete all events for an epoch (GC after snapshot built).
    fn delete_epoch_events(&self, epoch: EpochNumber) -> Result<()>;

    /// Check if any events exist for an epoch.
    fn has_epoch_events(&self, epoch: EpochNumber) -> Result<bool>;
}

impl<S: Store> EventLogOps for TapeStore<S> {
    fn append_record(
        &self,
        epoch: EpochNumber,
        slot: SlotNumber,
        block_time: Option<i64>,
        record: &ReplayRecord,
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
        let value = wincode::serialize(&EventLogValue {
            block_time,
            record: record.clone(),
        })
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

        // Group records by slot, maintaining order
        let mut slots: BTreeMap<u64, SlotRecords> = BTreeMap::new();

        for (key_bytes, value_bytes) in iter {
            let key: EventLogKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("event key: {}", e)))?;
            let value: EventLogValue = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("event value: {}", e)))?;

            let slot = slots.entry(key.slot).or_default();
            if slot.block_time.is_none() {
                slot.block_time = value.block_time;
            }
            slot.records.push(value.record);
        }

        let entries = slots
            .into_iter()
            .map(|(slot, value)| SnapshotEntry {
                slot: SlotNumber(slot),
                block_time: value.block_time,
                records: value.records,
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
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_core::system::NodePreferences;
    use tape_core::types::coin::TAPE;
    use tape_core::types::NodeId;
    use store_memory::MemoryStore;
    use tape_core::snapshot::replay::{ReplayRecord, ReplayTrack, ReplayableEvent};
    use tape_core::spooler::GroupIndex;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{SpoolIndex, StorageUnits, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::hash::Hash;
    use tape_crypto::tx::Txid;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event,
        }
    }

    #[test]
    fn test_append_and_get_events() {
        let store = test_store();
        let epoch = EpochNumber(1);

        // Append events across multiple slots
        store
            .append_record(
                epoch,
                SlotNumber(100),
                Some(1_700_000_000),
                &record(ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                }),
            )
            .unwrap();

        store
            .append_record(
                epoch,
                SlotNumber(150),
                Some(1_700_000_050),
                &record(ReplayableEvent::Track(ReplayTrack {
                    state: CompressedTrack {
                        tape: Address::new([2u8; 32]),
                        key: Hash::default(),
                        track_number: TrackNumber(1),
                        kind: TrackKind::Raw as u64,
                        state: TrackState::Certified as u64,
                        size: StorageUnits(100),
                        group: GroupIndex::from(7),
                        value_hash: Hash::default(),
                    },
                    epoch,
                    blob: None,
                })),
            )
            .unwrap();

        store
            .append_record(
                epoch,
                SlotNumber(150),
                Some(1_700_000_050),
                &record(ReplayableEvent::CertifyTrack {
                    track: Address::new([1u8; 32]),
                    epoch: EpochNumber(1),
                }),
            )
            .unwrap();

        let entries = store.get_epoch_events(epoch).unwrap();

        assert_eq!(entries.len(), 2); // 2 distinct slots
        assert_eq!(entries[0].slot, SlotNumber(100));
        assert_eq!(entries[0].block_time, Some(1_700_000_000));
        assert_eq!(entries[0].records.len(), 1);
        assert_eq!(entries[1].slot, SlotNumber(150));
        assert_eq!(entries[1].block_time, Some(1_700_000_050));
        assert_eq!(entries[1].records.len(), 2);
    }

    #[test]
    fn test_has_epoch_events() {
        let store = test_store();
        let epoch = EpochNumber(5);

        assert!(!store.has_epoch_events(epoch).unwrap());

        store
            .append_record(
                epoch,
                SlotNumber(10),
                None,
                &record(ReplayableEvent::JoinCommittee {
                    node: Address::new([1u8; 32]),
                    stake: TAPE(0),
                    key: BlsPubkey::zeroed(),
                    preferences: NodePreferences::zeroed(),
                    activation_epoch: EpochNumber(0),
                }),
            )
            .unwrap();

        assert!(store.has_epoch_events(epoch).unwrap());
    }

    #[test]
    fn test_delete_epoch_events() {
        let store = test_store();
        let epoch = EpochNumber(3);

        store
            .append_record(
                epoch,
                SlotNumber(10),
                None,
                &record(ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(2),
                    new_epoch: EpochNumber(3),
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                }),
            )
            .unwrap();

        store
            .append_record(
                epoch,
                SlotNumber(20),
                None,
                &record(ReplayableEvent::RegisterNode {
                    authority: [1u8; 32].into(),
                    node: [2u8; 32].into(),
                    id: NodeId(0),
                }),
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
            .append_record(
                EpochNumber(1),
                SlotNumber(10),
                None,
                &record(ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                }),
            )
            .unwrap();

        // Events in epoch 2
        store
            .append_record(
                EpochNumber(2),
                SlotNumber(100),
                None,
                &record(ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(1),
                    new_epoch: EpochNumber(2),
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                }),
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
                .append_record(
                    epoch,
                    slot,
                    None,
                    &record(ReplayableEvent::SyncSpool {
                        node: [i; 32].into(),
                        epoch,
                        group: GroupIndex::containing(SpoolIndex(i as u64)),
                        spool: SpoolIndex(i as u64),
                    }),
                )
                .unwrap();
        }

        let entries = store.get_epoch_events(epoch).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].records.len(), 5);

        // Verify order is preserved
        for (i, record) in entries[0].records.iter().enumerate() {
            match &record.event {
                ReplayableEvent::SyncSpool { node, .. } => {
                    assert_eq!(node.to_bytes()[0], i as u8);
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
