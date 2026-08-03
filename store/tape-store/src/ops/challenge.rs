//! Per-peer challenge history

use store::{Column, Store};
use tape_core::challenge::PeerRecord;
use tape_core::types::{EpochNumber, RoundNumber};
use tape_crypto::address::Address;

use crate::columns::{ChallengeRecordCol, ChallengeRoundCol};
use crate::error::{Result, TapeStoreError};
use crate::types::ChallengeRoundKey;
use crate::TapeStore;

/// Operations for the local record of peer answers
pub trait ChallengeOps {
    /// This node's record of one peer, default when it has never been challenged.
    fn peer_record(&self, peer: Address) -> Result<PeerRecord>;

    /// Store a peer's record.
    fn put_peer_record(&self, peer: Address, record: PeerRecord) -> Result<()>;

    /// Every peer this node holds a record for, for reporting and outliers.
    fn iter_peer_records(&self) -> Result<Vec<(Address, PeerRecord)>>;

    /// Forget a peer, once it is gone from the network rather than merely quiet.
    fn delete_peer_record(&self, peer: Address) -> Result<()>;

    /// Note how one peer fared in one round.
    fn put_round_outcome(
        &self,
        peer: Address,
        epoch: EpochNumber,
        round: RoundNumber,
        certified: bool,
    ) -> Result<()>;

    /// The outcome recorded for one peer in one round, if any.
    fn round_outcome(
        &self,
        peer: Address,
        epoch: EpochNumber,
        round: RoundNumber,
    ) -> Result<Option<bool>>;

    /// One peer's rounds in the order they happened, oldest first.
    ///
    /// Answers which rounds a node failed, where the counters on `PeerRecord`
    /// only answer how many.
    fn peer_rounds(&self, peer: Address) -> Result<Vec<(EpochNumber, RoundNumber, bool)>>;

    /// Drop every round recorded before an epoch, once nobody can dispute them.
    fn prune_rounds_before(&self, epoch: EpochNumber) -> Result<usize>;
}

impl<S: Store> ChallengeOps for TapeStore<S> {
    fn peer_record(&self, peer: Address) -> Result<PeerRecord> {
        Ok(self.get::<ChallengeRecordCol>(&peer)?.unwrap_or_default())
    }

    fn put_peer_record(&self, peer: Address, record: PeerRecord) -> Result<()> {
        self.put::<ChallengeRecordCol>(&peer, &record)?;
        Ok(())
    }

    fn iter_peer_records(&self) -> Result<Vec<(Address, PeerRecord)>> {
        Ok(self.iter::<ChallengeRecordCol>()?)
    }

    fn delete_peer_record(&self, peer: Address) -> Result<()> {
        self.delete::<ChallengeRecordCol>(&peer)?;
        Ok(())
    }

    fn put_round_outcome(
        &self,
        peer: Address,
        epoch: EpochNumber,
        round: RoundNumber,
        certified: bool,
    ) -> Result<()> {
        let key = ChallengeRoundKey::new(peer, epoch, round);
        self.put::<ChallengeRoundCol>(&key, &certified)?;
        Ok(())
    }

    fn round_outcome(
        &self,
        peer: Address,
        epoch: EpochNumber,
        round: RoundNumber,
    ) -> Result<Option<bool>> {
        let key = ChallengeRoundKey::new(peer, epoch, round);
        Ok(self.get::<ChallengeRoundCol>(&key)?)
    }

    fn peer_rounds(&self, peer: Address) -> Result<Vec<(EpochNumber, RoundNumber, bool)>> {
        let prefix = ChallengeRoundKey::peer_prefix(peer);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(ChallengeRoundCol::CF_NAME, &prefix)?;

        // The key is peer then epoch then round, all big-endian, so the scan is
        // already in the order the rounds happened.
        let mut rounds = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: ChallengeRoundKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("round key: {e}")))?;
            let certified: bool = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("round outcome: {e}")))?;
            rounds.push((key.epoch, key.round, certified));
        }
        Ok(rounds)
    }

    fn prune_rounds_before(&self, epoch: EpochNumber) -> Result<usize> {
        let raw = self.inner().inner();
        let mut batch = store::WriteBatch::new();
        let mut dropped = 0usize;

        for key_bytes in raw.iter_keys_prefix(ChallengeRoundCol::CF_NAME, &[])? {
            let key: ChallengeRoundKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("round key: {e}")))?;
            if key.epoch < epoch {
                batch.delete_owned(ChallengeRoundCol::CF_NAME, key_bytes);
                dropped += 1;
            }
        }

        if dropped > 0 {
            raw.write_batch(batch)?;
        }
        Ok(dropped)
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    // never challenged is not never answered, so a peer nobody has asked reads
    // as clean rather than as one that failed
    #[test]
    fn unknown_peer() {
        let store = test_store();
        let record = store.peer_record(Address::new_unique()).unwrap();

        assert_eq!(record, PeerRecord::default());
        assert!(!record.eviction_fires());
    }

    // the key orders by epoch then round, so a peer's history comes back in the
    // order it happened and one peer never picks up another's
    #[test]
    fn rounds_ordered() {
        let store = test_store();
        let peer = Address::new_unique();
        let other = Address::new_unique();

        for (epoch, round, certified) in [
            (2u64, 9u64, true),
            (3, 0, false),
            (2, 1, true),
            (3, 1, false),
        ] {
            store
                .put_round_outcome(peer, EpochNumber(epoch), RoundNumber(round), certified)
                .unwrap();
        }
        store
            .put_round_outcome(other, EpochNumber(2), RoundNumber(0), false)
            .unwrap();

        assert_eq!(
            store.peer_rounds(peer).unwrap(),
            vec![
                (EpochNumber(2), RoundNumber(1), true),
                (EpochNumber(2), RoundNumber(9), true),
                (EpochNumber(3), RoundNumber(0), false),
                (EpochNumber(3), RoundNumber(1), false),
            ]
        );

        // One peer's history never picks up another's.
        assert_eq!(store.peer_rounds(other).unwrap().len(), 1);
    }

    // an outcome reads back and a late certificate overwrites the miss it
    // supersedes in place
    #[test]
    fn outcome_replaced() {
        let store = test_store();
        let peer = Address::new_unique();
        let (epoch, round) = (EpochNumber(4), RoundNumber(7));

        assert_eq!(store.round_outcome(peer, epoch, round).unwrap(), None);

        store.put_round_outcome(peer, epoch, round, false).unwrap();
        assert_eq!(store.round_outcome(peer, epoch, round).unwrap(), Some(false));

        store.put_round_outcome(peer, epoch, round, true).unwrap();
        assert_eq!(store.round_outcome(peer, epoch, round).unwrap(), Some(true));
        assert_eq!(store.peer_rounds(peer).unwrap().len(), 1);
    }

    // the stored rounds name which ones a node failed, which is the report an
    // operator wants and the counters cannot give
    #[test]
    fn failed_rounds() {
        let store = test_store();
        let peer = Address::new_unique();
        for round in 0..6u64 {
            store
                .put_round_outcome(peer, EpochNumber(4), RoundNumber(round), round % 3 != 0)
                .unwrap();
        }

        let failed: Vec<u64> = store
            .peer_rounds(peer)
            .unwrap()
            .into_iter()
            .filter(|(_, _, certified)| !certified)
            .map(|(_, round, _)| round.0)
            .collect();
        assert_eq!(failed, vec![0, 3]);
    }

    // pruning drops the epochs before the cutoff and keeps the rest
    #[test]
    fn pruning_epochs() {
        let store = test_store();
        let peer = Address::new_unique();
        for epoch in 1..=4u64 {
            store
                .put_round_outcome(peer, EpochNumber(epoch), RoundNumber(0), true)
                .unwrap();
        }

        assert_eq!(store.prune_rounds_before(EpochNumber(3)).unwrap(), 2);
        let left: Vec<u64> = store
            .peer_rounds(peer)
            .unwrap()
            .into_iter()
            .map(|(epoch, _, _)| epoch.0)
            .collect();
        assert_eq!(left, vec![3, 4]);
    }

    // a record writes, reads, lists and deletes
    #[test]
    fn record_round_trip() {
        let store = test_store();
        let peer = Address::new_unique();

        let mut record = PeerRecord::default();
        record.record(EpochNumber(4), RoundNumber(9), false, None);
        store.put_peer_record(peer, record).unwrap();

        assert_eq!(store.peer_record(peer).unwrap(), record);
        assert_eq!(store.iter_peer_records().unwrap(), vec![(peer, record)]);

        store.delete_peer_record(peer).unwrap();
        assert_eq!(store.peer_record(peer).unwrap(), PeerRecord::default());
    }
}
