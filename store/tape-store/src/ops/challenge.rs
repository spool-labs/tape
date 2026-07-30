//! Per-peer challenge history

use store::Store;
use tape_core::challenge::PeerRecord;
use tape_crypto::address::Address;

use crate::columns::ChallengeRecordCol;
use crate::error::Result;
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
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::types::{EpochNumber, RoundNumber};

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn an_unknown_peer_reads_as_a_clean_record() {
        // Never challenged is not the same as never answered, so a peer nobody
        // has asked must not read as a peer that failed.
        let store = test_store();
        let record = store.peer_record(Address::new_unique()).unwrap();

        assert_eq!(record, PeerRecord::default());
        assert!(!record.eviction_fires());
    }

    #[test]
    fn a_record_survives_a_round_trip() {
        let store = test_store();
        let peer = Address::new_unique();

        let mut record = PeerRecord::default();
        record.record(EpochNumber(4), RoundNumber(9), false);
        store.put_peer_record(peer, record).unwrap();

        assert_eq!(store.peer_record(peer).unwrap(), record);
        assert_eq!(store.iter_peer_records().unwrap(), vec![(peer, record)]);

        store.delete_peer_record(peer).unwrap();
        assert_eq!(store.peer_record(peer).unwrap(), PeerRecord::default());
    }
}
