//! Committee management operations

use crate::columns::CommitteeCol;
use crate::error::Result;
use crate::types::{EpochKey, EpochNumber, NodeInfo};
use crate::TapeStore;
use store::Store;

/// High-level operations for committee management
pub trait CommitteeOps {
    /// Store committee for an epoch
    fn put_committee(&self, epoch: EpochNumber, members: Vec<NodeInfo>) -> Result<()>;

    /// Get committee for a specific epoch
    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<Vec<NodeInfo>>>;

    /// Delete committee for a specific epoch
    fn delete_committee(&self, epoch: EpochNumber) -> Result<()>;
}

impl<S: Store> CommitteeOps for TapeStore<S> {
    fn put_committee(&self, epoch: EpochNumber, members: Vec<NodeInfo>) -> Result<()> {
        let key = EpochKey::new(epoch.as_u64());
        self.put::<CommitteeCol>(&key, &members)?;
        Ok(())
    }

    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<Vec<NodeInfo>>> {
        let key = EpochKey::new(epoch.as_u64());
        Ok(self.get::<CommitteeCol>(&key)?)
    }

    fn delete_committee(&self, epoch: EpochNumber) -> Result<()> {
        let key = EpochKey::new(epoch.as_u64());
        self.delete::<CommitteeCol>(&key)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Pubkey;
    use bytemuck::Zeroable;
    use store_memory::MemoryStore;
    use tape_core::bls::BlsPubkey;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::NodeId;

    fn create_test_member(id: u8) -> NodeInfo {
        NodeInfo {
            node_id: NodeId(id as u64),
            node_address: Pubkey::new([id; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: Pubkey::new([id + 100; 32]),
            network_address: NetworkAddress::new_ipv4([192, 168, 1, id], 8080),
            spools: vec![id as u16 * 10, id as u16 * 10 + 1],
        }
    }

    #[test]
    fn test_put_and_get_committee() {
        let store = TapeStore::new(MemoryStore::new());
        let members = vec![create_test_member(1), create_test_member(2)];

        store
            .put_committee(EpochNumber(100), members.clone())
            .unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap();
        assert_eq!(retrieved, Some(members));
    }

    #[test]
    fn test_committee_not_found() {
        let store = TapeStore::new(MemoryStore::new());
        let result = store.get_committee(EpochNumber(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_committee() {
        let store = TapeStore::new(MemoryStore::new());
        let members = vec![create_test_member(1)];

        store
            .put_committee(EpochNumber(100), members)
            .unwrap();
        assert!(store.get_committee(EpochNumber(100)).unwrap().is_some());

        store.delete_committee(EpochNumber(100)).unwrap();
        assert!(store.get_committee(EpochNumber(100)).unwrap().is_none());
    }
}
