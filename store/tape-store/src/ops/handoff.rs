//! Handoff queue operations
//!
//! Manages the pending handoff queue for slices that need to be sent
//! to new owners during spool reassignment.

use crate::columns::PendingHandoff;
use crate::error::{Result, TapeStoreError};
use crate::types::{Pubkey, SliceKey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::{Column, Store};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Handoff info for slices that need to be sent
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct HandoffInfo {
    /// Node to send the slice to
    pub target_node: Pubkey,
    /// Number of retry attempts
    pub attempts: u8,
    /// Timestamp of last attempt (for backoff)
    pub last_attempt: i64,
}

/// High-level operations for handoff queue management
pub trait HandoffOps {
    /// Queue a slice for handoff to a new owner
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    /// * `info` - Handoff metadata (target node, attempts, timestamp)
    fn queue_handoff(&self, spool_idx: u16, track_address: Pubkey, info: HandoffInfo) -> Result<()>;

    /// Remove a slice from the handoff queue (on success or permanent failure)
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    fn dequeue_handoff(&self, spool_idx: u16, track_address: Pubkey) -> Result<()>;

    /// Get handoff info for a specific slice
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    ///
    /// # Returns
    /// The handoff info if queued
    fn get_handoff_info(&self, spool_idx: u16, track_address: Pubkey) -> Result<Option<HandoffInfo>>;

    /// Get all pending handoffs for a specific spool
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    ///
    /// # Returns
    /// Vector of (track_address, handoff_info) pairs
    fn get_spool_handoffs(&self, spool_idx: u16) -> Result<Vec<(Pubkey, HandoffInfo)>>;

    /// Get all pending handoffs across all spools
    ///
    /// # Returns
    /// Vector of (spool_idx, track_address, handoff_info) tuples
    fn get_all_handoffs(&self) -> Result<Vec<(u16, Pubkey, HandoffInfo)>>;

    /// Update handoff attempt metadata (for exponential backoff)
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    /// * `timestamp` - Unix timestamp of this attempt
    fn update_handoff_attempt(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        timestamp: i64,
    ) -> Result<()>;

    /// Get count of pending handoffs
    fn handoff_queue_len(&self) -> Result<usize>;
}

impl<S: Store> HandoffOps for TapeStore<S> {
    fn queue_handoff(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        info: HandoffInfo,
    ) -> Result<()> {
        let key = SliceKey::new(spool_idx, track_address);
        self.put::<PendingHandoff>(&key, &info)?;
        Ok(())
    }

    fn dequeue_handoff(&self, spool_idx: u16, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_idx, track_address);
        self.delete::<PendingHandoff>(&key)?;
        Ok(())
    }

    fn get_handoff_info(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
    ) -> Result<Option<HandoffInfo>> {
        let key = SliceKey::new(spool_idx, track_address);
        let info = self.get::<PendingHandoff>(&key)?;
        Ok(info)
    }

    fn get_spool_handoffs(&self, spool_idx: u16) -> Result<Vec<(Pubkey, HandoffInfo)>> {
        // Create prefix bytes for the spool_idx (2 bytes BE)
        let prefix_bytes = spool_idx.to_be_bytes();

        // Iterate with prefix to get all handoffs for this spool
        let iter = self
            .inner()
            .inner()
            .iter_prefix(PendingHandoff::CF_NAME, &prefix_bytes)?;

        let mut handoffs = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("handoff key: {}", e)))?;

            // Verify spool_idx matches (should be guaranteed by prefix)
            if key.spool_idx == spool_idx {
                let info: HandoffInfo = wincode::deserialize(&value_bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("handoff info: {}", e)))?;
                handoffs.push((key.track_address, info));
            }
        }

        Ok(handoffs)
    }

    fn get_all_handoffs(&self) -> Result<Vec<(u16, Pubkey, HandoffInfo)>> {
        let entries = self.iter::<PendingHandoff>()?;
        let handoffs: Vec<(u16, Pubkey, HandoffInfo)> = entries
            .into_iter()
            .map(|(key, info)| (key.spool_idx, key.track_address, info))
            .collect();
        Ok(handoffs)
    }

    fn update_handoff_attempt(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        timestamp: i64,
    ) -> Result<()> {
        let key = SliceKey::new(spool_idx, track_address);

        // Get existing info
        let mut info = self
            .get::<PendingHandoff>(&key)?
            .ok_or(TapeStoreError::HandoffNotFound(spool_idx, track_address))?;

        // Update attempt tracking
        info.attempts = info.attempts.saturating_add(1);
        info.last_attempt = timestamp;

        // Write back
        self.put::<PendingHandoff>(&key, &info)?;
        Ok(())
    }

    fn handoff_queue_len(&self) -> Result<usize> {
        let entries = self.iter::<PendingHandoff>()?;
        Ok(entries.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn create_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn queue_and_dequeue_handoff() {
        let store = create_store();
        let track = Pubkey::new([1u8; 32]);
        let info = HandoffInfo {
            target_node: Pubkey::new([2u8; 32]),
            attempts: 0,
            last_attempt: 0,
        };

        // Queue handoff
        store.queue_handoff(42, track, info.clone()).unwrap();

        // Verify it's there
        let retrieved = store.get_handoff_info(42, track).unwrap();
        assert_eq!(retrieved, Some(info));

        // Dequeue
        store.dequeue_handoff(42, track).unwrap();

        // Verify it's gone
        let retrieved = store.get_handoff_info(42, track).unwrap();
        assert_eq!(retrieved, None);
    }

    #[test]
    fn get_spool_handoffs() {
        let store = create_store();

        // Queue multiple handoffs for spool 42
        for i in 0..5 {
            let track = Pubkey::new([i; 32]);
            let info = HandoffInfo {
                target_node: Pubkey::new([100 + i; 32]),
                attempts: i,
                last_attempt: i as i64 * 1000,
            };
            store.queue_handoff(42, track, info).unwrap();
        }

        // Queue some for other spools
        store
            .queue_handoff(
                10,
                Pubkey::new([200u8; 32]),
                HandoffInfo {
                    target_node: Pubkey::default(),
                    attempts: 0,
                    last_attempt: 0,
                },
            )
            .unwrap();

        // Get handoffs for spool 42
        let handoffs = store.get_spool_handoffs(42).unwrap();
        assert_eq!(handoffs.len(), 5);

        // Get handoffs for spool 10
        let handoffs = store.get_spool_handoffs(10).unwrap();
        assert_eq!(handoffs.len(), 1);
    }

    #[test]
    fn get_all_handoffs() {
        let store = create_store();

        // Queue handoffs across multiple spools
        for spool in [10, 42, 100] {
            for i in 0..3 {
                let track = Pubkey::new([spool as u8 + i; 32]);
                let info = HandoffInfo {
                    target_node: Pubkey::default(),
                    attempts: 0,
                    last_attempt: 0,
                };
                store.queue_handoff(spool, track, info).unwrap();
            }
        }

        let all = store.get_all_handoffs().unwrap();
        assert_eq!(all.len(), 9);
    }

    #[test]
    fn update_handoff_attempt() {
        let store = create_store();
        let track = Pubkey::new([1u8; 32]);
        let info = HandoffInfo {
            target_node: Pubkey::default(),
            attempts: 0,
            last_attempt: 0,
        };

        store.queue_handoff(42, track, info).unwrap();

        // Update attempt
        store.update_handoff_attempt(42, track, 1000).unwrap();

        let updated = store.get_handoff_info(42, track).unwrap().unwrap();
        assert_eq!(updated.attempts, 1);
        assert_eq!(updated.last_attempt, 1000);

        // Update again
        store.update_handoff_attempt(42, track, 2000).unwrap();

        let updated = store.get_handoff_info(42, track).unwrap().unwrap();
        assert_eq!(updated.attempts, 2);
        assert_eq!(updated.last_attempt, 2000);
    }

    #[test]
    fn handoff_queue_len() {
        let store = create_store();

        assert_eq!(store.handoff_queue_len().unwrap(), 0);

        for i in 0..10 {
            let track = Pubkey::new([i; 32]);
            let info = HandoffInfo {
                target_node: Pubkey::default(),
                attempts: 0,
                last_attempt: 0,
            };
            store.queue_handoff(i as u16, track, info).unwrap();
        }

        assert_eq!(store.handoff_queue_len().unwrap(), 10);
    }
}
