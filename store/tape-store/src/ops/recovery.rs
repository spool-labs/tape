//! Recovery queue operations
//!
//! Manages the pending recovery queue for slices that failed to sync
//! from previous owners and need erasure recovery from the committee.

use crate::columns::PendingRecover;
use crate::error::{Result, TapeStoreError};
use crate::types::{Pubkey, SliceKey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::{Column, Store};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Recovery info for slices that need to be fetched
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct RecoveryInfo {
    /// Node to fetch the slice from
    pub source_node: Pubkey,
    /// Number of retry attempts
    pub attempts: u8,
    /// Timestamp of last attempt (for backoff)
    pub last_attempt: i64,
}

/// High-level operations for recovery queue management
pub trait RecoveryOps {
    /// Queue a slice for erasure recovery
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    /// * `info` - Recovery metadata (source hint, attempts, timestamp)
    fn queue_recovery(&self, spool_idx: u16, track_address: Pubkey, info: RecoveryInfo)
        -> Result<()>;

    /// Remove a slice from the recovery queue (on success or permanent failure)
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    fn dequeue_recovery(&self, spool_idx: u16, track_address: Pubkey) -> Result<()>;

    /// Get recovery info for a specific slice
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    ///
    /// # Returns
    /// The recovery info if queued
    fn get_recovery_info(&self, spool_idx: u16, track_address: Pubkey)
        -> Result<Option<RecoveryInfo>>;

    /// Get all pending recoveries for a specific spool
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    ///
    /// # Returns
    /// Vector of (track_address, recovery_info) pairs
    fn get_spool_recoveries(&self, spool_idx: u16) -> Result<Vec<(Pubkey, RecoveryInfo)>>;

    /// Get all pending recoveries across all spools
    ///
    /// # Returns
    /// Vector of (spool_idx, track_address, recovery_info) tuples
    fn get_all_recoveries(&self) -> Result<Vec<(u16, Pubkey, RecoveryInfo)>>;

    /// Update recovery attempt metadata (for exponential backoff)
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    /// * `timestamp` - Unix timestamp of this attempt
    fn update_recovery_attempt(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        timestamp: i64,
    ) -> Result<()>;

    /// Get count of pending recoveries
    fn recovery_queue_len(&self) -> Result<usize>;
}

impl<S: Store> RecoveryOps for TapeStore<S> {
    fn queue_recovery(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        info: RecoveryInfo,
    ) -> Result<()> {
        let key = SliceKey::new(spool_idx, track_address);
        self.put::<PendingRecover>(&key, &info)?;
        Ok(())
    }

    fn dequeue_recovery(&self, spool_idx: u16, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_idx, track_address);
        self.delete::<PendingRecover>(&key)?;
        Ok(())
    }

    fn get_recovery_info(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
    ) -> Result<Option<RecoveryInfo>> {
        let key = SliceKey::new(spool_idx, track_address);
        let info = self.get::<PendingRecover>(&key)?;
        Ok(info)
    }

    fn get_spool_recoveries(&self, spool_idx: u16) -> Result<Vec<(Pubkey, RecoveryInfo)>> {
        // Create prefix bytes for the spool_idx (2 bytes BE)
        let prefix_bytes = spool_idx.to_be_bytes();

        // Iterate with prefix to get all recoveries for this spool
        let iter = self
            .inner()
            .inner()
            .iter_prefix(PendingRecover::CF_NAME, &prefix_bytes)?;

        let mut recoveries = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("recovery key: {}", e)))?;

            // Verify spool_idx matches (should be guaranteed by prefix)
            if key.spool_idx == spool_idx {
                let info: RecoveryInfo = wincode::deserialize(&value_bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("recovery info: {}", e)))?;
                recoveries.push((key.track_address, info));
            }
        }

        Ok(recoveries)
    }

    fn get_all_recoveries(&self) -> Result<Vec<(u16, Pubkey, RecoveryInfo)>> {
        let entries = self.iter::<PendingRecover>()?;
        let recoveries: Vec<(u16, Pubkey, RecoveryInfo)> = entries
            .into_iter()
            .map(|(key, info)| (key.spool_idx, key.track_address, info))
            .collect();
        Ok(recoveries)
    }

    fn update_recovery_attempt(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        timestamp: i64,
    ) -> Result<()> {
        let key = SliceKey::new(spool_idx, track_address);

        // Get existing info
        let mut info = self
            .get::<PendingRecover>(&key)?
            .ok_or(TapeStoreError::RecoveryNotFound(spool_idx, track_address))?;

        // Update attempt tracking
        info.attempts = info.attempts.saturating_add(1);
        info.last_attempt = timestamp;

        // Write back
        self.put::<PendingRecover>(&key, &info)?;
        Ok(())
    }

    fn recovery_queue_len(&self) -> Result<usize> {
        let entries = self.iter::<PendingRecover>()?;
        Ok(entries.len())
    }
}

/// Calculate backoff delay based on attempt count
///
/// Uses exponential backoff: 2^attempts seconds, capped at 1 hour
pub fn backoff_delay_secs(attempts: u8) -> i64 {
    let base_delay = 1i64 << attempts.min(12); // 2^attempts, max 4096
    base_delay.min(3600) // Cap at 1 hour
}

/// Check if enough time has passed since last attempt
pub fn is_ready_for_retry(info: &RecoveryInfo, now: i64) -> bool {
    let delay = backoff_delay_secs(info.attempts);
    now >= info.last_attempt + delay
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn create_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn queue_and_dequeue_recovery() {
        let store = create_store();
        let track = Pubkey::new([1u8; 32]);
        let info = RecoveryInfo {
            source_node: Pubkey::new([2u8; 32]),
            attempts: 0,
            last_attempt: 0,
        };

        // Queue recovery
        store.queue_recovery(42, track, info.clone()).unwrap();

        // Verify it's there
        let retrieved = store.get_recovery_info(42, track).unwrap();
        assert_eq!(retrieved, Some(info));

        // Dequeue
        store.dequeue_recovery(42, track).unwrap();

        // Verify it's gone
        let retrieved = store.get_recovery_info(42, track).unwrap();
        assert_eq!(retrieved, None);
    }

    #[test]
    fn get_spool_recoveries() {
        let store = create_store();

        // Queue multiple recoveries for spool 42
        for i in 0..5 {
            let track = Pubkey::new([i; 32]);
            let info = RecoveryInfo {
                source_node: Pubkey::new([100 + i; 32]),
                attempts: i,
                last_attempt: i as i64 * 1000,
            };
            store.queue_recovery(42, track, info).unwrap();
        }

        // Queue some for other spools
        store
            .queue_recovery(
                10,
                Pubkey::new([200u8; 32]),
                RecoveryInfo {
                    source_node: Pubkey::default(),
                    attempts: 0,
                    last_attempt: 0,
                },
            )
            .unwrap();

        // Get recoveries for spool 42
        let recoveries = store.get_spool_recoveries(42).unwrap();
        assert_eq!(recoveries.len(), 5);

        // Get recoveries for spool 10
        let recoveries = store.get_spool_recoveries(10).unwrap();
        assert_eq!(recoveries.len(), 1);
    }

    #[test]
    fn get_all_recoveries() {
        let store = create_store();

        // Queue recoveries across multiple spools
        for spool in [10, 42, 100] {
            for i in 0..3 {
                let track = Pubkey::new([spool as u8 + i; 32]);
                let info = RecoveryInfo {
                    source_node: Pubkey::default(),
                    attempts: 0,
                    last_attempt: 0,
                };
                store.queue_recovery(spool, track, info).unwrap();
            }
        }

        let all = store.get_all_recoveries().unwrap();
        assert_eq!(all.len(), 9);
    }

    #[test]
    fn update_recovery_attempt() {
        let store = create_store();
        let track = Pubkey::new([1u8; 32]);
        let info = RecoveryInfo {
            source_node: Pubkey::default(),
            attempts: 0,
            last_attempt: 0,
        };

        store.queue_recovery(42, track, info).unwrap();

        // Update attempt
        store.update_recovery_attempt(42, track, 1000).unwrap();

        let updated = store.get_recovery_info(42, track).unwrap().unwrap();
        assert_eq!(updated.attempts, 1);
        assert_eq!(updated.last_attempt, 1000);

        // Update again
        store.update_recovery_attempt(42, track, 2000).unwrap();

        let updated = store.get_recovery_info(42, track).unwrap().unwrap();
        assert_eq!(updated.attempts, 2);
        assert_eq!(updated.last_attempt, 2000);
    }

    #[test]
    fn backoff_calculation() {
        assert_eq!(backoff_delay_secs(0), 1);
        assert_eq!(backoff_delay_secs(1), 2);
        assert_eq!(backoff_delay_secs(2), 4);
        assert_eq!(backoff_delay_secs(5), 32);
        assert_eq!(backoff_delay_secs(10), 1024);
        assert_eq!(backoff_delay_secs(12), 3600); // Capped at 1 hour
        assert_eq!(backoff_delay_secs(20), 3600); // Still capped
    }

    #[test]
    fn retry_readiness() {
        let info = RecoveryInfo {
            source_node: Pubkey::default(),
            attempts: 2,
            last_attempt: 1000,
        };

        // With 2 attempts, backoff is 4 seconds
        assert!(!is_ready_for_retry(&info, 1003)); // Not yet
        assert!(is_ready_for_retry(&info, 1004)); // Ready
        assert!(is_ready_for_retry(&info, 2000)); // Well past ready
    }

    #[test]
    fn recovery_queue_len() {
        let store = create_store();

        assert_eq!(store.recovery_queue_len().unwrap(), 0);

        for i in 0..10 {
            let track = Pubkey::new([i; 32]);
            let info = RecoveryInfo {
                source_node: Pubkey::default(),
                attempts: 0,
                last_attempt: 0,
            };
            store.queue_recovery(i as u16, track, info).unwrap();
        }

        assert_eq!(store.recovery_queue_len().unwrap(), 10);
    }
}
