//! Spool management operations
//!
//! Tracks spools assigned to this node and their state.

use crate::columns::*;
use crate::error::Result;
use crate::types::{EpochNumber, Pubkey, SpoolKey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::Store;
use wincode_derive::{SchemaRead, SchemaWrite};

/// State of an assigned spool
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SpoolState {
    /// Current status of this spool
    pub status: SpoolStatus,
    /// Epoch when this spool was assigned to us
    pub assigned_epoch: EpochNumber,
    /// Last track synced (for resumption during epoch transitions)
    pub sync_cursor: Option<Pubkey>,
}

impl Default for SpoolState {
    fn default() -> Self {
        Self {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(0),
            sync_cursor: None,
        }
    }
}

/// Status of a spool in its lifecycle
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SpoolStatus {
    /// Normal operation - accepting and serving slices
    Active = 0,
    /// Receiving slices from previous owner
    Syncing = 1,
    /// Sending slices to next owner
    Handoff = 2,
}

/// High-level operations for spool management
pub trait SpoolOps {
    /// Store spool state
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `state` - The spool state to store
    fn put_spool_state(&self, spool_idx: u16, state: SpoolState) -> Result<()>;

    /// Get spool state
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    ///
    /// # Returns
    /// The spool state if found
    fn get_spool_state(&self, spool_idx: u16) -> Result<Option<SpoolState>>;

    /// Get all spools assigned to this node
    ///
    /// # Returns
    /// Vector of spool indices we own
    fn get_my_spools(&self) -> Result<Vec<u16>>;
}

impl<S: Store> SpoolOps for TapeStore<S> {
    fn put_spool_state(&self, spool_idx: u16, state: SpoolState) -> Result<()> {
        self.put::<SpoolsAssigned>(&SpoolKey(spool_idx), &state)?;
        Ok(())
    }

    fn get_spool_state(&self, spool_idx: u16) -> Result<Option<SpoolState>> {
        let state = self.get::<SpoolsAssigned>(&SpoolKey(spool_idx))?;
        Ok(state)
    }

    fn get_my_spools(&self) -> Result<Vec<u16>> {
        let entries = self.iter::<SpoolsAssigned>()?;
        let spools: Vec<u16> = entries.into_iter().map(|(key, _state)| key.0).collect();
        Ok(spools)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    #[test]
    fn put_and_get_spool_state() {
        let store = TapeStore::new(MemoryStore::new());
        let state = SpoolState {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };

        store.put_spool_state(42, state.clone()).unwrap();
        let retrieved = store.get_spool_state(42).unwrap();
        assert_eq!(retrieved, Some(state));
    }

    #[test]
    fn get_my_spools() {
        let store = TapeStore::new(MemoryStore::new());

        // Add some spools
        for spool_idx in [10, 42, 100, 500] {
            let state = SpoolState {
                status: SpoolStatus::Active,
                assigned_epoch: EpochNumber(100),
                sync_cursor: None,
            };
            store.put_spool_state(spool_idx, state).unwrap();
        }

        let spools = store.get_my_spools().unwrap();
        assert_eq!(spools.len(), 4);

        // Spools should be returned in sorted order (due to BE encoding)
        assert_eq!(spools, vec![10, 42, 100, 500]);
    }

    #[test]
    fn spool_status_transitions() {
        let store = TapeStore::new(MemoryStore::new());

        // Start as syncing
        let mut state = SpoolState {
            status: SpoolStatus::Syncing,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };
        store.put_spool_state(42, state.clone()).unwrap();

        // Transition to active
        state.status = SpoolStatus::Active;
        store.put_spool_state(42, state.clone()).unwrap();

        let retrieved = store.get_spool_state(42).unwrap().unwrap();
        assert_eq!(retrieved.status, SpoolStatus::Active);

        // Transition to handoff
        state.status = SpoolStatus::Handoff;
        store.put_spool_state(42, state).unwrap();

        let retrieved = store.get_spool_state(42).unwrap().unwrap();
        assert_eq!(retrieved.status, SpoolStatus::Handoff);
    }

    #[test]
    fn sync_cursor_tracking() {
        let store = TapeStore::new(MemoryStore::new());
        let last_track = Pubkey::new_unique();

        let state = SpoolState {
            status: SpoolStatus::Syncing,
            assigned_epoch: EpochNumber(100),
            sync_cursor: Some(last_track),
        };

        store.put_spool_state(42, state).unwrap();

        let retrieved = store.get_spool_state(42).unwrap().unwrap();
        assert_eq!(retrieved.sync_cursor, Some(last_track));
    }
}
