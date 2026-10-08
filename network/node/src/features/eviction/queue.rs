//! Nodes with an open eviction vote this node joins, scoped to one voting epoch.
//!
//! An entry is dropped when the eviction lands or its voting epoch passes.

use std::collections::HashMap;
use std::sync::Mutex;

use tape_core::types::EpochNumber;
use tape_crypto::Address;

#[derive(Default)]
pub struct EvictionQueue {
    // The voting epoch each target was opened in.
    targets: Mutex<HashMap<Address, EpochNumber>>,
}

impl EvictionQueue {
    pub fn insert(&self, node: Address, epoch: EpochNumber) {
        self.lock().insert(node, epoch);
    }

    pub fn remove(&self, node: &Address) {
        self.lock().remove(node);
    }

    /// Drop every target whose voting epoch has passed.
    pub fn retain_epoch(&self, epoch: EpochNumber) {
        self.lock().retain(|_, opened| *opened >= epoch);
    }

    pub fn snapshot(&self) -> Vec<Address> {
        self.lock().keys().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Address, EpochNumber>> {
        self.targets.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
