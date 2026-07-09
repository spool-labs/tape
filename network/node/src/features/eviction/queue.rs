//! Admin-seeded set of nodes this node should vote to evict.
//!
//! The eviction manager reads it each round and drops a target once the node
//! leaves the next committee (the eviction landed or the epoch advanced past
//! it). The queue is the trigger surface for the admin HTTP endpoint.

use std::collections::HashSet;
use std::sync::Mutex;

use tape_crypto::Address;

#[derive(Default)]
pub struct EvictionQueue {
    targets: Mutex<HashSet<Address>>,
}

impl EvictionQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, node: Address) {
        self.lock().insert(node);
    }

    pub fn remove(&self, node: &Address) {
        self.lock().remove(node);
    }

    pub fn snapshot(&self) -> Vec<Address> {
        self.lock().iter().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<Address>> {
        self.targets.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
