//! Set of nodes with an open eviction vote this node may join.
//!
//! Targets are added when an on-chain eviction proposal is observed. The
//! eviction manager probes each target itself and only votes while its own
//! probe fails; a target is dropped once the eviction lands or the target
//! probes healthy again.

use std::collections::HashSet;
use std::sync::Mutex;

use tape_crypto::Address;

#[derive(Default)]
pub struct EvictionQueue {
    targets: Mutex<HashSet<Address>>,
}

impl EvictionQueue {
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
