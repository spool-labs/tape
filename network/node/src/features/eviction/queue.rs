//! Nodes with an open eviction vote this node joins, scoped to one voting epoch.
//!
//! Two things open a vote and they are not judged the same. A proposal is a
//! decision its proposer has already taken, so members sign it and what holds
//! the eviction back is the group supermajority and the supermajority of groups
//! it then needs. A record is this node's own reading of a peer, and the run arm
//! it fires on claims the peer stopped answering, which a live probe can settle.
//!
//! An entry is dropped when the eviction lands, and otherwise when its voting
//! epoch passes. A proposal expires with that epoch, so a target left behind
//! would be re-proposed every epoch for the rest of the node's life. One that
//! still deserves the vote is queued again by the next proposal, or by the next
//! round its record fails.

use std::collections::HashMap;
use std::sync::Mutex;

use tape_core::types::EpochNumber;
use tape_crypto::Address;

/// What opened the vote on a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Opened {
    /// An eviction proposal observed on chain.
    Proposal,
    /// This node's own challenge record firing its rule.
    Record,
}

#[derive(Default)]
pub struct EvictionQueue {
    // The voting epoch each target was opened in, and what opened it.
    targets: Mutex<HashMap<Address, (EpochNumber, Opened)>>,
}

impl EvictionQueue {
    pub fn insert(&self, node: Address, epoch: EpochNumber, opened: Opened) {
        self.lock().insert(node, (epoch, opened));
    }

    pub fn remove(&self, node: &Address) {
        self.lock().remove(node);
    }

    /// Drop every target whose voting epoch has passed.
    pub fn retain_epoch(&self, epoch: EpochNumber) {
        self.lock().retain(|_, (opened, _)| *opened >= epoch);
    }

    pub fn snapshot(&self) -> Vec<(Address, Opened)> {
        self.lock()
            .iter()
            .map(|(node, (_, opened))| (*node, *opened))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Address, (EpochNumber, Opened)>> {
        self.targets.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
