//! ProtocolState — cached view of on-chain protocol state.

use std::sync::Arc;

use arc_swap::ArcSwap;
use rpc::{Rpc, RpcError};
use tape_api::program::MEMBER_COUNT;
use tape_api::state::System;
use tape_core::erasure::SPOOL_COUNT;
use tape_core::spooler::{SpoolAssignment, SpoolIndex};
use tape_core::system::{Committee, CommitteeMember, EpochPhase};
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::Hash;

use crate::RpcClient;

/// Snapshot of on-chain protocol state.
///
/// Contains committee membership, spool assignments, and epoch info.
/// Produced by `RpcClient::fetch_state()`. Does not include network
/// addresses — those live in `TrustedPeers` (peer crate).
#[derive(Debug, Clone)]
pub struct ProtocolState {
    pub epoch: EpochNumber,
    pub phase: EpochPhase,
    pub nonce: Hash,
    pub committee: Vec<CommitteeMember>,
    pub committee_prev: Vec<CommitteeMember>,
    pub spools: SpoolAssignment<SPOOL_COUNT>,
    pub spools_prev: SpoolAssignment<SPOOL_COUNT>,
}

impl Default for ProtocolState {
    fn default() -> Self {
        Self {
            epoch: EpochNumber(0),
            phase: EpochPhase::Active,
            nonce: Hash::default(),
            committee: Vec::new(),
            committee_prev: Vec::new(),
            spools: bytemuck::Zeroable::zeroed(),
            spools_prev: bytemuck::Zeroable::zeroed(),
        }
    }
}

impl ProtocolState {
    /// Which node owns this spool in the current committee?
    pub fn spool_owner(&self, spool: SpoolIndex) -> Option<NodeId> {
        let mapping = self.spools.0.get(spool as usize)?;
        let member_index = *mapping as usize;
        self.committee.get(member_index).map(|m| m.id)
    }

    /// Which node owned this spool in the previous committee?
    pub fn spool_owner_prev(&self, spool: SpoolIndex) -> Option<NodeId> {
        let mapping = self.spools_prev.0.get(spool as usize)?;
        let member_index = *mapping as usize;
        self.committee_prev.get(member_index).map(|m| m.id)
    }

    /// All spools assigned to a member (by index in current committee).
    pub fn member_spools(&self, member_index: usize) -> Vec<SpoolIndex> {
        self.spools.spools_for_member(member_index)
    }

    /// Find a member in the current committee by NodeId.
    /// Returns (member_index, &CommitteeMember).
    pub fn find_member(&self, node_id: NodeId) -> Option<(usize, &CommitteeMember)> {
        self.committee
            .iter()
            .enumerate()
            .find(|(_, m)| m.id == node_id)
    }

    /// Build a fixed-size Committee array from the current committee Vec.
    pub fn committee_as_array(&self) -> Committee<MEMBER_COUNT> {
        let mut committee = Committee::new();
        for member in &self.committee {
            let _ = committee.try_join(member);
        }
        committee
    }
}

/// Thread-safe handle for sharing a `ProtocolState` across tasks.
///
/// Uses `ArcSwap` for lock-free reads with atomic updates.
#[derive(Clone)]
pub struct StateCache {
    inner: Arc<ArcSwap<ProtocolState>>,
}

impl StateCache {
    /// Create a new cache seeded with the given initial state.
    pub fn new(initial: ProtocolState) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Load the current state (lock-free).
    pub fn load(&self) -> arc_swap::Guard<Arc<ProtocolState>> {
        self.inner.load()
    }

    /// Replace the cached state atomically.
    pub fn store(&self, state: ProtocolState) {
        self.inner.store(Arc::new(state));
    }

    /// Update just the epoch phase (read-modify-write).
    pub fn update_phase(&self, phase: EpochPhase) {
        let current = self.inner.load();
        let mut updated = (**current).clone();
        updated.phase = phase;
        self.inner.store(Arc::new(updated));
    }
}

fn build_state(system: &System, epoch_id: EpochNumber, phase: EpochPhase, nonce: Hash) -> ProtocolState {
    let committee: Vec<CommitteeMember> = system.committee.iter().cloned().collect();
    let committee_prev: Vec<CommitteeMember> = if epoch_id.0 > 0 && system.committee_prev.size() > 0 {
        system.committee_prev.iter().cloned().collect()
    } else {
        Vec::new()
    };

    ProtocolState {
        epoch: epoch_id,
        phase,
        nonce,
        committee,
        committee_prev,
        spools: system.spools,
        spools_prev: system.spools_prev,
    }
}

impl<R: Rpc> RpcClient<R> {
    /// Fetch current protocol state from on-chain accounts.
    ///
    /// Makes 2 RPC calls: `get_system()` + `get_epoch()`.
    /// Does NOT fetch individual Node accounts (network addresses).
    pub async fn fetch_state(&self) -> Result<ProtocolState, RpcError> {
        let system = self.get_system().await?;
        let epoch = self.get_epoch().await?;

        let phase = EpochPhase::try_from(epoch.state.phase).unwrap_or(EpochPhase::Unknown);

        Ok(build_state(&system, epoch.id, phase, epoch.nonce))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_state() -> ProtocolState {
        ProtocolState::default()
    }

    #[test]
    fn state_cache_store_load() {
        let cache = StateCache::new(empty_state());
        assert_eq!(cache.load().epoch, EpochNumber(0));

        let mut s = empty_state();
        s.epoch = EpochNumber(5);
        cache.store(s);
        assert_eq!(cache.load().epoch, EpochNumber(5));
    }

    #[test]
    fn state_cache_update_phase() {
        let cache = StateCache::new(empty_state());
        assert_eq!(cache.load().phase, EpochPhase::Active);

        cache.update_phase(EpochPhase::Syncing);
        assert_eq!(cache.load().phase, EpochPhase::Syncing);
    }

    #[test]
    fn find_member_empty() {
        let state = empty_state();
        assert!(state.find_member(NodeId(1)).is_none());
    }

    #[test]
    fn spool_owner_empty() {
        let state = empty_state();
        assert!(state.spool_owner(0).is_none());
    }
}
