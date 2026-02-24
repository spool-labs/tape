//! In-memory chain state shared across all node components.
//!
//! `ChainState` holds epoch, phase, committee, spool assignments, and node
//! status. It is updated atomically via `ArcSwap` pointer swap — either by the
//! runtime buffer (on epoch transitions) or by direct phase updates from the
//! FSM.
//!
//! All consumers (scheduler, HTTP handlers, tasks) read through
//! `ChainStateHandle`, which is a thin wrapper around `Arc<ArcSwap<ChainState>>`.

use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytemuck::Zeroable;
use rpc::Rpc;
use rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tape_api::state::Node;
use tape_core::bls::BlsPubkey;
use tape_core::prelude::Hash;
use tape_core::spooler::{SpoolAssignment, SpoolIndex};
use tape_core::system::{Committee, EpochPhase};
use tape_core::types::EpochNumber;
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_store::types::{NodeInfo, NodeStatus, Pubkey as StorePubkey};

/// Snapshot of the current chain-derived state.
///
/// Updated atomically via pointer swap. All fields are consistent with each
/// other — epoch N's committee is always paired with epoch N's phase and spools.
#[derive(Debug, Clone)]
pub struct ChainState {
    pub epoch: EpochNumber,
    pub phase: EpochPhase,
    pub nonce: Hash,
    /// Current epoch committee members.
    pub committee: Vec<NodeInfo>,
    /// Previous epoch committee members.
    pub committee_prev: Vec<NodeInfo>,
    /// This node's status (Active, Standby, etc.)
    pub node_status: NodeStatus,
    /// Spool indices assigned to this node.
    pub spools: HashSet<SpoolIndex>,
}

impl ChainState {
    /// True if we have a known epoch (not zero/uninitialized).
    pub fn has_epoch(&self) -> bool {
        !self.epoch.is_zero()
    }

    /// Look up committee for the given epoch.
    /// Returns current committee, previous committee, or None.
    pub fn committee_for(&self, epoch: EpochNumber) -> Option<&Vec<NodeInfo>> {
        if epoch == self.epoch {
            Some(&self.committee)
        } else if !self.epoch.is_zero() && epoch == self.epoch - EpochNumber(1) {
            Some(&self.committee_prev)
        } else {
            None
        }
    }
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            epoch: EpochNumber(0),
            phase: EpochPhase::Unknown,
            nonce: Hash::default(),
            committee: Vec::new(),
            committee_prev: Vec::new(),
            node_status: NodeStatus::Standby,
            spools: HashSet::new(),
        }
    }
}

/// Shared handle for reading chain state across components.
///
/// Cloning is cheap (Arc clone). Reads are lock-free via `ArcSwap::load`.
#[derive(Clone)]
pub struct ChainStateHandle {
    inner: Arc<ArcSwap<ChainState>>,
}

impl ChainStateHandle {
    /// Create a new handle with default (empty) state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(ChainState::default())),
        }
    }

    /// Create a handle seeded with initial state.
    pub fn with_state(state: ChainState) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(state)),
        }
    }

    /// Load the current chain state (lock-free).
    pub fn load(&self) -> arc_swap::Guard<Arc<ChainState>> {
        self.inner.load()
    }

    /// Atomically swap the entire chain state.
    pub fn store(&self, state: ChainState) {
        self.inner.store(Arc::new(state));
    }

    /// Update only the phase, keeping everything else unchanged.
    pub fn update_phase(&self, phase: EpochPhase) {
        let current = self.inner.load();
        let mut updated = (**current).clone();
        updated.phase = phase;
        self.inner.store(Arc::new(updated));
    }
}

/// Fetch on-chain state and build a `ChainState` snapshot.
///
/// Calls `get_system()`, `get_epoch()`, and `get_all_nodes()` via RPC, then
/// derives committee, spool assignments, and node status from the results.
pub async fn fetch_chain_state<R: Rpc>(
    rpc: &RpcClient<R>,
    our_bls: &BlsPubkey,
) -> Result<ChainState, String> {
    let system = rpc.get_system().await
        .map_err(|e| format!("get_system: {e}"))?;
    let epoch_account = rpc.get_epoch().await
        .map_err(|e| format!("get_epoch: {e}"))?;
    let all_nodes = rpc.get_all_nodes().await
        .map_err(|e| format!("get_all_nodes: {e}"))?;

    let phase = EpochPhase::try_from(epoch_account.state.phase)
        .unwrap_or(EpochPhase::Unknown);

    let node_map: std::collections::HashMap<NodeId, &(Pubkey, Node)> =
        all_nodes.iter().map(|entry| (entry.1.id, entry)).collect();

    let committee = build_committee(
        &system.committee,
        &system.spools,
        &node_map,
    );

    let committee_prev = if epoch_account.id.0 > 0 && system.committee_prev.size() > 0 {
        build_committee(
            &system.committee_prev,
            &system.spools_prev,
            &node_map,
        )
    } else {
        Vec::new()
    };

    let our_membership = system.committee.iter()
        .enumerate()
        .find(|(_, member)| member.key == *our_bls);

    let (node_status, spools) = match our_membership {
        Some((member_index, _)) => {
            let assigned: HashSet<SpoolIndex> = system
                .spools
                .spools_for_member(member_index)
                .into_iter()
                .collect();
            (NodeStatus::Active, assigned)
        }
        None => {
            // Bootstrap: current committee empty, check committee_next.
            // Active with no spools lets lifecycle scheduler run AdvanceEpoch.
            let in_next = system.committee.size() == 0
                && system.committee_next.iter().any(|m| m.key == *our_bls);
            if in_next {
                (NodeStatus::Active, HashSet::new())
            } else {
                (NodeStatus::Standby, HashSet::new())
            }
        }
    };

    Ok(ChainState {
        epoch: epoch_account.id,
        phase,
        nonce: epoch_account.nonce,
        committee,
        committee_prev,
        node_status,
        spools,
    })
}

/// Build `Vec<NodeInfo>` from an on-chain committee and spool assignment.
fn build_committee<const N: usize, const S: usize>(
    committee: &Committee<N>,
    spools: &SpoolAssignment<S>,
    node_map: &std::collections::HashMap<NodeId, &(Pubkey, Node)>,
) -> Vec<NodeInfo> {
    committee
        .iter()
        .enumerate()
        .map(|(index, member)| {
            let (node_address, tls_pubkey, network_address) =
                if let Some(&(pubkey, ref node)) = node_map.get(&member.id) {
                    (
                        StorePubkey(pubkey.to_bytes()),
                        StorePubkey(node.metadata.network_tls.to_bytes()),
                        node.metadata.network_address,
                    )
                } else {
                    (
                        StorePubkey([0u8; 32]),
                        StorePubkey([0u8; 32]),
                        NetworkAddress::zeroed(),
                    )
                };

            NodeInfo {
                node_address,
                bls_pubkey: member.key,
                tls_pubkey,
                network_address,
                spools: spools.spools_for_member(index),
            }
        })
        .collect()
}
