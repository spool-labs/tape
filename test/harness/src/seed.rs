use std::sync::Arc;

use anyhow::Result;
use bytemuck::Zeroable;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_api::program::tapedrive::{
    archive_pda, epoch_pda, history_pda, node_pda, snapshot_state_pda, system_pda,
};
use tape_api::state::{Archive, Epoch, History, Node, SnapshotState, System};
use tape_core::bls::BlsPrivateKey;
use tape_core::prelude::{
    Committee, CommitteeMember, EpochSchedule, NodeId, PoolHistory, ShareAmount, StakingPool,
    VersionId,
};
use tape_core::spooler::SpoolAssignment;
use tape_core::system::{NodeMetadata, NodePreferences};
use tape_crypto::Hash;
use tape_protocol::ProtocolState;

use crate::node::HarnessNode;
use crate::spec::{HarnessSpec, HarnessNodeSpec, previous_epoch};

pub(crate) struct SeedAccount<T> {
    pub address: Pubkey,
    pub data: T,
}

pub(crate) struct SeededWorld {
    pub protocol_state: ProtocolState,
    pub system: SeedAccount<System>,
    pub epoch: SeedAccount<Epoch>,
    pub archive: SeedAccount<Archive>,
    pub snapshot_state: SeedAccount<SnapshotState>,
    pub nodes: Vec<HarnessNode>,
    pub node_accounts: Vec<SeedAccount<Node>>,
    pub history_accounts: Vec<SeedAccount<History>>,
}

pub(crate) fn build_seeded_world(spec: &HarnessSpec) -> Result<SeededWorld> {
    let identities = build_identities(spec.nodes.len());

    let prev_members = committee_members(&spec.prev_committee_nodes, spec, &identities);
    let curr_members = committee_members(&spec.current_committee_nodes, spec, &identities);
    let next_members = committee_members(&spec.next_committee_nodes, spec, &identities);

    let mut committee_prev = Committee::from_members(&prev_members);
    let mut committee = Committee::from_members(&curr_members);
    let committee_next = Committee::from_members(&next_members);

    let spools_prev = spool_assignment(&spec.prev_spool_counts);
    let spools = spool_assignment(&spec.current_spool_counts);

    committee_prev.apply_weights_from_spools(&spools_prev);
    committee.apply_weights_from_spools(&spools);

    let (system_address, _) = system_pda();
    let system = System {
        version: VersionId(1),
        total_nodes: spec.nodes.len() as u64,
        committee_prev,
        committee,
        committee_next,
        spools_prev,
        spools,
    };

    let (epoch_address, _) = epoch_pda();
    let epoch = Epoch {
        id: spec.epoch,
        state: spec.epoch_state(),
        last_epoch: spec.last_epoch,
        nonce: Hash::default(),
    };

    let (archive_address, _) = archive_pda();
    let archive = Archive {
        schedule: EpochSchedule::new_at(spec.epoch),
        ..Archive::zeroed()
    };

    let (snapshot_state_address, _) = snapshot_state_pda();
    let snapshot_state = SnapshotState {
        tail_epoch: previous_epoch(spec.epoch),
        ..SnapshotState::zeroed()
    };

    let mut nodes = Vec::with_capacity(spec.nodes.len());
    let mut node_accounts = Vec::with_capacity(spec.nodes.len());
    let mut history_accounts = Vec::with_capacity(spec.nodes.len());

    for (index, identity) in identities.into_iter().enumerate() {
        let node_id = NodeId(index as u64);
        let node_spec = &spec.nodes[index];
        let bls_pubkey = identity.bls_keypair.public_key().expect("bls public key");

        let preferences = NodePreferences {
            storage_capacity: node_spec.storage_capacity,
            storage_price: node_spec.storage_price,
        };

        let node = Node {
            id: node_id,
            authority: identity.authority,
            metadata: NodeMetadata {
                bls_pubkey,
                next_bls_pubkey: bls_pubkey,
                ..NodeMetadata::zeroed()
            },
            preferences,
            pool: StakingPool {
                shares: ShareAmount(node_spec.stake.as_u64()),
                stake: node_spec.stake,
                commission_rate: node_spec.commission_rate,
                ..StakingPool::zeroed()
            },
            registered_epoch: node_spec.registered_epoch,
            latest_sync_epoch: node_spec.latest_sync_epoch,
            latest_advance_epoch: node_spec.latest_advance_epoch,
            ..Node::zeroed()
        };

        let (history_address, _) = history_pda(identity.node_address);
        let history = History {
            node: identity.node_address,
            registered_epoch: node_spec.registered_epoch,
            latest_epoch: previous_epoch(spec.epoch),
            inner: PoolHistory::new(),
            ..History::zeroed()
        };

        let harness_node = HarnessNode::new(
            index,
            node_id,
            identity.authority,
            identity.node_address,
            system.committee.index_of(&node_id),
            system.committee_prev.index_of(&node_id),
            system.committee_next.index_of(&node_id),
            identity.keypair,
            identity.bls_keypair,
        );

        nodes.push(harness_node);
        node_accounts.push(SeedAccount {
            address: identity.node_address,
            data: node,
        });
        history_accounts.push(SeedAccount {
            address: history_address,
            data: history,
        });
    }

    let protocol_state = ProtocolState {
        epoch: spec.epoch,
        phase: spec.phase,
        last_epoch: spec.last_epoch,
        nonce: epoch.nonce,
        committee: system.committee.iter().copied().collect(),
        committee_prev: system.committee_prev.iter().copied().collect(),
        committee_next: system.committee_next.iter().copied().collect(),
        spools: system.spools,
        spools_prev: system.spools_prev,
    };

    Ok(SeededWorld {
        protocol_state,
        system: SeedAccount {
            address: system_address,
            data: system,
        },
        epoch: SeedAccount {
            address: epoch_address,
            data: epoch,
        },
        archive: SeedAccount {
            address: archive_address,
            data: archive,
        },
        snapshot_state: SeedAccount {
            address: snapshot_state_address,
            data: snapshot_state,
        },
        nodes,
        node_accounts,
        history_accounts,
    })
}

struct NodeIdentity {
    authority: Pubkey,
    node_address: Pubkey,
    keypair: Arc<Keypair>,
    bls_keypair: Arc<BlsPrivateKey>,
}

fn build_identities(count: usize) -> Vec<NodeIdentity> {
    (0..count)
        .map(|_| {
            let keypair = Arc::new(Keypair::new());
            let authority = keypair.pubkey();
            let (node_address, _) = node_pda(authority);
            let bls_keypair = Arc::new(BlsPrivateKey::from_random());
            NodeIdentity {
                authority,
                node_address,
                keypair,
                bls_keypair,
            }
        })
        .collect()
}

fn committee_members(
    indices: &[usize],
    spec: &HarnessSpec,
    identities: &[NodeIdentity],
) -> Vec<CommitteeMember> {
    indices
        .iter()
        .map(|&index| committee_member(index, &spec.nodes[index], &identities[index]))
        .collect()
}

fn committee_member(
    index: usize,
    node_spec: &HarnessNodeSpec,
    identity: &NodeIdentity,
) -> CommitteeMember {
    CommitteeMember {
        id: NodeId(index as u64),
        stake: node_spec.stake,
        key: identity.bls_keypair.public_key().expect("bls public key"),
        blacklist: 0u64.into(),
        preferences: NodePreferences {
            storage_capacity: node_spec.storage_capacity,
            storage_price: node_spec.storage_price,
        },
        weight: 0,
    }
}

fn spool_assignment(counts: &[u16]) -> SpoolAssignment<{ tape_core::erasure::SPOOL_COUNT }> {
    if counts.is_empty() {
        SpoolAssignment::zeroed()
    } else {
        SpoolAssignment::try_from_counts(counts).expect("validated spool counts")
    }
}
