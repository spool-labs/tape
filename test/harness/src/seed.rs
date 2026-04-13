use std::sync::Arc;

use anyhow::Result;
use bytemuck::Zeroable;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_api::program::tapedrive::{
    archive_pda, epoch_pda, history_pda, node_pda, snapshot_manifest_pda, system_pda,
};
use tape_api::state::{
    Archive, Epoch, History, Node, SnapshotChunkRecord, SnapshotManifest, System,
};
use tape_core::bls::BlsPrivateKey;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::prelude::NodeId;
use tape_core::spooler::SpoolAssignment;
use tape_core::staking::{PoolHistory, StakingPool};
use tape_core::system::{Committee, CommitteeMember, EpochSchedule, NodeMetadata, NodePreferences};
use tape_core::types::{EpochNumber, ShareAmount, SnapshotGroupBitmap, StorageUnits, VersionId};
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
    /// Fully-sealed snapshot manifest at `spec.epoch - 1`. Required by the
    /// `advance_epoch` gate for any non-bootstrap epoch (`spec.epoch > 1`).
    /// Set to `None` when `spec.epoch <= 1`.
    pub prev_snapshot_manifest: Option<SeedAccount<SnapshotManifest>>,
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

    // For any non-bootstrap epoch the on-chain `advance_epoch` gate requires
    // the previous epoch's snapshot manifest to be fully sealed. We seed a
    // synthetic fully-sealed manifest at `spec.epoch - 1` so the harness can
    // exercise the advance path without first running the actual snapshot
    // build/init/certify/finalize flow. Tests that exercise the snapshot
    // pipeline itself opt out via `seed_prev_snapshot_manifest = false`, so
    // the manifest PDA is empty when init tries to create it.
    let prev_snapshot_manifest = if spec.seed_prev_snapshot_manifest && spec.epoch > EpochNumber(1)
    {
        let prev_epoch = spec.epoch - EpochNumber(1);
        let (manifest_address, _) = snapshot_manifest_pda(prev_epoch);
        let mut group_bitmap = SnapshotGroupBitmap::zeroed();
        for group_index in 0..SPOOL_GROUP_COUNT {
            group_bitmap.set(group_index);
        }
        let manifest = SnapshotManifest {
            epoch: prev_epoch,
            group_bitmap,
            chunk_size: StorageUnits::from_bytes(1_024),
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };
        Some(SeedAccount {
            address: manifest_address.into(),
            data: manifest,
        })
    } else {
        None
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
            authority: identity.authority.into(),
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

        let (history_address, _) = history_pda(identity.node_address.into());
        let history = History {
            node: identity.node_address.into(),
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
            address: history_address.into(),
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
            address: system_address.into(),
            data: system,
        },
        epoch: SeedAccount {
            address: epoch_address.into(),
            data: epoch,
        },
        archive: SeedAccount {
            address: archive_address.into(),
            data: archive,
        },
        prev_snapshot_manifest,
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
            let (node_address, _) = node_pda(authority.into());
            let bls_keypair = Arc::new(BlsPrivateKey::from_random());
            NodeIdentity {
                authority,
                node_address: node_address.into(),
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
