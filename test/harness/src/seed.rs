use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytemuck::Zeroable;
use solana_sdk::program_option::COption;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use spl_token::state::{Account as TokenAccount, AccountState, Mint};
use tape_api::dynamic::DynamicState;
use tape_api::program::{
    MAX_SUPPLY, MIN_COMMITTEE_SIZE, MIN_STORAGE_CAPACITY, MIN_STORAGE_PRICE, TOKEN_DECIMALS,
};
use tape_api::program::tapedrive::{
    archive_pda, blacklist_pda, committee_pda, epoch_pda, group_pda, history_pda, node_pda,
    peer_set_pda, snapshot_tape_pda, system_pda, ARCHIVE_ADDRESS, ARCHIVE_ATA,
    SUBSIDY_ADDRESS, SUBSIDY_ATA,
};
use tape_api::program::token::{MINT_ADDRESS, TREASURY_ADDRESS};
use tape_api::state::{Archive, Committee, Epoch, Group, Node, PeerSet, System, Tape};
use tape_core::bls::BlsPrivateKey;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::staking::StakingPool;
use tape_core::system::{
    aggregate_node_preferences, EpochPhase, EpochSchedule, EpochState, Member, NodeMetadata,
    NodePreferences, Peer, Spool,
};
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber, NodeId, ShareAmount, StorageUnits, Tail, VersionId};
use tape_crypto::{Address, Hash};
use tape_protocol::{EpochBundle, ProtocolState};

use crate::node::HarnessNode;
use crate::spec::{previous_epoch, HarnessNodeSpec, HarnessSpec};

const DEFAULT_GROUP_SIZE: StorageUnits = StorageUnits(StorageUnits::GB);

pub(crate) struct SeedAccount {
    pub address: Pubkey,
    pub data: Vec<u8>,
}

pub(crate) struct SeededWorld {
    pub protocol_state: ProtocolState,
    pub system: SeedAccount,
    pub epochs: Vec<SeedAccount>,
    pub committees: Vec<SeedAccount>,
    pub peer_set: SeedAccount,
    pub groups: Vec<SeedAccount>,
    pub archive: SeedAccount,
    pub mint: SeedAccount,
    pub archive_ata: SeedAccount,
    pub subsidy_ata: SeedAccount,
    pub prev_snapshot_tape: Option<SeedAccount>,
    pub nodes: Vec<HarnessNode>,
    pub node_accounts: Vec<SeedAccount>,
    pub history_accounts: Vec<SeedAccount>,
    pub blacklist_accounts: Vec<SeedAccount>,
}

pub(crate) fn build_seeded_world(spec: &HarnessSpec) -> Result<SeededWorld> {
    let identities = build_identities(spec.nodes.len());

    let committee_capacity = committee_capacity(spec);
    let prev_epoch = previous_epoch(spec.epoch);
    let next_epoch = spec.epoch.next();
    let candidate_epoch = spec.epoch.saturating_add(EpochNumber(2));

    let prev = build_epoch_bundle(
        prev_epoch,
        &spec.prev_committee_nodes,
        spec.prev_group_count,
        spec,
        &identities,
    );
    let current = build_epoch_bundle(
        spec.epoch,
        &spec.current_committee_nodes,
        spec.current_group_count,
        spec,
        &identities,
    );
    let next = if spec.next_assignment_ready {
        build_epoch_bundle(
            next_epoch,
            &spec.next_committee_nodes,
            spec.current_group_count,
            spec,
            &identities,
        )
    } else {
        EpochBundle {
            epoch: Epoch {
                id: next_epoch,
                state: epoch_state(EpochPhase::Unknown),
                ..Epoch::zeroed()
            },
            committee: committee_members(&spec.next_committee_nodes, spec, &identities),
            groups: Vec::new(),
        }
    };

    let peers = peer_set(spec, &identities);
    let committed_preferences = committed_preferences(spec, &current.committee, &peers)
        .context("build committed preferences")?;
    let peer_capacity = if spec.candidate_ready {
        committee_capacity
            .max(committed_preferences.committee_size)
            .saturating_mul(3)
    } else {
        committee_capacity.saturating_mul(3)
    };

    let system = System {
        current_epoch: spec.epoch,
        min_version: VersionId(1),
        total_nodes: spec.nodes.len() as u64,
        committee_size: committee_capacity,
        target_group_count: spec.current_group_count,
        live_group_count: spec.current_group_count,
    };

    let current_epoch_account = Epoch {
        id: spec.epoch,
        start_time: spec.last_epoch,
        state: spec.epoch_state(),
        nonce: Hash::default(),
        total_groups: spec.current_group_count,
        total_assigned: total_assigned(&current.groups),
        ..Epoch::zeroed()
    };

    let prev_epoch_account = Epoch {
        id: prev_epoch,
        state: epoch_state(EpochPhase::Completed),
        nonce: Hash::default(),
        total_groups: spec.prev_group_count,
        total_assigned: total_assigned(&prev.groups),
        ..Epoch::zeroed()
    };

    let next_epoch_account = if spec.next_assignment_ready {
        Epoch {
            id: next_epoch,
            state: epoch_state(EpochPhase::Unknown),
            assignment_hash: Hash::from([0x88; 32]),
            preferences: committed_preferences,
            total_groups: next.epoch.total_groups,
            total_assigned: next.epoch.total_assigned,
            ..Epoch::zeroed()
        }
    } else {
        Epoch {
            preferences: if spec.phase == EpochPhase::Closing || spec.candidate_ready {
                committed_preferences
            } else {
                NodePreferences::zeroed()
            },
            ..next.epoch
        }
    };

    let candidate_epoch_account = spec.candidate_ready.then(|| Epoch {
        id: candidate_epoch,
        state: epoch_state(EpochPhase::Unknown),
        ..Epoch::zeroed()
    });
    let candidate_committee_capacity = spec
        .candidate_ready
        .then_some(committed_preferences.committee_size);

    let archive = Archive {
        schedule: EpochSchedule::new_at(spec.epoch),
        ..Archive::zeroed()
    };

    let prev_snapshot_tape = if spec.seed_prev_snapshot_tape && spec.epoch > EpochNumber(1) {
        let (snapshot_address, _) = snapshot_tape_pda(prev_epoch);
        let tape = Tape::snapshot(prev_epoch);
        Some(SeedAccount {
            address: snapshot_address.into(),
            data: tape.pack(),
        })
    } else {
        None
    };

    let mut nodes = Vec::with_capacity(spec.nodes.len());
    let mut node_accounts = Vec::with_capacity(spec.nodes.len());
    let mut history_accounts = Vec::with_capacity(spec.nodes.len());
    let mut blacklist_accounts = Vec::with_capacity(spec.nodes.len());

    let current_index = member_index_by_node(&spec.current_committee_nodes);
    let prev_index = member_index_by_node(&spec.prev_committee_nodes);
    let next_index = member_index_by_node(&spec.next_committee_nodes);

    for (index, identity) in identities.iter().enumerate() {
        let node_spec = &spec.nodes[index];
        let node_id = NodeId(index as u64);
        let bls_pubkey = identity.bls_keypair.public_key().expect("bls public key");
        let preferences = node_preferences(node_spec, committee_capacity, spec.current_group_count);

        let rate_span_start = node_spec.latest_advance_epoch.next().min(spec.epoch);

        let node = Node {
            id: node_id,
            authority: identity.authority.into(),
            metadata: NodeMetadata {
                bls_pubkey,
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
            rate_span_start,
            ..Node::zeroed()
        };

        let (history_address, _) = history_pda(identity.node_address.into());
        let history_tape = Tape::history(node_id, node_spec.registered_epoch);

        let (blacklist_address, _) = blacklist_pda(identity.node_address.into());
        let blacklist_tape = Tape::blacklist(node_id, node_spec.registered_epoch);

        nodes.push(HarnessNode::new(
            index,
            node_id,
            identity.authority,
            identity.node_address,
            current_index.get(&index).copied(),
            prev_index.get(&index).copied(),
            next_index.get(&index).copied(),
            identity.keypair.clone(),
            identity.bls_keypair.clone(),
        ));
        node_accounts.push(SeedAccount {
            address: identity.node_address,
            data: node.pack(),
        });
        history_accounts.push(SeedAccount {
            address: history_address.into(),
            data: history_tape.pack(),
        });
        blacklist_accounts.push(SeedAccount {
            address: blacklist_address.into(),
            data: blacklist_tape.pack(),
        });
    }

    let mut protocol_current = current.clone();
    protocol_current.epoch = current_epoch_account;

    let mut protocol_prev = prev.clone();
    protocol_prev.epoch = prev_epoch_account;

    let protocol_previous = if spec.epoch.is_zero() {
        None
    } else {
        Some(protocol_prev)
    };

    let protocol_state = ProtocolState {
        system,
        peers: peers.clone(),
        peer_capacity,
        current: protocol_current,
        previous: protocol_previous,
        next_epoch: Some(next_epoch_account),
        next_committee: Some(next.committee.clone()),
        next_committee_capacity: Some(committee_capacity),
        candidate_epoch: candidate_epoch_account,
        candidate_committee_capacity,
    };

    let mut epochs = vec![
        seed(epoch_pda(prev_epoch).0, prev_epoch_account.pack()),
        seed(epoch_pda(spec.epoch).0, current_epoch_account.pack()),
        seed(epoch_pda(next_epoch).0, next_epoch_account.pack()),
    ];
    if let Some(epoch) = candidate_epoch_account {
        epochs.push(seed(epoch_pda(candidate_epoch).0, epoch.pack()));
    }

    let mut committees = vec![
        seed_committee(prev_epoch, committee_capacity, &prev.committee),
        seed_committee(spec.epoch, committee_capacity, &current.committee),
        seed_committee(next_epoch, committee_capacity, &next.committee),
    ];
    if let Some(capacity) = candidate_committee_capacity {
        committees.push(seed_committee(candidate_epoch, capacity, &[]));
    }

    Ok(SeededWorld {
        protocol_state,
        system: seed(system_pda().0, system.pack()),
        epochs,
        committees,
        peer_set: seed_peer_set(peer_capacity, &peers),
        groups: seed_groups(&prev.groups)
            .into_iter()
            .chain(seed_groups(&current.groups))
            .chain(seed_groups(&next.groups))
            .collect(),
        archive: seed(archive_pda().0, archive.pack()),
        mint: seed_mint(),
        archive_ata: seed_token_account(ARCHIVE_ATA, ARCHIVE_ADDRESS, 0),
        subsidy_ata: seed_token_account(SUBSIDY_ATA, SUBSIDY_ADDRESS, 0),
        prev_snapshot_tape,
        nodes,
        node_accounts,
        history_accounts,
        blacklist_accounts,
    })
}

#[derive(Clone)]
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

fn build_epoch_bundle(
    epoch: EpochNumber,
    indices: &[usize],
    group_count: u64,
    spec: &HarnessSpec,
    identities: &[NodeIdentity],
) -> EpochBundle {
    let mut members = committee_members(indices, spec, identities);
    let groups = groups_for_members(epoch, indices, identities, &mut members, group_count);

    EpochBundle {
        epoch: Epoch {
            id: epoch,
            total_groups: group_count,
            total_assigned: total_assigned(&groups),
            ..Epoch::zeroed()
        },
        committee: members,
        groups,
    }
}

fn committee_members(
    indices: &[usize],
    spec: &HarnessSpec,
    identities: &[NodeIdentity],
) -> Vec<Member> {
    indices
        .iter()
        .map(|&index| {
            let node_spec = &spec.nodes[index];
            Member {
                node: identities[index].node_address.into(),
                stake: node_spec.stake,
                assigned: StorageUnits::zero(),
                blacklisted: StorageUnits::zero(),
                spools: 0,
            }
        })
        .collect()
}

fn groups_for_members(
    epoch: EpochNumber,
    indices: &[usize],
    identities: &[NodeIdentity],
    members: &mut [Member],
    group_count: u64,
) -> Vec<Group> {
    if indices.is_empty() || group_count == 0 {
        return Vec::new();
    }

    (0..group_count)
        .map(|group_number| {
            let group_id = GroupIndex(group_number);
            let mut group = Group {
                id: group_id,
                epoch,
                size: DEFAULT_GROUP_SIZE,
                ..Group::zeroed()
            };

            for position in 0..GROUP_SIZE {
                let member_idx = ((group_number as usize * GROUP_SIZE) + position) % indices.len();
                let node_index = indices[member_idx];
                let identity = &identities[node_index];
                group.spools[position] = Spool::new(
                    identity.node_address.into(),
                    identity.bls_keypair.public_key().expect("bls public key"),
                );
                members[member_idx].spools = members[member_idx].spools.saturating_add(1);
            }

            group
        })
        .collect()
}

fn peer_set(spec: &HarnessSpec, identities: &[NodeIdentity]) -> Vec<Peer> {
    let mut selected = BTreeSet::new();
    selected.extend(spec.prev_committee_nodes.iter().copied());
    selected.extend(spec.current_committee_nodes.iter().copied());
    selected.extend(spec.next_committee_nodes.iter().copied());
    let committee_size = committee_capacity(spec);

    selected
        .into_iter()
        .map(|index| {
            let node_spec = &spec.nodes[index];
            let identity = &identities[index];
            Peer {
                node: identity.node_address.into(),
                bls_pubkey: identity.bls_keypair.public_key().expect("bls public key"),
                preferences: node_preferences(node_spec, committee_size, spec.current_group_count),
                ..Peer::zeroed()
            }
        })
        .collect()
}

fn committed_preferences(
    spec: &HarnessSpec,
    members: &[Member],
    peers: &[Peer],
) -> Result<NodePreferences> {
    let bounds = NodePreferences {
        storage_capacity: StorageUnits(MIN_STORAGE_CAPACITY as u64),
        storage_price: TAPE(MIN_STORAGE_PRICE as u64),
        committee_size: MIN_COMMITTEE_SIZE as u64,
        spool_groups: spec.current_group_count,
        min_version: VersionId(1),
        burn_fee_bps: BasisPoints(0),
        subsidy_decay_bps: BasisPoints(0),
    };

    aggregate_node_preferences(members, peers, bounds)
        .map_err(|error| anyhow!("aggregate node preferences: {error:?}"))
}

fn node_preferences(
    spec: &HarnessNodeSpec,
    committee_size: u64,
    spool_groups: u64,
) -> NodePreferences {
    NodePreferences {
        storage_capacity: spec.storage_capacity,
        storage_price: spec.storage_price,
        committee_size,
        spool_groups,
        min_version: VersionId(1),
        burn_fee_bps: spec.burn_fee_bps,
        subsidy_decay_bps: spec.subsidy_decay_bps,
    }
}

fn member_index_by_node(indices: &[usize]) -> std::collections::BTreeMap<usize, usize> {
    indices
        .iter()
        .enumerate()
        .map(|(member_index, node_index)| (*node_index, member_index))
        .collect()
}

fn total_assigned(groups: &[Group]) -> StorageUnits {
    let total_spools = groups.len().saturating_mul(GROUP_SIZE) as u64;
    StorageUnits(DEFAULT_GROUP_SIZE.0.saturating_mul(total_spools))
}

fn committee_capacity(spec: &HarnessSpec) -> u64 {
    let max_committee = spec
        .current_committee_nodes
        .len()
        .max(spec.prev_committee_nodes.len())
        .max(spec.next_committee_nodes.len())
        .max(MIN_COMMITTEE_SIZE);
    max_committee as u64
}

fn epoch_state(phase: EpochPhase) -> EpochState {
    EpochState {
        phase: phase as u64,
        ..EpochState::zeroed()
    }
}

fn seed(address: Address, data: Vec<u8>) -> SeedAccount {
    SeedAccount {
        address: address.into(),
        data,
    }
}

fn seed_mint() -> SeedAccount {
    let state = Mint {
        mint_authority: COption::Some(TREASURY_ADDRESS.into()),
        supply: MAX_SUPPLY,
        decimals: TOKEN_DECIMALS,
        is_initialized: true,
        freeze_authority: COption::None,
    };
    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(state, &mut data).expect("pack mint");
    seed(MINT_ADDRESS, data)
}

fn seed_token_account(address: Address, owner: Address, amount: u64) -> SeedAccount {
    let state = TokenAccount {
        mint: MINT_ADDRESS.into(),
        owner: owner.into(),
        amount,
        delegate: COption::None,
        state: AccountState::Initialized,
        is_native: COption::None,
        delegated_amount: 0,
        close_authority: COption::None,
    };
    let mut data = vec![0u8; TokenAccount::LEN];
    TokenAccount::pack(state, &mut data).expect("pack token account");
    seed(address, data)
}

fn seed_committee(epoch: EpochNumber, capacity: u64, members: &[Member]) -> SeedAccount {
    seed(
        committee_pda(epoch).0,
        Committee {
            epoch,
            members: Tail::new(capacity, members.len() as u64),
        }
        .pack_with(members),
    )
}

fn seed_peer_set(capacity: u64, peers: &[Peer]) -> SeedAccount {
    seed(
        peer_set_pda().0,
        PeerSet {
            peers: Tail::new(capacity, peers.len() as u64),
        }
        .pack_with(peers),
    )
}

fn seed_groups(groups: &[Group]) -> Vec<SeedAccount> {
    groups
        .iter()
        .map(|group| seed(group_pda(group.epoch, group.id).0, group.pack()))
        .collect()
}
