use std::collections::HashSet;

use bytemuck::Zeroable;
use tape_api::state::{Epoch, Group, System};
use tape_core::spooler::SpoolGroup;
use tape_core::system::{EpochPhase, Member, Peer};
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::Address;

/// On-chain state for one epoch, normalized for off-chain protocol use.
#[derive(Debug, Clone)]
pub struct EpochBundle {
    pub epoch: Epoch,
    pub committee: Vec<Member>,
    pub groups: Vec<Group>,
}

/// Snapshot of on-chain protocol state.
///
/// This is address-first. `NodeId` remains useful for logs and local node
/// metadata, but peer routing and spool ownership are keyed by node account
/// `Address`.
#[derive(Debug, Clone)]
pub struct ProtocolState {
    pub system: System,
    pub peers: Vec<Peer>,
    pub current: EpochBundle,
    pub previous: Option<EpochBundle>,
    pub next_epoch: Option<Epoch>,
    pub next_committee: Option<Vec<Member>>,
}

impl Default for EpochBundle {
    fn default() -> Self {
        Self {
            epoch: Epoch::zeroed(),
            committee: Vec::new(),
            groups: Vec::new(),
        }
    }
}

impl Default for ProtocolState {
    fn default() -> Self {
        Self {
            system: System::zeroed(),
            peers: Vec::new(),
            current: EpochBundle::default(),
            previous: None,
            next_epoch: None,
            next_committee: None,
        }
    }
}

impl ProtocolState {
    /// The current epoch number.
    pub fn epoch(&self) -> EpochNumber {
        self.current.epoch.id
    }

    /// The current epoch phase.
    pub fn phase(&self) -> EpochPhase {
        EpochPhase::try_from(self.current.epoch.state.phase).unwrap_or(EpochPhase::Unknown)
    }

    /// The current epoch's nonce, used for group assignments
    pub fn nonce(&self) -> tape_crypto::Hash {
        self.current.epoch.nonce
    }

    /// Find peer directory information by node account address.
    pub fn peer(&self, node: Address) -> Option<&Peer> {
        self.peers.iter().find(|peer| peer.node == node)
    }

    /// Find a member in the current committee by node account address.
    pub fn find_member(&self, node: Address) -> Option<&Member> {
        self.current
            .committee
            .iter()
            .find(|member| member.node == node)
    }

    /// Find a member in the next committee by node account address.
    pub fn find_member_next(&self, node: Address) -> Option<&Member> {
        self.next_committee
            .as_deref()?
            .iter()
            .find(|member| member.node == node)
    }

    /// Find a member in the previous committee by node account address.
    pub fn find_member_prev(&self, node: Address) -> Option<&Member> {
        self.previous
            .as_ref()?
            .committee
            .iter()
            .find(|member| member.node == node)
    }

    /// True if `node` is in the current, previous, or next committee.
    pub fn is_committee_peer(&self, node: Address) -> bool {
        self.find_member(node).is_some()
            || self.find_member_prev(node).is_some()
            || self.find_member_next(node).is_some()
    }

    /// Which node owns this spool in the current epoch?
    pub fn spool_owner(&self, spool: SpoolIndex) -> Option<Address> {
        spool_owner_inner(&self.current.groups, spool)
    }

    /// Which node owned this spool in the previous epoch?
    pub fn spool_owner_prev(&self, spool: SpoolIndex) -> Option<Address> {
        spool_owner_inner(&self.previous.as_ref()?.groups, spool)
    }

    /// All spools assigned to a node in the current epoch.
    pub fn member_spools(&self, node: Address) -> Vec<SpoolIndex> {
        member_spools_inner(&self.current.groups, node)
    }

    /// All spools assigned to a node in the previous epoch.
    pub fn member_spools_prev(&self, node: Address) -> Vec<SpoolIndex> {
        self.previous
            .as_ref()
            .map(|previous| member_spools_inner(&previous.groups, node))
            .unwrap_or_default()
    }

    /// Map each spool in a group to its owning node account address.
    pub fn group_peers(&self, group: SpoolGroup) -> Vec<(SpoolIndex, Address)> {
        group_peers_inner(&self.current.groups, group)
    }

    /// Map each spool in a previous-epoch group to its owning node account address.
    pub fn group_peers_prev(&self, group: SpoolGroup) -> Vec<(SpoolIndex, Address)> {
        self.previous
            .as_ref()
            .map(|previous| group_peers_inner(&previous.groups, group))
            .unwrap_or_default()
    }

    /// Count unique nodes responsible for spools in a current-epoch group.
    pub fn group_member_count(&self, group: SpoolGroup) -> usize {
        group_member_count_inner(&self.current.groups, group)
    }

    /// Count unique nodes responsible for spools in a previous-epoch group.
    pub fn group_member_count_prev(&self, group: SpoolGroup) -> usize {
        self.previous
            .as_ref()
            .map(|previous| group_member_count_inner(&previous.groups, group))
            .unwrap_or_default()
    }
}

fn spool_owner_inner(groups: &[Group], spool: SpoolIndex) -> Option<Address> {
    let group = SpoolGroup::of(spool);
    let slice = group.slice_of(spool)?.as_usize();
    groups
        .iter()
        .find(|candidate| candidate.id == group)?
        .spools
        .get(slice)
        .and_then(|spool| assigned_node(spool.node))
}

fn member_spools_inner(groups: &[Group], node: Address) -> Vec<SpoolIndex> {
    if assigned_node(node).is_none() {
        return Vec::new();
    }

    groups
        .iter()
        .flat_map(|group| {
            group
                .spools
                .iter()
                .enumerate()
                .filter_map(move |(slice, spool)| {
                    (spool.node == node).then_some(group.id.spool_at(slice))
                })
        })
        .collect()
}

fn group_peers_inner(groups: &[Group], group: SpoolGroup) -> Vec<(SpoolIndex, Address)> {
    groups
        .iter()
        .find(|candidate| candidate.id == group)
        .map(|group_account| {
            group_account
                .spools
                .iter()
                .enumerate()
                .filter_map(|(slice, spool)| {
                    assigned_node(spool.node).map(|node| (group.spool_at(slice), node))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn group_member_count_inner(groups: &[Group], group: SpoolGroup) -> usize {
    groups
        .iter()
        .find(|candidate| candidate.id == group)
        .map(|group_account| {
            group_account
                .spools
                .iter()
                .filter_map(|spool| assigned_node(spool.node))
                .collect::<HashSet<_>>()
                .len()
        })
        .unwrap_or_default()
}

fn assigned_node(node: Address) -> Option<Address> {
    (node != Address::default()).then_some(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::system::Spool;
    use tape_core::types::coin::TAPE;
    use tape_core::types::StorageUnits;
    use tape_crypto::Hash;

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn member(node: Address, stake: u64) -> Member {
        Member {
            node,
            stake: TAPE(stake),
            blacklist: StorageUnits::zero(),
            spools: 0,
        }
    }

    fn group(epoch: EpochNumber, id: SpoolGroup, owners: &[Address]) -> Group {
        let mut group = Group {
            epoch,
            id,
            ..Group::zeroed()
        };
        for i in 0..GROUP_SIZE {
            group.spools[i] = Spool {
                node: owners[i % owners.len()],
                ..Spool::zeroed()
            };
        }
        group
    }

    fn state_with_groups() -> ProtocolState {
        let epoch = EpochNumber(5);
        let a = address(1);
        let b = address(2);
        let c = address(3);
        ProtocolState {
            current: EpochBundle {
                epoch: Epoch {
                    id: epoch,
                    nonce: Hash::from([7; 32]),
                    ..Epoch::zeroed()
                },
                committee: vec![member(a, 100), member(b, 90), member(c, 80)],
                groups: vec![group(epoch, SpoolGroup(0), &[a, b, c])],
            },
            next_committee: Some(vec![member(address(9), 50)]),
            ..ProtocolState::default()
        }
    }

    #[test]
    fn find_member_empty() {
        let state = ProtocolState::default();
        assert!(state.find_member(address(1)).is_none());
        assert!(state.find_member_next(address(1)).is_none());
    }

    #[test]
    fn spool_owner_empty() {
        let state = ProtocolState::default();
        assert!(state.spool_owner(SpoolIndex(0)).is_none());
    }

    #[test]
    fn group_peers_all_spools() {
        let state = state_with_groups();
        let peers = state.group_peers(SpoolGroup(0));
        assert_eq!(peers.len(), GROUP_SIZE);
        assert_eq!(peers[0], (SpoolIndex(0), address(1)));
        assert_eq!(peers[1], (SpoolIndex(1), address(2)));
    }

    #[test]
    fn member_spools_uses_group_ownership() {
        let state = state_with_groups();
        assert_eq!(
            state.member_spools(address(1)),
            vec![
                SpoolIndex(0),
                SpoolIndex(3),
                SpoolIndex(6),
                SpoolIndex(9),
                SpoolIndex(12),
                SpoolIndex(15),
                SpoolIndex(18),
            ]
        );
    }

    #[test]
    fn group_member_count_counts_unique_addresses() {
        let state = state_with_groups();
        assert_eq!(state.group_member_count(SpoolGroup(0)), 3);
    }

    #[test]
    fn default_address_is_not_treated_as_owner() {
        let epoch = EpochNumber(5);
        let state = ProtocolState {
            current: EpochBundle {
                epoch: Epoch {
                    id: epoch,
                    ..Epoch::zeroed()
                },
                groups: vec![Group {
                    epoch,
                    id: SpoolGroup(0),
                    ..Group::zeroed()
                }],
                ..EpochBundle::default()
            },
            ..ProtocolState::default()
        };

        assert!(state.spool_owner(SpoolIndex(0)).is_none());
        assert!(state.member_spools(Address::default()).is_empty());
        assert!(state.group_peers(SpoolGroup(0)).is_empty());
        assert_eq!(state.group_member_count(SpoolGroup(0)), 0);
    }

    #[test]
    fn previous_helpers_use_previous_bundle() {
        let prev_epoch = EpochNumber(4);
        let prev_owner = address(8);
        let mut state = state_with_groups();
        state.previous = Some(EpochBundle {
            epoch: Epoch {
                id: prev_epoch,
                ..Epoch::zeroed()
            },
            committee: vec![member(prev_owner, 10)],
            groups: vec![group(prev_epoch, SpoolGroup(0), &[prev_owner])],
        });

        assert_eq!(state.spool_owner_prev(SpoolIndex(0)), Some(prev_owner));
        assert_eq!(state.group_member_count_prev(SpoolGroup(0)), 1);
    }

    #[test]
    fn find_member_next_uses_next_committee() {
        let state = state_with_groups();
        let member = state.find_member_next(address(9)).unwrap();
        assert_eq!(member.node, address(9));
    }
}
