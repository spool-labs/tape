use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytemuck::Zeroable;
use tape_api::state::{Epoch, Group, System};
use tape_core::spooler::GroupIndex;
use tape_core::system::{EpochPhase, Member, Peer, Spool};
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
    pub peer_capacity: u64,
    pub current: EpochBundle,
    pub previous: Option<EpochBundle>,
    pub next_epoch: Option<Epoch>,
    pub next_committee: Option<Vec<Member>>,
    pub next_committee_capacity: Option<u64>,
    pub candidate_epoch: Option<Epoch>,
    pub candidate_committee_capacity: Option<u64>,
    pub verified_at: Freshness,
}

/// When a snapshot was last verified against the chain.
///
/// Interior mutability, because verification re-confirms the snapshot already
/// in hand: a holder marks it fresh in place rather than swapping in a copy
/// that differs only by a timestamp.
#[derive(Debug, Default)]
pub struct Freshness(AtomicU64);

impl Clone for Freshness {
    fn clone(&self) -> Self {
        Self(AtomicU64::new(self.0.load(Ordering::Relaxed)))
    }
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
            peer_capacity: 0,
            current: EpochBundle::default(),
            previous: None,
            next_epoch: None,
            next_committee: None,
            next_committee_capacity: None,
            candidate_epoch: None,
            candidate_committee_capacity: None,
            verified_at: Freshness::default(),
        }
    }
}

impl ProtocolState {
    /// Age since this state was last verified against the chain.
    ///
    /// State that was never verified is infinitely old, so a default snapshot
    /// can never pass a freshness check.
    pub fn age(&self) -> Duration {
        match self.verified_at.0.load(Ordering::Relaxed) {
            0 => Duration::MAX,
            verified => Duration::from_millis(monotonic_ms().saturating_sub(verified)),
        }
    }

    /// Mark this state as freshly verified against the chain.
    pub fn touch(&self) {
        self.verified_at.0.store(monotonic_ms(), Ordering::Relaxed);
    }

    /// Drop the freshness mark, forcing the next reader to verify against the chain.
    pub fn invalidate(&self) {
        self.verified_at.0.store(0, Ordering::Relaxed);
    }

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

    /// Find a current-epoch group account by index.
    pub fn group(&self, group: GroupIndex) -> Option<&Group> {
        group_inner(&self.current.groups, group)
    }

    /// Find a previous-epoch group account by index.
    pub fn group_prev(&self, group: GroupIndex) -> Option<&Group> {
        group_inner(&self.previous.as_ref()?.groups, group)
    }

    /// Find a current-epoch spool assignment by global spool index.
    pub fn spool(&self, spool: SpoolIndex) -> Option<&Spool> {
        spool_inner(&self.current.groups, spool)
    }

    /// Find a previous-epoch spool assignment by global spool index.
    pub fn spool_prev(&self, spool: SpoolIndex) -> Option<&Spool> {
        spool_inner(&self.previous.as_ref()?.groups, spool)
    }

    /// Iterate current-epoch spool assignments for one group.
    pub fn spools_in_group(
        &self,
        group: GroupIndex,
    ) -> Option<impl Iterator<Item = (SpoolIndex, &Spool)> + '_> {
        spools_in_group_inner(&self.current.groups, group)
    }

    /// Iterate previous-epoch spool assignments for one group.
    pub fn spools_in_group_prev(
        &self,
        group: GroupIndex,
    ) -> Option<impl Iterator<Item = (SpoolIndex, &Spool)> + '_> {
        spools_in_group_inner(&self.previous.as_ref()?.groups, group)
    }

    /// Find the concrete spool position a node owns inside a current-epoch group.
    pub fn spool_for_node_in_group(
        &self,
        group: GroupIndex,
        node: Address,
    ) -> Option<(SpoolIndex, &Spool)> {
        spool_for_node_in_group_inner(&self.current.groups, group, node)
    }

    /// Find the concrete spool position a node owns inside a previous-epoch group.
    pub fn spool_for_node_in_group_prev(
        &self,
        group: GroupIndex,
        node: Address,
    ) -> Option<(SpoolIndex, &Spool)> {
        spool_for_node_in_group_inner(&self.previous.as_ref()?.groups, group, node)
    }

    /// Which node owns this spool in the current epoch?
    pub fn spool_owner(&self, spool: SpoolIndex) -> Option<Address> {
        self.spool(spool)
            .and_then(|spool| assigned_node(spool.node))
    }

    /// Which node owned this spool in the previous epoch?
    pub fn spool_owner_prev(&self, spool: SpoolIndex) -> Option<Address> {
        self.spool_prev(spool)
            .and_then(|spool| assigned_node(spool.node))
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
    pub fn group_peers(&self, group: GroupIndex) -> Vec<(SpoolIndex, Address)> {
        group_peers_inner(self.spools_in_group(group))
    }

    /// Map each spool in a previous-epoch group to its owning node account address.
    pub fn group_peers_prev(&self, group: GroupIndex) -> Vec<(SpoolIndex, Address)> {
        group_peers_inner(self.spools_in_group_prev(group))
    }

    /// Count unique nodes responsible for spools in a current-epoch group.
    pub fn group_member_count(&self, group: GroupIndex) -> usize {
        group_member_count_inner(self.group(group))
    }

    /// Count unique nodes responsible for spools in a previous-epoch group.
    pub fn group_member_count_prev(&self, group: GroupIndex) -> usize {
        group_member_count_inner(self.group_prev(group))
    }

    /// Each spool in a group with its owner as of `epoch`, current or previous
    pub fn group_peers_at(&self, epoch: EpochNumber, group: GroupIndex) -> Option<Vec<(SpoolIndex, Address)>> {
        let groups = self.groups_at(epoch)?;
        Some(group_peers_inner(spools_in_group_inner(groups, group)))
    }

    /// Unique nodes in a group as of `epoch`, current or previous
    pub fn group_member_count_at(&self, epoch: EpochNumber, group: GroupIndex) -> Option<usize> {
        let groups = self.groups_at(epoch)?;
        Some(group_member_count_inner(group_inner(groups, group)))
    }

    /// Whether a node held a spool in a group as of `epoch`
    pub fn is_member_at(&self, epoch: EpochNumber, group: GroupIndex, node: Address) -> bool {
        self.groups_at(epoch)
            .is_some_and(|groups| spool_for_node_in_group_inner(groups, group, node).is_some())
    }

    /// The groups as of `epoch`, or nothing once the state no longer holds it
    fn groups_at(&self, epoch: EpochNumber) -> Option<&[Group]> {
        if epoch == self.epoch() {
            return Some(&self.current.groups);
        }
        match &self.previous {
            Some(previous) if previous.epoch.id == epoch => Some(&previous.groups),
            Some(_) | None => None,
        }
    }
}

fn group_inner(groups: &[Group], group: GroupIndex) -> Option<&Group> {
    groups.iter().find(|candidate| candidate.id == group)
}

fn spool_inner(groups: &[Group], spool: SpoolIndex) -> Option<&Spool> {
    let group = GroupIndex::containing(spool);
    let position = group.position_of(spool)?;
    group_inner(groups, group)?.spools.get(position)
}

fn spools_in_group_inner(
    groups: &[Group],
    group: GroupIndex,
) -> Option<impl Iterator<Item = (SpoolIndex, &Spool)> + '_> {
    Some(
        group_inner(groups, group)?
            .spools
            .iter()
            .enumerate()
            .map(move |(position, spool)| (group.spool_at(position), spool)),
    )
}

fn spool_for_node_in_group_inner(
    groups: &[Group],
    group: GroupIndex,
    node: Address,
) -> Option<(SpoolIndex, &Spool)> {
    assigned_node(node)?;

    spools_in_group_inner(groups, group)?
        .find(|(_, spool)| spool.node == node)
}

fn member_spools_inner(groups: &[Group], node: Address) -> Vec<SpoolIndex> {
    if assigned_node(node).is_none() {
        return Vec::new();
    }

    groups
        .iter()
        .flat_map(|group| spools_in_group_inner(groups, group.id).into_iter().flatten())
        .filter_map(|(spool_index, spool)| (spool.node == node).then_some(spool_index))
        .collect()
}

fn group_peers_inner<'a>(
    spools: Option<impl Iterator<Item = (SpoolIndex, &'a Spool)>>,
) -> Vec<(SpoolIndex, Address)> {
    spools
        .map(|spools| {
            spools
                .filter_map(|(spool_index, spool)| {
                    assigned_node(spool.node).map(|node| (spool_index, node))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn group_member_count_inner(group: Option<&Group>) -> usize {
    group
        .map(|group| {
            group
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

/// Monotonic milliseconds since the first call in this process, offset by one
/// so that zero stays available to mean never verified.
fn monotonic_ms() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
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
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 0,
        }
    }

    fn group(epoch: EpochNumber, id: GroupIndex, owners: &[Address]) -> Group {
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
                groups: vec![group(epoch, GroupIndex(0), &[a, b, c])],
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
        let peers = state.group_peers(GroupIndex(0));
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
        assert_eq!(state.group_member_count(GroupIndex(0)), 3);
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
                    id: GroupIndex(0),
                    ..Group::zeroed()
                }],
                ..EpochBundle::default()
            },
            ..ProtocolState::default()
        };

        assert!(state.spool_owner(SpoolIndex(0)).is_none());
        assert!(state.member_spools(Address::default()).is_empty());
        assert!(state.group_peers(GroupIndex(0)).is_empty());
        assert_eq!(state.group_member_count(GroupIndex(0)), 0);
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
            groups: vec![group(prev_epoch, GroupIndex(0), &[prev_owner])],
        });

        assert_eq!(state.spool_owner_prev(SpoolIndex(0)), Some(prev_owner));
        assert_eq!(state.group_member_count_prev(GroupIndex(0)), 1);
    }

    #[test]
    fn find_member_next_uses_next_committee() {
        let state = state_with_groups();
        let member = state.find_member_next(address(9)).unwrap();
        assert_eq!(member.node, address(9));
    }

    // unverified state is infinitely old however early in the process it is read
    #[test]
    fn freshness_starts_expired() {
        let state = ProtocolState::default();
        assert_eq!(state.age(), Duration::MAX);

        state.touch();
        assert!(state.age() < Duration::from_secs(1));

        state.invalidate();
        assert_eq!(state.age(), Duration::MAX);
    }

    // a clone carries the mark rather than reading as freshly verified
    #[test]
    fn freshness_survives_a_clone() {
        let state = ProtocolState::default();
        state.touch();
        assert!(state.clone().age() < Duration::from_secs(1));

        state.invalidate();
        assert_eq!(state.clone().age(), Duration::MAX);
    }
}
