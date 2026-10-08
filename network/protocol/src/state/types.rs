use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytemuck::Zeroable;
use tape_api::state::{Epoch, Group, System};
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup, VIRTUAL_GROUP_COUNT};
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

    /// Where each virtual group lives in the current epoch, once a group is live.
    pub fn fold(&self) -> Option<GroupFold> {
        self.current.fold()
    }

    /// Where each virtual group lived in the previous epoch.
    pub fn fold_prev(&self) -> Option<GroupFold> {
        self.previous.as_ref()?.fold()
    }

    /// Where each virtual group lived as of `epoch`, current or previous
    pub fn fold_at(&self, epoch: EpochNumber) -> Option<GroupFold> {
        Some(self.groups_at(epoch)?.fold)
    }

    /// Where each virtual group will live at the next epoch's group count.
    pub fn fold_next(&self) -> Option<GroupFold> {
        GroupFold::new(self.system.target_group_count)
    }

    /// The live group that carries a virtual group in the current epoch.
    pub fn live_group(&self, group: VirtualGroup) -> Option<GroupIndex> {
        self.current_groups()?.live_group(group)
    }

    /// A live group's own virtual group, or nothing for a group that isn't
    /// live in the current epoch.
    pub fn virtual_group(&self, group: GroupIndex) -> Option<VirtualGroup> {
        self.fold()?.virtual_group(group)
    }

    /// A live group's own virtual group as of `epoch`, current or previous
    pub fn virtual_group_at(&self, epoch: EpochNumber, group: GroupIndex) -> Option<VirtualGroup> {
        self.fold_at(epoch)?.virtual_group(group)
    }

    /// List the stored spools a live spool carries in the current epoch, its own first.
    pub fn carried_spools(&self, spool: SpoolIndex) -> Vec<SpoolIndex> {
        self.fold()
            .map(|fold| fold.carried_spools(spool))
            .unwrap_or_default()
    }

    /// Find a current-epoch group account by index.
    pub fn group(&self, group: GroupIndex) -> Option<&Group> {
        group_inner(&self.current.groups, group)
    }

    /// Find a previous-epoch group account by index.
    pub fn group_prev(&self, group: GroupIndex) -> Option<&Group> {
        group_inner(&self.previous.as_ref()?.groups, group)
    }

    /// Find the current-epoch assignment that holds a stored spool.
    pub fn spool(&self, spool: SpoolIndex) -> Option<&Spool> {
        self.current_groups()?.spool(spool)
    }

    /// Find the previous-epoch assignment that held a stored spool.
    pub fn spool_prev(&self, spool: SpoolIndex) -> Option<&Spool> {
        self.previous_groups()?.spool(spool)
    }

    /// Iterate a virtual group's stored spools with their current-epoch assignments.
    pub fn spools_in_group(
        &self,
        group: VirtualGroup,
    ) -> Option<impl Iterator<Item = (SpoolIndex, &Spool)> + '_> {
        self.current_groups()?.spools_in_group(group)
    }

    /// Iterate a virtual group's stored spools with their previous-epoch assignments.
    pub fn spools_in_group_prev(
        &self,
        group: VirtualGroup,
    ) -> Option<impl Iterator<Item = (SpoolIndex, &Spool)> + '_> {
        self.previous_groups()?.spools_in_group(group)
    }

    /// Find the stored spool of a virtual group that a node holds in the current epoch.
    pub fn spool_for_node_in_group(
        &self,
        group: VirtualGroup,
        node: Address,
    ) -> Option<(SpoolIndex, &Spool)> {
        self.current_groups()?.spool_for_node_in_group(group, node)
    }

    /// Find the stored spool of a virtual group that a node held in the previous epoch.
    pub fn spool_for_node_in_group_prev(
        &self,
        group: VirtualGroup,
        node: Address,
    ) -> Option<(SpoolIndex, &Spool)> {
        self.previous_groups()?.spool_for_node_in_group(group, node)
    }

    /// Which node holds this stored spool in the current epoch?
    pub fn spool_owner(&self, spool: SpoolIndex) -> Option<Address> {
        self.spool(spool)
            .and_then(|spool| assigned_node(spool.node))
    }

    /// Which node held this stored spool in the previous epoch?
    pub fn spool_owner_prev(&self, spool: SpoolIndex) -> Option<Address> {
        self.spool_prev(spool)
            .and_then(|spool| assigned_node(spool.node))
    }

    /// All stored spools a node carries in the current epoch: one per track
    /// group folded onto each live spool it is assigned.
    pub fn member_spools(&self, node: Address) -> Vec<SpoolIndex> {
        self.current_groups()
            .map(|groups| groups.member_spools(node))
            .unwrap_or_default()
    }

    /// All stored spools a node carried in the previous epoch.
    pub fn member_spools_prev(&self, node: Address) -> Vec<SpoolIndex> {
        self.previous_groups()
            .map(|groups| groups.member_spools(node))
            .unwrap_or_default()
    }

    /// The live spools assigned to a node in the current epoch, as the chain counts them.
    pub fn member_live_spools(&self, node: Address) -> Vec<SpoolIndex> {
        self.current_groups()
            .map(|groups| groups.member_live_spools(node))
            .unwrap_or_default()
    }

    /// Map each stored spool of a virtual group to the node that holds it.
    pub fn group_peers(&self, group: VirtualGroup) -> Vec<(SpoolIndex, Address)> {
        group_peers_inner(self.spools_in_group(group))
    }

    /// Map each stored spool of a virtual group to the node that held it in the previous epoch.
    pub fn group_peers_prev(&self, group: VirtualGroup) -> Vec<(SpoolIndex, Address)> {
        group_peers_inner(self.spools_in_group_prev(group))
    }

    /// Every node that may hold a virtual group's data: this epoch's holders,
    /// then last epoch's.
    ///
    /// A node that held the group last epoch keeps serving it for one more,
    /// which covers a reader while the new holders pull.
    pub fn group_peers_with_prev(&self, group: VirtualGroup) -> Vec<(SpoolIndex, Address)> {
        let mut peers = self.group_peers(group);
        for peer in self.group_peers_prev(group) {
            if !peers.contains(&peer) {
                peers.push(peer);
            }
        }
        peers
    }

    /// Count unique nodes that hold a virtual group in the current epoch.
    pub fn group_member_count(&self, group: VirtualGroup) -> usize {
        self.current_groups()
            .map(|groups| groups.member_count(group))
            .unwrap_or_default()
    }

    /// Count unique nodes that held a virtual group in the previous epoch.
    pub fn group_member_count_prev(&self, group: VirtualGroup) -> usize {
        self.previous_groups()
            .map(|groups| groups.member_count(group))
            .unwrap_or_default()
    }

    fn current_groups(&self) -> Option<EpochGroups<'_>> {
        EpochGroups::of(&self.current)
    }

    fn previous_groups(&self) -> Option<EpochGroups<'_>> {
        EpochGroups::of(self.previous.as_ref()?)
    }

    /// The groups as of `epoch`, or nothing once the state no longer holds it
    fn groups_at(&self, epoch: EpochNumber) -> Option<EpochGroups<'_>> {
        if epoch == self.epoch() {
            return self.current_groups();
        }
        match &self.previous {
            Some(previous) if previous.epoch.id == epoch => self.previous_groups(),
            Some(_) | None => None,
        }
    }
}

impl EpochBundle {
    /// Where each virtual group lives in this epoch, once a group is live.
    ///
    /// The epoch's own group count decides it.
    pub fn fold(&self) -> Option<GroupFold> {
        GroupFold::new(self.epoch.total_groups)
    }
}

/// One epoch's group accounts with the fold that routes a virtual group to them
#[derive(Clone, Copy)]
struct EpochGroups<'a> {
    groups: &'a [Group],
    fold: GroupFold,
}

impl<'a> EpochGroups<'a> {
    fn of(bundle: &'a EpochBundle) -> Option<Self> {
        Some(Self {
            groups: &bundle.groups,
            fold: bundle.fold()?,
        })
    }

    // No leaf carries a virtual group past the range, so nobody holds one.
    fn live_group(&self, group: VirtualGroup) -> Option<GroupIndex> {
        (group.0 < VIRTUAL_GROUP_COUNT).then(|| self.fold.live_group(group))
    }

    // A group's array is as wide as the widest group. A network with n spools per
    // group assigns the first n and leaves the rest zeroed, so skipping unassigned
    // positions stops every walk at n.
    fn spool(&self, spool: SpoolIndex) -> Option<&'a Spool> {
        let group = VirtualGroup::containing(spool);
        let position = group.position_of(spool)?;
        group_inner(self.groups, self.live_group(group)?)?
            .spools
            .get(position)
            .filter(|spool| assigned_node(spool.node).is_some())
    }

    // The live group's assignments, each under the virtual group's own spool index.
    fn spools_in_group(
        &self,
        group: VirtualGroup,
    ) -> Option<impl Iterator<Item = (SpoolIndex, &'a Spool)> + 'a> {
        Some(
            group_inner(self.groups, self.live_group(group)?)?
                .spools
                .iter()
                .enumerate()
                .filter(|(_, spool)| assigned_node(spool.node).is_some())
                .map(move |(position, spool)| (group.spool_at(position), spool)),
        )
    }

    fn spool_for_node_in_group(
        &self,
        group: VirtualGroup,
        node: Address,
    ) -> Option<(SpoolIndex, &'a Spool)> {
        assigned_node(node)?;

        self.spools_in_group(group)?
            .find(|(_, spool)| spool.node == node)
    }

    fn member_live_spools(&self, node: Address) -> Vec<SpoolIndex> {
        if assigned_node(node).is_none() {
            return Vec::new();
        }

        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .spools
                    .iter()
                    .enumerate()
                    .filter(move |(_, spool)| spool.node == node)
                    .map(move |(position, _)| group.id.spool_at(position))
            })
            .collect()
    }

    fn member_spools(&self, node: Address) -> Vec<SpoolIndex> {
        self.member_live_spools(node)
            .into_iter()
            .flat_map(|spool| self.fold.carried_spools(spool))
            .collect()
    }

    fn member_count(&self, group: VirtualGroup) -> usize {
        self.live_group(group)
            .and_then(|live| group_inner(self.groups, live))
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
}

fn group_inner(groups: &[Group], group: GroupIndex) -> Option<&Group> {
    groups.iter().find(|candidate| candidate.id == group)
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
    use tape_core::erasure::MAX_GROUP_SIZE;
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
        group_of(epoch, id, owners, MAX_GROUP_SIZE)
    }

    /// A group whose first `width` spools cycle through `owners`.
    fn group_of(epoch: EpochNumber, id: GroupIndex, owners: &[Address], width: usize) -> Group {
        let mut group = Group {
            epoch,
            id,
            ..Group::zeroed()
        };
        for i in 0..width {
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
                    total_groups: 1,
                    ..Epoch::zeroed()
                },
                committee: vec![member(a, 100), member(b, 90), member(c, 80)],
                groups: vec![group(epoch, GroupIndex(0), &[a, b, c])],
            },
            next_committee: Some(vec![member(address(9), 50)]),
            ..ProtocolState::default()
        }
    }

    /// An epoch of `count` groups, where group g is held by the one node
    /// `address(base + g)`.
    fn bundle_of(epoch: EpochNumber, count: u64, base: u8) -> EpochBundle {
        EpochBundle {
            epoch: Epoch {
                id: epoch,
                total_groups: count,
                ..Epoch::zeroed()
            },
            committee: Vec::new(),
            groups: (0..count)
                .map(|id| group(epoch, GroupIndex(id), &[address(base + id as u8)]))
                .collect(),
        }
    }

    /// Ten groups last epoch, five now, with different nodes in each epoch.
    fn shrunk_state() -> ProtocolState {
        ProtocolState {
            system: System {
                live_group_count: 5,
                previous_group_count: 10,
                ..System::zeroed()
            },
            current: bundle_of(EpochNumber(5), 5, 100),
            previous: Some(bundle_of(EpochNumber(4), 10, 200)),
            ..ProtocolState::default()
        }
    }

    // a virtual group past the live count is held by the live group that carries it, under its own spools
    #[test]
    fn retired_group_peers() {
        let state = shrunk_state();
        let retired = VirtualGroup(7);

        assert_eq!(state.live_group(retired), Some(GroupIndex(3)));
        assert_eq!(state.live_group(VirtualGroup(VIRTUAL_GROUP_COUNT)), None);

        let peers = state.group_peers(retired);
        assert_eq!(peers.len(), MAX_GROUP_SIZE);
        assert_eq!(peers[0], (retired.spool_at(0), address(103)));
        assert_eq!(peers[19], (retired.spool_at(19), address(103)));

        assert_eq!(state.spool_owner(retired.spool_at(4)), Some(address(103)));
        assert_eq!(state.group_member_count(retired), 1);
        assert_eq!(
            state.spool_for_node_in_group(retired, address(103)).map(|(spool, _)| spool),
            Some(retired.spool_at(0)),
        );
        assert!(state.spool_for_node_in_group(retired, address(107)).is_none());
    }

    // each epoch routes by its own group count
    #[test]
    fn previous_epoch_folds_at_its_count() {
        let state = shrunk_state();
        let retired = VirtualGroup(7);
        let spool = retired.spool_at(4);

        assert_eq!(state.spool_owner_prev(spool), Some(address(207)));
        assert_eq!(state.group_peers_prev(retired)[0], (retired.spool_at(0), address(207)));

        // Group 7 was live last epoch and is gone now.
        let gone = GroupIndex(7);
        assert_eq!(state.virtual_group_at(EpochNumber(4), gone), Some(retired));
        assert_eq!(state.virtual_group_at(EpochNumber(5), gone), None);
        assert_eq!(state.virtual_group(GroupIndex(3)), Some(VirtualGroup(3)));

        // A reader asks today's holders first, then the ones that held it last epoch.
        let holders = state.group_peers_with_prev(retired);
        assert_eq!(holders.len(), 2 * MAX_GROUP_SIZE);
        assert_eq!(holders[0], (retired.spool_at(0), address(103)));
        assert_eq!(holders[MAX_GROUP_SIZE], (retired.spool_at(0), address(207)));
    }

    // a group whose holders stayed put lists each of them once
    #[test]
    fn unmoved_holders_listed_once() {
        let mut state = shrunk_state();
        state.previous = Some(bundle_of(EpochNumber(4), 5, 100));

        assert_eq!(state.group_peers_with_prev(VirtualGroup(2)).len(), MAX_GROUP_SIZE);
    }

    // a node stores every virtual group folded onto the spools it is assigned
    #[test]
    fn member_spools_carry_retired_groups() {
        let state = shrunk_state();
        let node = address(103);

        let live = state.member_live_spools(node);
        assert_eq!(live.len(), MAX_GROUP_SIZE);
        assert!(live.iter().all(|spool| GroupIndex(3).contains(*spool)));

        // Group 3 carries 512 of the virtual groups while five groups are live.
        let stored = state.member_spools(node);
        assert_eq!(stored.len(), 512 * MAX_GROUP_SIZE);
        assert!(stored.contains(&VirtualGroup(3).spool_at(6)));
        assert!(stored.contains(&VirtualGroup(7).spool_at(6)));
        assert!(!stored.contains(&VirtualGroup(6).spool_at(6)));

        let carried = state.carried_spools(GroupIndex(3).spool_at(6));
        assert_eq!(carried.len(), 512);
        assert_eq!(carried[..2], [VirtualGroup(3).spool_at(6), VirtualGroup(7).spool_at(6)]);

        // Group 7 carried 256 of them while ten were live.
        assert_eq!(state.member_spools_prev(address(207)).len(), 256 * MAX_GROUP_SIZE);
    }

    // a group that is new this epoch was carried last epoch, by a node of its carrier
    #[test]
    fn new_group_was_carried() {
        let mut state = shrunk_state();
        state.current = bundle_of(EpochNumber(5), 10, 100);
        state.previous = Some(bundle_of(EpochNumber(4), 5, 200));

        let split = VirtualGroup(7);
        assert_eq!(state.spool_owner(split.spool_at(0)), Some(address(107)));
        assert_eq!(state.spool_owner_prev(split.spool_at(0)), Some(address(203)));
        assert_eq!(state.group_peers_prev(split)[0], (split.spool_at(0), address(203)));
        assert_eq!(state.group_peers_with_prev(split).len(), 2 * MAX_GROUP_SIZE);
        assert!(state.member_spools_prev(address(203)).contains(&split.spool_at(0)));
    }

    // a group that returns is routed to itself again
    #[test]
    fn regrown_group() {
        let mut state = shrunk_state();
        state.current = bundle_of(EpochNumber(5), 10, 100);
        state.system.live_group_count = 10;

        assert_eq!(state.live_group(VirtualGroup(7)), Some(GroupIndex(7)));
        assert_eq!(state.spool_owner(VirtualGroup(7).spool_at(4)), Some(address(107)));
        assert_eq!(state.member_spools(address(103)).len(), 256 * MAX_GROUP_SIZE);
        assert!(!state.member_spools(address(103)).contains(&VirtualGroup(7).spool_at(4)));
    }

    // a state missing some of the epoch's groups finds no holder for them
    #[test]
    fn missing_group_has_no_holder() {
        let mut state = shrunk_state();
        state.current.groups.truncate(3);

        assert!(state.group_peers(VirtualGroup(7)).is_empty());
        assert!(state.spool_owner(VirtualGroup(4).spool_at(0)).is_none());
        assert_eq!(state.spool_owner(VirtualGroup(6).spool_at(0)), Some(address(101)));
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
        let peers = state.group_peers(VirtualGroup(0));
        assert_eq!(peers.len(), MAX_GROUP_SIZE);
        assert_eq!(peers[0], (SpoolIndex(0), address(1)));
        assert_eq!(peers[1], (SpoolIndex(1), address(2)));
    }

    // a 4-wide network's group walks stop at its four assigned spools
    #[test]
    fn narrow_group() {
        let epoch = EpochNumber(5);
        let owners = [address(1), address(2), address(3), address(4)];
        let second = VirtualGroup(1);
        let state = ProtocolState {
            current: EpochBundle {
                epoch: Epoch { id: epoch, total_groups: 2, ..Epoch::zeroed() },
                committee: owners.iter().map(|owner| member(*owner, 100)).collect(),
                groups: vec![
                    group_of(epoch, GroupIndex(0), &owners, 4),
                    group_of(epoch, GroupIndex(1), &owners, 4),
                ],
            },
            ..ProtocolState::default()
        };

        let spools: Vec<SpoolIndex> = state
            .spools_in_group(second)
            .expect("group")
            .map(|(spool, _)| spool)
            .collect();

        assert_eq!(spools, vec![SpoolIndex(20), SpoolIndex(21), SpoolIndex(22), SpoolIndex(23)]);
        assert_eq!(state.group_peers(second).len(), 4);
        assert_eq!(state.group_member_count(second), 4);
        assert_eq!(state.member_live_spools(address(1)), vec![SpoolIndex(0), SpoolIndex(20)]);
        // One position in each of the two live groups, which carry every virtual group between them.
        assert_eq!(state.member_spools(address(1)).len() as u64, VIRTUAL_GROUP_COUNT);
        assert_eq!(state.spool_owner(SpoolIndex(23)), Some(address(4)));
        assert!(state.spool(SpoolIndex(24)).is_none());
        assert!(state.spool_owner(SpoolIndex(24)).is_none());
    }

    #[test]
    fn member_spools_uses_group_ownership() {
        let state = state_with_groups();
        let live = vec![
            SpoolIndex(0),
            SpoolIndex(3),
            SpoolIndex(6),
            SpoolIndex(9),
            SpoolIndex(12),
            SpoolIndex(15),
            SpoolIndex(18),
        ];
        assert_eq!(state.member_live_spools(address(1)), live);

        // The one live group carries every virtual group, at each of those positions.
        let stored = state.member_spools(address(1));
        assert_eq!(stored.len() as u64, live.len() as u64 * VIRTUAL_GROUP_COUNT);
        assert!(stored.contains(&VirtualGroup(1_000).spool_at(3)));
        assert!(!stored.contains(&VirtualGroup(1_000).spool_at(4)));
    }

    #[test]
    fn group_member_count_counts_unique_addresses() {
        let state = state_with_groups();
        assert_eq!(state.group_member_count(VirtualGroup(0)), 3);
    }

    #[test]
    fn default_address_is_not_treated_as_owner() {
        let epoch = EpochNumber(5);
        let state = ProtocolState {
            current: EpochBundle {
                epoch: Epoch {
                    id: epoch,
                    total_groups: 1,
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
        assert!(state.member_live_spools(Address::default()).is_empty());
        assert!(state.group_peers(VirtualGroup(0)).is_empty());
        assert_eq!(state.group_member_count(VirtualGroup(0)), 0);
    }

    #[test]
    fn previous_helpers_use_previous_bundle() {
        let prev_epoch = EpochNumber(4);
        let prev_owner = address(8);
        let mut state = state_with_groups();
        state.previous = Some(EpochBundle {
            epoch: Epoch {
                id: prev_epoch,
                total_groups: 1,
                ..Epoch::zeroed()
            },
            committee: vec![member(prev_owner, 10)],
            groups: vec![group(prev_epoch, GroupIndex(0), &[prev_owner])],
        });

        assert_eq!(state.spool_owner_prev(SpoolIndex(0)), Some(prev_owner));
        assert_eq!(state.group_member_count_prev(VirtualGroup(0)), 1);
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
