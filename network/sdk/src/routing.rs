//! Slice routing for distributed storage.
//!
//! This module maps slice indices to storage node addresses using the
//! on-chain committee and spool assignment state.
//!
//! # Overview
//!
//! Each slice is assigned to a "spool" (0-1023), and each spool is owned
//! by a committee member. The routing flow is:
//!
//! 1. `slice_index` → `spool_index` (identity - they're the same)
//! 2. `spool_index` → `member_index` via `SpoolAssignment`
//! 3. `member_index` → `NodeId` via `Committee`
//! 4. `NodeId` → `NetworkAddress` via on-chain Node account lookup
//!
//! # Usage
//!
//! ```rust,ignore
//! use tape_sdk::routing::SliceRouter;
//!
//! // Build router from on-chain state
//! let system = rpc_client.get_system().await?;
//! let router = SliceRouter::from_system(&system);
//!
//! // Route a slice to find which member owns it
//! let member_idx = router.member_for_slice(42);
//! let node_id = router.node_id_for_slice(42);
//! ```

use std::net::SocketAddr;

use tape_core::erasure::SLICE_COUNT;
use tape_core::spooler::SpoolAssignment;
use tape_core::system::{Committee, CommitteeMember};
use tape_core::types::{NetworkAddress, NodeId};

/// Error types for routing operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingError {
    /// The slice index is out of range (must be < SLICE_COUNT).
    InvalidSliceIndex(u16),
    /// The member index from spool assignment points to an invalid committee slot.
    InvalidMemberIndex(u8),
    /// Committee member not found at the expected index.
    MemberNotFound(usize),
    /// The node address could not be resolved.
    AddressResolutionFailed(NodeId),
    /// The network address is invalid or zeroed.
    InvalidNetworkAddress,
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSliceIndex(idx) => write!(f, "invalid slice index: {} (max {})", idx, SLICE_COUNT - 1),
            Self::InvalidMemberIndex(idx) => write!(f, "invalid member index: {}", idx),
            Self::MemberNotFound(idx) => write!(f, "committee member not found at index {}", idx),
            Self::AddressResolutionFailed(id) => write!(f, "failed to resolve address for node {:?}", id),
            Self::InvalidNetworkAddress => write!(f, "network address is invalid or zeroed"),
        }
    }
}

impl std::error::Error for RoutingError {}

/// Routes slices to storage nodes based on spool assignment.
///
/// This struct holds a snapshot of the committee and spool assignment state,
/// providing efficient slice → node routing without RPC calls.
///
/// The router must be refreshed when the committee changes (epoch transitions).
#[derive(Clone)]
pub struct SliceRouter<const MEMBERS: usize> {
    /// Maps spool_index → member_index
    spool_assignment: SpoolAssignment<SLICE_COUNT>,
    /// Committee with member NodeIds
    committee: Committee<MEMBERS>,
    /// Cached network addresses indexed by member_index.
    /// None if not yet resolved.
    cached_addresses: Vec<Option<NetworkAddress>>,
}

impl<const MEMBERS: usize> SliceRouter<MEMBERS> {
    /// Create a router from System state components.
    ///
    /// # Arguments
    /// * `spool_assignment` - Maps each spool (slice) to a committee member index
    /// * `committee` - The active committee with member NodeIds
    ///
    /// The router is created without cached addresses. Use `set_address()` or
    /// `set_addresses()` to populate the cache for efficient routing.
    pub fn new(
        spool_assignment: SpoolAssignment<SLICE_COUNT>,
        committee: Committee<MEMBERS>,
    ) -> Self {
        Self {
            spool_assignment,
            committee,
            cached_addresses: vec![None; MEMBERS],
        }
    }

    /// Get the member index that owns a given slice.
    ///
    /// # Arguments
    /// * `slice_index` - The slice index (0 to SLICE_COUNT-1)
    ///
    /// # Returns
    /// The committee member index that owns this slice's spool.
    pub fn member_for_slice(&self, slice_index: u16) -> Result<usize, RoutingError> {
        if slice_index as usize >= SLICE_COUNT {
            return Err(RoutingError::InvalidSliceIndex(slice_index));
        }
        let member_idx = self.spool_assignment.0[slice_index as usize];
        Ok(member_idx as usize)
    }

    /// Get the CommitteeMember that owns a given slice.
    ///
    /// # Arguments
    /// * `slice_index` - The slice index (0 to SLICE_COUNT-1)
    ///
    /// # Returns
    /// The CommitteeMember that owns this slice's spool.
    pub fn committee_member_for_slice(&self, slice_index: u16) -> Result<CommitteeMember, RoutingError> {
        let member_idx = self.member_for_slice(slice_index)?;
        self.committee
            .member_at(member_idx)
            .ok_or(RoutingError::MemberNotFound(member_idx))
    }

    /// Get the NodeId that should store a given slice.
    ///
    /// # Arguments
    /// * `slice_index` - The slice index (0 to SLICE_COUNT-1)
    ///
    /// # Returns
    /// The NodeId of the storage node responsible for this slice.
    pub fn node_id_for_slice(&self, slice_index: u16) -> Result<NodeId, RoutingError> {
        let member = self.committee_member_for_slice(slice_index)?;
        Ok(member.id)
    }

    /// Get all slices owned by a specific committee member.
    ///
    /// # Arguments
    /// * `member_index` - The committee member index
    ///
    /// # Returns
    /// Vector of slice indices owned by this member.
    pub fn slices_for_member(&self, member_index: usize) -> Vec<u16> {
        self.spool_assignment
            .spools_for_member(member_index)
            .into_iter()
            .map(|s| s as u16)
            .collect()
    }

    /// Get the number of active committee members.
    pub fn committee_size(&self) -> usize {
        self.committee.size()
    }

    /// Iterate over all committee members.
    pub fn committee_members(&self) -> impl Iterator<Item = &CommitteeMember> {
        self.committee.iter()
    }

    // =========================================================================
    // Address caching
    // =========================================================================

    /// Set the cached network address for a committee member.
    ///
    /// # Arguments
    /// * `member_index` - The committee member index
    /// * `address` - The resolved NetworkAddress
    pub fn set_address(&mut self, member_index: usize, address: NetworkAddress) {
        if member_index < self.cached_addresses.len() {
            self.cached_addresses[member_index] = Some(address);
        }
    }

    /// Set cached addresses for all members at once.
    ///
    /// # Arguments
    /// * `addresses` - Vector of (member_index, NetworkAddress) pairs
    pub fn set_addresses(&mut self, addresses: impl IntoIterator<Item = (usize, NetworkAddress)>) {
        for (idx, addr) in addresses {
            self.set_address(idx, addr);
        }
    }

    /// Get the cached network address for a committee member.
    ///
    /// Returns None if the address hasn't been cached yet.
    pub fn get_cached_address(&self, member_index: usize) -> Option<&NetworkAddress> {
        self.cached_addresses.get(member_index)?.as_ref()
    }

    /// Get the socket address for a given slice (if cached).
    ///
    /// # Arguments
    /// * `slice_index` - The slice index (0 to SLICE_COUNT-1)
    ///
    /// # Returns
    /// The SocketAddr of the storage node, or an error if not cached or invalid.
    pub fn socket_addr_for_slice(&self, slice_index: u16) -> Result<SocketAddr, RoutingError> {
        let member_idx = self.member_for_slice(slice_index)?;
        let addr = self
            .get_cached_address(member_idx)
            .ok_or(RoutingError::AddressResolutionFailed(
                self.committee
                    .member_at(member_idx)
                    .map(|m| m.id)
                    .unwrap_or(NodeId::new(0)),
            ))?;

        addr.to_socket_addr()
            .map_err(|_| RoutingError::InvalidNetworkAddress)
    }

    /// Check if all active committee members have cached addresses.
    pub fn is_fully_cached(&self) -> bool {
        let size = self.committee.size();
        for i in 0..size {
            if self.cached_addresses.get(i).map(|a| a.is_none()).unwrap_or(true) {
                return false;
            }
        }
        true
    }

    /// Get members that don't have cached addresses.
    pub fn uncached_members(&self) -> Vec<(usize, NodeId)> {
        let mut uncached = Vec::new();
        for i in 0..self.committee.size() {
            if self.cached_addresses.get(i).map(|a| a.is_none()).unwrap_or(true) {
                if let Some(member) = self.committee.member_at(i) {
                    uncached.push((i, member.id));
                }
            }
        }
        uncached
    }

    // =========================================================================
    // Bulk routing for upload/download
    // =========================================================================

    /// Group slices by the member that owns them.
    ///
    /// Returns a map of member_index → list of slice indices.
    /// Useful for batching uploads/downloads to the same node.
    pub fn group_slices_by_member(&self) -> Vec<(usize, Vec<u16>)> {
        let mut groups: Vec<Vec<u16>> = vec![Vec::new(); MEMBERS];

        for slice_idx in 0..SLICE_COUNT as u16 {
            if let Ok(member_idx) = self.member_for_slice(slice_idx) {
                if member_idx < groups.len() {
                    groups[member_idx].push(slice_idx);
                }
            }
        }

        groups
            .into_iter()
            .enumerate()
            .filter(|(_, slices)| !slices.is_empty())
            .collect()
    }

    /// Get all unique members and their slice counts.
    ///
    /// Returns Vec of (member_index, node_id, slice_count).
    pub fn member_slice_counts(&self) -> Vec<(usize, NodeId, usize)> {
        let mut counts = vec![0usize; MEMBERS];

        for slice_idx in 0..SLICE_COUNT {
            let member_idx = self.spool_assignment.0[slice_idx] as usize;
            if member_idx < counts.len() {
                counts[member_idx] += 1;
            }
        }

        counts
            .into_iter()
            .enumerate()
            .filter(|(_, count)| *count > 0)
            .filter_map(|(idx, count)| {
                self.committee.member_at(idx).map(|m| (idx, m.id, count))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::{Coin, TAPE};

    fn make_committee<const N: usize>(count: usize) -> Committee<N> {
        let mut committee = Committee::new();
        for i in 0..count.min(N) {
            let member = CommitteeMember::new(
                NodeId::new(i as u64 + 1),
                Coin::<TAPE>::new(1000 - i as u64), // Descending stake
            );
            let _ = committee.try_join(&member);
        }
        committee
    }

    fn make_uniform_assignment(member_count: usize) -> SpoolAssignment<SLICE_COUNT> {
        let mut spools = [0u8; SLICE_COUNT];
        for i in 0..SLICE_COUNT {
            spools[i] = (i % member_count) as u8;
        }
        SpoolAssignment::new(spools)
    }

    #[test]
    fn test_member_for_slice() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let router = SliceRouter::new(assignment, committee);

        // Slices should round-robin across 3 members
        assert_eq!(router.member_for_slice(0).unwrap(), 0);
        assert_eq!(router.member_for_slice(1).unwrap(), 1);
        assert_eq!(router.member_for_slice(2).unwrap(), 2);
        assert_eq!(router.member_for_slice(3).unwrap(), 0);
        assert_eq!(router.member_for_slice(1023).unwrap(), 1023 % 3);
    }

    #[test]
    fn test_invalid_slice_index() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let router = SliceRouter::new(assignment, committee);

        let result = router.member_for_slice(1024);
        assert!(matches!(result, Err(RoutingError::InvalidSliceIndex(1024))));
    }

    #[test]
    fn test_node_id_for_slice() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let router = SliceRouter::new(assignment, committee);

        // NodeIds are 1, 2, 3 (1-indexed in our test)
        assert_eq!(router.node_id_for_slice(0).unwrap(), NodeId::new(1));
        assert_eq!(router.node_id_for_slice(1).unwrap(), NodeId::new(2));
        assert_eq!(router.node_id_for_slice(2).unwrap(), NodeId::new(3));
    }

    #[test]
    fn test_slices_for_member() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let router = SliceRouter::new(assignment, committee);

        let member_0_slices = router.slices_for_member(0);
        let member_1_slices = router.slices_for_member(1);

        // Each of 3 members should have ~f slices (SLICE_COUNT / num_members)
        assert!(member_0_slices.len() >= PARITY_SLICES);
        assert!(member_1_slices.len() >= PARITY_SLICES);

        // First few slices should be correct
        assert!(member_0_slices.contains(&0));
        assert!(member_0_slices.contains(&3));
        assert!(member_1_slices.contains(&1));
        assert!(member_1_slices.contains(&4));
    }

    #[test]
    fn test_address_caching() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let mut router = SliceRouter::new(assignment, committee);

        // Initially no addresses cached
        assert!(!router.is_fully_cached());
        assert_eq!(router.uncached_members().len(), 3);

        // Cache an address
        let addr = NetworkAddress::from("127.0.0.1:8080").unwrap();
        router.set_address(0, addr);

        assert!(router.get_cached_address(0).is_some());
        assert!(router.get_cached_address(1).is_none());

        // Socket addr for slice 0 should work now
        let sock = router.socket_addr_for_slice(0).unwrap();
        assert_eq!(sock.port(), 8080);

        // Slice 1 (member 1) should fail
        assert!(router.socket_addr_for_slice(1).is_err());
    }

    #[test]
    fn test_group_slices_by_member() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let router = SliceRouter::new(assignment, committee);

        let groups = router.group_slices_by_member();

        // Should have 3 groups (one per member)
        assert_eq!(groups.len(), 3);

        // Total slices across all groups should equal SLICE_COUNT
        let total: usize = groups.iter().map(|(_, slices)| slices.len()).sum();
        assert_eq!(total, SLICE_COUNT);
    }

    #[test]
    fn test_member_slice_counts() {
        let committee = make_committee::<10>(3);
        let assignment = make_uniform_assignment(3);
        let router = SliceRouter::new(assignment, committee);

        let counts = router.member_slice_counts();

        // Should have 3 entries
        assert_eq!(counts.len(), 3);

        // Each member should have roughly equal slices
        for (_, _, count) in &counts {
            assert!(*count >= 340 && *count <= 342);
        }

        // Total should be SLICE_COUNT
        let total: usize = counts.iter().map(|(_, _, c)| *c).sum();
        assert_eq!(total, SLICE_COUNT);
    }

    #[test]
    fn test_committee_size() {
        let committee = make_committee::<10>(5);
        let assignment = make_uniform_assignment(5);
        let router = SliceRouter::new(assignment, committee);

        assert_eq!(router.committee_size(), 5);
    }
}
