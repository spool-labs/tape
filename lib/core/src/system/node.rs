use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_crypto::address::Address;

use crate::bft::{quorum_above, quorum_below};
use crate::bls::BlsPubkey;
use crate::types::*;

use super::{Member, Peer};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct NodeMetadata {
    /// The name of this node storage node.
    pub name: [u8; 32],

    /// The advertised network address of the node.
    pub network_address: NetworkAddress,

    /// The TLS public key of this node.
    pub network_tls: NetworkTlsPubkey,

    /// The BLS public key of this node.
    pub bls_pubkey: BlsPubkey,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Zeroable, Pod, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct NodePreferences {
    /// The preferred capacity of new committees.
    pub committee_size: u64,

    /// The preferred number of spool groups per epoch.
    pub spool_groups: u64,

    /// The preferred storage payment burn rate in basis points.
    pub burn_fee_bps: BasisPoints,

    /// The preferred subsidy decay rate in basis points per epoch.
    pub subsidy_decay_bps: BasisPoints,

    /// The preferred total archive size.
    pub network_capacity: StorageUnits,

    /// The preferred price per storage unit.
    pub storage_price: Coin<TAPE>,

    /// Minimum staked Node balance required for read access.
    pub access_threshold: Coin<TAPE>,

    /// The preferred epoch duration in seconds.
    /// Reference targets: ~20s simnet, ~100s localnet, ~3_600s devnet, ~604_800s mainnet.
    pub epoch_duration: EpochDuration,

    /// The largest spool this node wants to hold; zero means no preference.
    /// Operators compare the voted value with the real spool sizes to see
    /// when the group count should go up. No instruction reads it.
    pub spool_size: StorageUnits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodePreferenceAggregationError {
    MissingPeer { node: Address },
    ZeroWeight,
}

pub fn aggregate_node_preferences(
    members: &[Member],
    peers: &[Peer],
    current: NodePreferences,
    bounds: NodePreferences,
) -> Result<NodePreferences, NodePreferenceAggregationError> {
    let mut total_weight: u64 = 0;
    let mut committee_sizes: Vec<(u64, u64)> = Vec::new();
    let mut spool_group_counts: Vec<(u64, u64)> = Vec::new();
    let mut burn_fee_bps: Vec<(u64, u64)> = Vec::new();
    let mut subsidy_decay_bps: Vec<(u64, u64)> = Vec::new();
    let mut network_capacities: Vec<(u64, u64)> = Vec::new();
    let mut storage_prices: Vec<(u64, u64)> = Vec::new();
    let mut access_thresholds: Vec<(u64, u64)> = Vec::new();
    let mut epoch_durations: Vec<(u64, u64)> = Vec::new();
    let mut spool_sizes: Vec<(u64, u64)> = Vec::new();

    for member in members.iter() {
        let peer = peers
            .iter()
            .find(|p| p.node == member.node)
            .ok_or(NodePreferenceAggregationError::MissingPeer { node: member.node })?;
        let weight = member.spools;

        committee_sizes.push((peer.preferences.committee_size, weight));
        spool_group_counts.push((peer.preferences.spool_groups, weight));
        burn_fee_bps.push((peer.preferences.burn_fee_bps.0, weight));
        subsidy_decay_bps.push((peer.preferences.subsidy_decay_bps.0, weight));
        network_capacities.push((peer.preferences.network_capacity.0, weight));
        storage_prices.push((peer.preferences.storage_price.0, weight));
        access_thresholds.push((peer.preferences.access_threshold.0, weight));
        epoch_durations.push((peer.preferences.epoch_duration.0, weight));
        spool_sizes.push((peer.preferences.spool_size.0, weight));

        total_weight = total_weight.saturating_add(weight);
    }

    if total_weight == 0 {
        return Err(NodePreferenceAggregationError::ZeroWeight);
    }

    Ok(NodePreferences {
        committee_size: quorum_above(&committee_sizes, total_weight)
            .max(bounds.committee_size),
        // The group count moves only when two thirds voted past the current
        // one: up to the highest count they all reach, down to the lowest.
        spool_groups: current
            .spool_groups
            .max(quorum_above(&spool_group_counts, total_weight))
            .min(quorum_below(&spool_group_counts, total_weight))
            .max(bounds.spool_groups),
        burn_fee_bps: BasisPoints(
            quorum_below(&burn_fee_bps, total_weight)
                .min(BasisPoints::MAX)
                .max(bounds.burn_fee_bps.0),
        ),
        subsidy_decay_bps: BasisPoints(
            quorum_below(&subsidy_decay_bps, total_weight)
                .min(BasisPoints::MAX)
                .max(bounds.subsidy_decay_bps.0),
        ),
        network_capacity: StorageUnits(
            quorum_above(&network_capacities, total_weight)
                .max(bounds.network_capacity.0),
        ),
        storage_price: TAPE(
            quorum_below(&storage_prices, total_weight)
                .max(bounds.storage_price.0),
        ),
        access_threshold: TAPE(
            quorum_above(&access_thresholds, total_weight)
                .max(bounds.access_threshold.0),
        ),
        epoch_duration: EpochDuration(
            quorum_above(&epoch_durations, total_weight)
                .max(bounds.epoch_duration.0),
        ),
        // The largest spool two thirds of the weight are willing to hold.
        spool_size: StorageUnits(quorum_above(&spool_sizes, total_weight)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // spool weight per voted group count, against a current count of 10
    fn spool_groups(votes: &[(u64, u64)]) -> u64 {
        let mut members = Vec::new();
        let mut peers = Vec::new();

        for (index, (count, weight)) in votes.iter().enumerate() {
            let node = Address::from([index as u8 + 1; 32]);

            members.push(Member { spools: *weight, ..Member::new(node, TAPE(1)) });
            peers.push(Peer {
                preferences: NodePreferences { spool_groups: *count, ..NodePreferences::zeroed() },
                ..Peer::new(node)
            });
        }

        let current = NodePreferences { spool_groups: 10, ..NodePreferences::zeroed() };
        let bounds = NodePreferences { spool_groups: 1, ..NodePreferences::zeroed() };

        aggregate_node_preferences(&members, &peers, current, bounds)
            .expect("aggregate")
            .spool_groups
    }

    // just over a third can't move the count either way
    #[test]
    fn minority_keeps_count() {
        assert_eq!(spool_groups(&[(10, 66), (5, 34)]), 10);
        assert_eq!(spool_groups(&[(10, 66), (20, 34)]), 10);
    }

    #[test]
    fn two_thirds_lower() {
        assert_eq!(spool_groups(&[(10, 30), (5, 70)]), 5);
    }

    // the count stops at the lowest value two thirds reach
    #[test]
    fn split_lower() {
        assert_eq!(spool_groups(&[(10, 30), (7, 30), (5, 40)]), 7);
    }

    #[test]
    fn two_thirds_higher() {
        assert_eq!(spool_groups(&[(20, 70), (10, 30)]), 20);
    }

    #[test]
    fn never_zero() {
        assert_eq!(spool_groups(&[(0, 100)]), 1);
    }

    // spool weight per voted spool size, in bytes
    fn spool_size(votes: &[(u64, u64)]) -> StorageUnits {
        let mut members = Vec::new();
        let mut peers = Vec::new();

        for (index, (size, weight)) in votes.iter().enumerate() {
            let node = Address::from([index as u8 + 1; 32]);

            members.push(Member { spools: *weight, ..Member::new(node, TAPE(1)) });
            peers.push(Peer {
                preferences: NodePreferences {
                    spool_size: StorageUnits(*size),
                    ..NodePreferences::zeroed()
                },
                ..Peer::new(node)
            });
        }

        let current = NodePreferences { spool_groups: 1, ..NodePreferences::zeroed() };
        let bounds = NodePreferences { spool_groups: 1, ..NodePreferences::zeroed() };

        aggregate_node_preferences(&members, &peers, current, bounds)
            .expect("aggregate")
            .spool_size
    }

    // the voted size is the largest one two thirds of the weight reach
    #[test]
    fn spool_size_two_thirds() {
        assert_eq!(spool_size(&[(500, 100)]), StorageUnits(500));
        assert_eq!(spool_size(&[(1_000, 67), (500, 33)]), StorageUnits(1_000));
        assert_eq!(spool_size(&[(1_000, 66), (500, 34)]), StorageUnits(500));
        assert_eq!(spool_size(&[(1_000, 40), (700, 30), (500, 30)]), StorageUnits(700));
    }

    // more than a third with no preference leaves the network with none
    #[test]
    fn spool_size_unset() {
        assert_eq!(spool_size(&[(0, 100)]), StorageUnits(0));
        assert_eq!(spool_size(&[(1_000, 66), (0, 34)]), StorageUnits(0));
        assert_eq!(spool_size(&[(1_000, 67), (0, 33)]), StorageUnits(1_000));
    }
}
