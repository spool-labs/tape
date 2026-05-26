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

    /// The SocketAddr of the node.
    pub network_address: NetworkAddress,

    /// The TLS public key of this node.
    pub network_tls: NetworkTlsPubkey,

    /// The BLS public key of this node.
    pub bls_pubkey: BlsPubkey,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Zeroable, Pod, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct NodePreferences {
    /// The preferred minimum protocol version.
    pub min_version: VersionId,

    /// The preferred capacity of new committees.
    pub committee_size: u64,

    /// The preferred number of spool groups per epoch.
    pub spool_groups: u64,

    /// The preferred storage payment burn rate in basis points.
    pub burn_fee_bps: BasisPoints,

    /// The preferred subsidy decay rate in basis points per epoch.
    pub subsidy_decay_bps: BasisPoints,

    /// The preferred total archive size.
    pub storage_capacity: StorageUnits,

    /// The preferred price per storage unit.
    pub storage_price: Coin<TAPE>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodePreferenceAggregationError {
    MissingPeer { node: Address },
    ZeroWeight,
}

pub fn aggregate_node_preferences(
    members: &[Member],
    peers: &[Peer],
    bounds: NodePreferences,
) -> Result<NodePreferences, NodePreferenceAggregationError> {
    let mut total_weight: u64 = 0;
    let mut min_versions: Vec<(u64, u64)> = Vec::new();
    let mut committee_sizes: Vec<(u64, u64)> = Vec::new();
    let mut spool_group_counts: Vec<(u64, u64)> = Vec::new();
    let mut burn_fee_bps: Vec<(u64, u64)> = Vec::new();
    let mut subsidy_decay_bps: Vec<(u64, u64)> = Vec::new();
    let mut storage_capacities: Vec<(u64, u64)> = Vec::new();
    let mut storage_prices: Vec<(u64, u64)> = Vec::new();

    for member in members.iter() {
        let peer = peers
            .iter()
            .find(|p| p.node == member.node)
            .ok_or(NodePreferenceAggregationError::MissingPeer { node: member.node })?;
        let weight = member.spools;

        min_versions.push((peer.preferences.min_version.0, weight));
        committee_sizes.push((peer.preferences.committee_size, weight));
        spool_group_counts.push((peer.preferences.spool_groups, weight));
        burn_fee_bps.push((peer.preferences.burn_fee_bps.0, weight));
        subsidy_decay_bps.push((peer.preferences.subsidy_decay_bps.0, weight));
        storage_capacities.push((peer.preferences.storage_capacity.0, weight));
        storage_prices.push((peer.preferences.storage_price.0, weight));

        total_weight = total_weight.saturating_add(weight);
    }

    if total_weight == 0 {
        return Err(NodePreferenceAggregationError::ZeroWeight);
    }

    Ok(NodePreferences {
        min_version: VersionId(
            quorum_above(&min_versions, total_weight)
                .max(bounds.min_version.0),
        ),
        committee_size: quorum_above(&committee_sizes, total_weight)
            .max(bounds.committee_size),
        spool_groups: quorum_above(&spool_group_counts, total_weight)
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
        storage_capacity: StorageUnits(
            quorum_above(&storage_capacities, total_weight)
                .max(bounds.storage_capacity.0),
        ),
        storage_price: TAPE(
            quorum_below(&storage_prices, total_weight)
                .max(bounds.storage_price.0),
        ),
    })
}
