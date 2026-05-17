use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::bls::BlsPubkey;
use crate::types::*;

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
    /// The preferred total archive size.
    pub storage_capacity: StorageUnits,

    /// The preferred price per storage unit.
    pub storage_price: Coin<TAPE>,

    /// The preferred capacity of new committees.
    pub committee_size: u64,

    /// The preferred number of spool groups per epoch.
    pub spool_groups: u64,

    /// The preferred minimum protocol version.
    pub min_version: VersionId,
}
