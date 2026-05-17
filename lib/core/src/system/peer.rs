use bytemuck::{Pod, Zeroable};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_crypto::address::Address;

use crate::bls::BlsPubkey;
use crate::types::network::NetworkAddress;
use crate::types::tls::NetworkTlsPubkey;

use super::node::NodePreferences;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Peer {
    pub node: Address,
    pub bls_pubkey: BlsPubkey,
    pub network_address: NetworkAddress,
    pub network_tls: NetworkTlsPubkey,
    pub preferences: NodePreferences,
}

impl Peer {
    pub fn new(node: Address) -> Self {
        Peer {
            node,
            ..Peer::zeroed()
        }
    }
}
