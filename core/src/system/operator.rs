use crate::types::*;
use tape_crypto::Pubkey;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct NodeMetadata {
    /// The name of this node storage node.
    pub name: [u8; 32],

    /// The storage capacity of the node in bytes.
    pub storage_capacity: u64,

    /// The storage used by the node in bytes.
    pub storage_used: u64,

    /// The SocketAddr of the node
    pub network_address: NetworkAddress,

    /// The public key used for TLS connections to this node.
    pub network_tls: Pubkey,
}
