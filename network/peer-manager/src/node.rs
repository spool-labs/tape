use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_core::bls::BlsPubkey;
use tape_crypto::address::Address;

#[derive(Clone, Debug)]
pub struct PeerNode {
    pub node_id: NodeId,
    pub authority: Address,
    pub state_address: Address,
    pub bls_pubkey: BlsPubkey,
    pub tls_pubkey: Address,
    pub network_address: NetworkAddress,
}
