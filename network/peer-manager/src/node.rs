//! PeerNode — known, trusted peer node metadata.

use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_core::bls::BlsPubkey;
use tape_crypto::Pubkey;

#[derive(Clone, Debug)]
pub struct PeerNode {
    pub node_id: NodeId,
    pub authority: Pubkey,
    pub state_address: Pubkey,
    pub bls_pubkey: BlsPubkey,
    pub tls_pubkey: Pubkey,
    pub network_address: NetworkAddress,
}
