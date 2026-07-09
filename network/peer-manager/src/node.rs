use tape_core::bls::BlsPubkey;
use tape_core::system::{NodePreferences, Peer};
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::address::Address;

#[derive(Clone, Debug)]
pub struct PeerNode {
    pub node: Address,
    pub bls_pubkey: BlsPubkey,
    pub tls_pubkey: NetworkTlsPubkey,
    pub network_address: NetworkAddress,
    pub preferences: NodePreferences,
    pub stake: Coin<TAPE>,
}

impl PeerNode {
    pub fn from_peer(peer: Peer) -> Option<Self> {
        (peer.node != Address::default()).then_some(Self {
            node: peer.node,
            bls_pubkey: peer.bls_pubkey,
            tls_pubkey: peer.network_tls,
            network_address: peer.network_address,
            preferences: peer.preferences,
            stake: TAPE(0),
        })
    }

    pub fn with_stake(mut self, stake: Coin<TAPE>) -> Self {
        self.stake = stake;
        self
    }
}
