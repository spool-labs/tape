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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSetError {
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSetUpsert {
    Inserted,
    Updated,
}

pub fn apply_peer_join(peers: &mut Vec<Peer>, peer: Peer) -> PeerSetUpsert {
    let (index, result) = match peers.iter().position(|p| p.node == peer.node) {
        Some(index) => {
            peers[index] = peer;
            (index, PeerSetUpsert::Updated)
        }
        None => {
            peers.push(peer);
            (peers.len() - 1, PeerSetUpsert::Inserted)
        }
    };

    let count = peers.len();
    bubble_up_peer(peers, count, index);
    result
}

pub fn apply_peer_join_slice(
    peers: &mut [Peer],
    count: &mut u64,
    capacity: u64,
    peer: Peer,
) -> Result<PeerSetUpsert, PeerSetError> {
    let count_usize = *count as usize;
    let capacity_usize = capacity as usize;
    if count_usize > peers.len() || capacity_usize > peers.len() {
        return Err(PeerSetError::Full);
    }

    let (index, result) = match peers[..count_usize]
        .iter()
        .position(|p| p.node == peer.node)
    {
        Some(index) => {
            peers[index] = peer;
            (index, PeerSetUpsert::Updated)
        }
        None => {
            if count_usize >= capacity_usize {
                return Err(PeerSetError::Full);
            }
            peers[count_usize] = peer;
            *count = (*count).saturating_add(1);
            (count_usize, PeerSetUpsert::Inserted)
        }
    };

    bubble_up_peer(peers, *count as usize, index);
    Ok(result)
}

pub fn bubble_up_peer<T>(peers: &mut [T], count: usize, index: usize) -> bool {
    let bottom_threshold = (count * 2) / 3;
    if index >= bottom_threshold && index != 0 {
        peers.swap(0, index);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> Peer {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Peer::new(Address::new(bytes))
    }

    #[test]
    fn peer_join_bubbles_bottom_third_to_front() {
        let mut peers = vec![peer(1), peer(2), peer(3)];

        apply_peer_join(&mut peers, peer(4));

        assert_eq!(peers[0].node, peer(4).node);
    }
}
