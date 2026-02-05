//! Value types for tape-store columns

use crate::types::{Pubkey, SpoolAllocation};
use serde::{Deserialize, Serialize};
use tape_core::bls::BlsPubkey;
use tape_core::types::EpochNumber;
use tape_core::types::network::NetworkAddress;
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Metadata about a tape (storage allocation)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeInfo {
    /// Epoch when the tape expires
    pub end_epoch: EpochNumber,
}

/// Metadata about a track (individual blob)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackInfo {
    /// Address of the tape this track belongs to
    pub tape_address: Pubkey,
    /// How slices are allocated across spools
    pub spool_allocation: SpoolAllocation,
    /// Original unencoded data size in bytes
    pub original_size: u64,
    /// Size of each stripe after encoding
    pub stripe_size: u64,
    /// Number of stripes
    pub stripe_count: u32,
    /// Encoding type as u64 for Pod compat
    pub encoding_type: u64,
    /// Commitment hashes (empty = no commitments, non-empty = all hashes)
    pub commitment: Vec<Hash>,
}

/// Serde helper for NetworkAddress (Pod type without native serde support)
mod network_address_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tape_core::types::network::NetworkAddress;

    pub fn serialize<S: Serializer>(addr: &NetworkAddress, s: S) -> Result<S::Ok, S::Error> {
        addr.as_bytes().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<NetworkAddress, D::Error> {
        let bytes = <[u8; 24]>::deserialize(d)?;
        Ok(NetworkAddress::from_bytes(bytes))
    }
}

/// Information about a single committee member
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct NodeInfo {
    /// Node's on-chain account pubkey
    pub node_address: Pubkey,
    /// BLS public key for signatures
    pub bls_pubkey: BlsPubkey,
    /// TLS public key for secure communication
    pub tls_pubkey: Pubkey,
    /// Network address for P2P communication
    #[serde(with = "network_address_serde")]
    pub network_address: NetworkAddress,
    /// Spools assigned to this node
    pub spools: Vec<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;

    #[test]
    fn test_tape_info_roundtrip() {
        let info = TapeInfo {
            end_epoch: EpochNumber(200),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TapeInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_info_roundtrip() {
        let info = TrackInfo {
            tape_address: Pubkey([1u8; 32]),
            spool_allocation: SpoolAllocation::SpoolGroup(3),
            original_size: 1024 * 1024,
            stripe_size: 1024,
            stripe_count: 1024,
            encoding_type: 3,
            commitment: vec![Hash::default(); 10],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TrackInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_info_empty_commitment() {
        let info = TrackInfo {
            tape_address: Pubkey([2u8; 32]),
            spool_allocation: SpoolAllocation::SpoolSingle(42),
            original_size: 512,
            stripe_size: 256,
            stripe_count: 2,
            encoding_type: 1,
            commitment: vec![],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TrackInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_node_info_roundtrip() {
        let info = NodeInfo {
            node_address: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: Pubkey::new([2u8; 32]),
            network_address: NetworkAddress::new_ipv4([192, 168, 1, 1], 8080),
            spools: vec![0, 10, 20, 30],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: NodeInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }
}
