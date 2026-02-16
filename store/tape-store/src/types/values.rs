//! Value types for tape-store columns

use crate::types::Pubkey;
use serde::{Deserialize, Serialize};
use tape_core::bls::BlsPubkey;
use tape_core::encoding::EncodingProfile;
use tape_core::spooler::SpoolGroup;
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
    /// Spool group this track's slices are distributed across (0..SPOOL_GROUP_COUNT-1)
    pub spool_group: SpoolGroup,
    /// Original unencoded data size in bytes
    pub original_size: u64,
    /// Stripe size in bytes (from encoding)
    pub stripe_size: u64,
    /// Number of stripes
    pub stripe_count: u64,
    /// Encoding type discriminant (EncodingType as u64)
    pub encoding_type: u64,
    /// Encoding params (e.g., ClayParams packed as u64)
    pub encoding_params: u64,
    /// Per-slice commitment leaf hashes (SPOOL_GROUP_SIZE entries)
    pub commitment: Vec<Hash>,
}

impl TrackInfo {
    /// Get the encoding profile (type + params).
    pub fn profile(&self) -> EncodingProfile {
        EncodingProfile {
            encoding: self.encoding_type,
            params: self.encoding_params,
        }
    }

    /// Set the encoding profile (type + params).
    pub fn set_profile(&mut self, profile: EncodingProfile) {
        self.encoding_type = profile.encoding;
        self.encoding_params = profile.params;
    }

    /// Recompute the commitment root from stored leaf hashes.
    pub fn commitment_root(&self) -> Hash {
        tape_crypto::merkle::root_from_leaf_hashes::<
            { tape_core::erasure::COMMITMENT_TREE_HEIGHT },
        >(&self.commitment)
    }

    /// Verify a single slice against its stored leaf hash.
    pub fn verify_slice(&self, position: usize, data: &[u8]) -> bool {
        if position >= self.commitment.len() {
            return false;
        }
        tape_crypto::merkle::hash_leaf(data) == self.commitment[position]
    }
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

/// Proof data needed to submit an on-chain track invalidation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct InvalidationProof {
    pub bitmap: u128,
    pub signature: [u8; 32],
    pub computed_root: [u8; 32],
}

/// Snapshot chunk encoding metadata (stored during build, consumed during registration).
///
/// This is intentionally separate from `TrackInfo`: snapshots are built before
/// on-chain registration creates any track state, and we only store local slices
/// (not all group slices). Persisting this metadata lets `RegisterSnapshot` resume
/// after crashes without re-running full snapshot encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotChunkMeta {
    /// Per-slice leaf hashes (SPOOL_GROUP_SIZE entries)
    pub leaves: Vec<Hash>,
    /// Stripe size used during encoding
    pub stripe_size: u64,
    /// Number of stripes
    pub stripe_count: u64,
    /// Encoding type discriminant
    pub encoding_type: u64,
    /// Encoding params
    pub encoding_params: u64,
}

/// Collected BLS certification for a snapshot chunk
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotCertResult {
    /// Committee member indices that signed
    pub member_indices: Vec<u8>,
    /// Aggregated BLS signature bytes
    pub signature: [u8; 32],
    /// Epoch of the certification
    pub epoch: u64,
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
            spool_group: 3,
            original_size: 1024 * 1024,
            stripe_size: 10 * 1024 * 1024,
            stripe_count: 1,
            encoding_type: 2, // Clay
            encoding_params: 0x100714, // n=20, k=7, d=16 packed
            commitment: vec![Hash::default(); 20],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TrackInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_info_basic_encoding() {
        let info = TrackInfo {
            tape_address: Pubkey([2u8; 32]),
            spool_group: 0,
            original_size: 512,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 1, // Basic
            encoding_params: 0,
            commitment: vec![],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TrackInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_info_profile_helpers() {
        use tape_core::encoding::{EncodingProfile, EncodingType, ClayParams};

        let mut info = TrackInfo {
            tape_address: Pubkey([3u8; 32]),
            spool_group: 1,
            original_size: 1024,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        };

        // Set profile
        let profile = EncodingProfile::clay(ClayParams::new(20, 7, 16));
        info.set_profile(profile);

        // Get profile and verify
        let retrieved = info.profile();
        assert_eq!(retrieved.encoding_type(), Some(EncodingType::Clay));
        assert_eq!(retrieved.clay_params().n(), 20);
        assert_eq!(retrieved.clay_params().k(), 7);
        assert_eq!(retrieved.clay_params().d(), 16);
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
