//! Distributed uploader for parallel slice uploads.
//!
//! Uploads slices to storage nodes based on spool assignments from the
//! on-chain committee. Each slice goes to the node that owns that spool.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tape_core::erasure::spool_for_slice;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_crypto::Hash;
use tape_protocol::api::{Api, SlicePayload, PutSliceReq};
use tape_retry::RetryConfig;
use tape_protocol::ProtocolState;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::codec::encoder::SliceMerkleProof;
use crate::error::UploadError;

/// Default concurrency limit for uploads.
const DEFAULT_CONCURRENCY: usize = 32;

/// A slice with its merkle proof, ready for upload.
#[derive(Clone)]
pub struct SliceWithProof {
    pub index: SpoolIndex,
    pub data: Vec<u8>,
    pub leaf_hash: Hash,
    pub merkle_proof: SliceMerkleProof,
}

impl SliceWithProof {
    /// Create a new slice with proof.
    pub fn new(index: SpoolIndex, data: Vec<u8>, leaf_hash: Hash, merkle_proof: SliceMerkleProof) -> Self {
        Self { index, data, leaf_hash, merkle_proof }
    }

    /// Convert to SlicePayload for network transmission.
    pub fn to_payload(&self) -> SlicePayload {
        SlicePayload::new(self.data.clone(), self.leaf_hash, self.merkle_proof.to_vec())
    }
}

/// Distributed uploader for parallel slice uploads to storage nodes.
///
/// Uses proper spool-based routing from the on-chain committee. Each slice
/// is sent to the node that owns that slice's spool according to the
/// SpoolAssignment.
pub struct DistributedUploader {
    track: Pubkey,
    spool_group: SpoolGroup,
    slices: Vec<SliceWithProof>,
    /// Spool-to-node map for this group, built from ProtocolState at construction.
    group_peers: Vec<(SpoolIndex, NodeId)>,
    /// Unique member count in this group (for quorum checks).
    group_member_count: usize,
    concurrency_limit: Arc<Semaphore>,
}

impl DistributedUploader {
    /// Create a new uploader with group-aware spool-based routing.
    pub fn new(
        track: Pubkey,
        spool_group: SpoolGroup,
        slices: Vec<SliceWithProof>,
        state: &ProtocolState,
    ) -> Result<Self, UploadError> {
        if slices.len() != tape_core::erasure::SPOOL_GROUP_SIZE {
            return Err(UploadError::InvalidSliceCount {
                expected: tape_core::erasure::SPOOL_GROUP_SIZE,
                got: slices.len(),
            });
        }

        let group_peers = state.group_peers(spool_group);
        let group_member_count = state.group_member_count(spool_group);

        Ok(Self {
            track,
            spool_group,
            slices,
            group_peers,
            group_member_count,
            concurrency_limit: Arc::new(Semaphore::new(DEFAULT_CONCURRENCY)),
        })
    }

    /// Set the concurrency limit.
    pub fn with_concurrency(mut self, limit: usize) -> Self {
        self.concurrency_limit = Arc::new(Semaphore::new(limit));
        self
    }

    /// Upload all slices to the network via the Api trait.
    ///
    /// Sends each slice to the correct spool owner based on the committee's
    /// spool assignment. Returns when all nodes have been attempted.
    /// Failed uploads are left for the recovery worker to handle.
    pub async fn upload_all<P: Api>(&self, peer_client: &P) -> Result<(), UploadError> {
        if self.group_peers.is_empty() {
            return Err(UploadError::NoNodesAvailable);
        }

        // Group spools by NodeId
        let mut node_groups: HashMap<NodeId, Vec<SpoolIndex>> = HashMap::new();
        for &(spool, node_id) in &self.group_peers {
            node_groups.entry(node_id).or_default().push(spool);
        }

        // Build a lookup: global spool index → slice data
        let slice_map: HashMap<SpoolIndex, &SliceWithProof> = self
            .slices
            .iter()
            .map(|s| {
                let global_spool = spool_for_slice(self.spool_group, s.index as usize);
                (global_spool, s)
            })
            .collect();

        // Upload to each node in parallel
        let upload_futures: Vec<_> = node_groups
            .into_iter()
            .map(|(node_id, spools)| {
                let track = self.track;
                let permit = self.concurrency_limit.clone();

                // Collect slices for this node
                let slices: Vec<(SpoolIndex, SliceWithProof)> = spools
                    .iter()
                    .filter_map(|spool| slice_map.get(spool).map(|s| (*spool, (*s).clone())))
                    .collect();

                async move {
                    let _permit = permit
                        .acquire()
                        .await
                        .map_err(|_| UploadError::Semaphore)?;

                    let mut failed_slices = Vec::new();

                    for (global_spool, slice) in slices {
                        let payload = slice.to_payload();
                        let req = PutSliceReq {
                            track: track.into(),
                            spool: global_spool,
                            payload,
                        };

                        if let Err(e) = tape_retry::retry(
                            RetryConfig::ten(),
                            None,
                            || peer_client.put_slice(node_id, &req),
                        ).await {
                            warn!(
                                slice = global_spool,
                                node = %node_id,
                                error = %e,
                                "Slice upload failed after retries, left for recovery"
                            );
                            failed_slices.push(global_spool);
                        }
                    }

                    Ok::<_, UploadError>(failed_slices)
                }
            })
            .collect();

        // Wait for all uploads
        let results: Vec<Result<Vec<SpoolIndex>, UploadError>> = stream::iter(upload_futures)
            .buffer_unordered(DEFAULT_CONCURRENCY)
            .collect()
            .await;

        // Count successful members (those with no errors, may have failed individual slices)
        let mut total_failed_slices = 0;
        let mut member_failures = 0;

        for result in &results {
            match result {
                Ok(failed) => {
                    total_failed_slices += failed.len();
                }
                Err(_) => {
                    member_failures += 1;
                }
            }
        }

        // Check quorum - need 2f+1 of group members to acknowledge
        let successful_members = self.group_member_count - member_failures;
        let required = tape_core::bft::min_correct(self.group_member_count as u64) as usize;

        if successful_members < required {
            return Err(UploadError::InsufficientQuorum {
                got: successful_members,
                need: required,
            });
        }

        // Log if there were any slice-level failures (but we still succeeded overall)
        if total_failed_slices > 0 {
            warn!(
                failed_slices = total_failed_slices,
                "Some slices failed to upload, left for recovery worker"
            );
        }

        Ok(())
    }

    /// Get the number of slices.
    pub fn slice_count(&self) -> usize {
        self.slices.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::CommitteeMember;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_slicer::MERKLE_HEIGHT;

    fn make_test_slices(count: usize) -> Vec<SliceWithProof> {
        (0..count)
            .map(|i| SliceWithProof {
                index: i as u16,
                data: vec![i as u8; 100],
                leaf_hash: Hash::default(),
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            })
            .collect()
    }

    fn make_test_state(member_count: usize) -> ProtocolState {
        let mut state = ProtocolState::default();
        for i in 0..member_count {
            state.committee.push(CommitteeMember::new(
                NodeId(i as u64 + 1),
                Coin::<TAPE>::new(1000 - i as u64),
            ));
        }
        let mut spools = [0u8; SPOOL_COUNT];
        for (i, s) in spools.iter_mut().enumerate() {
            *s = (i % member_count) as u8;
        }
        state.spools = SpoolAssignment::new(spools);
        state
    }

    #[test]
    fn uploader_creation() {
        let slices = make_test_slices(tape_core::erasure::SPOOL_GROUP_SIZE);
        let state = make_test_state(2);

        let uploader = DistributedUploader::new(
            Pubkey::new_unique(),
            SpoolGroup(0),
            slices,
            &state,
        )
        .unwrap();

        assert_eq!(uploader.slice_count(), tape_core::erasure::SPOOL_GROUP_SIZE);
    }

    #[test]
    fn slice_with_proof_to_payload() {
        let slice = SliceWithProof {
            index: 42,
            data: vec![0xAB; 500],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        };

        let payload = slice.to_payload();

        assert_eq!(payload.data, slice.data);
        assert_eq!(payload.leaf_hash, slice.leaf_hash);
        assert_eq!(payload.merkle_proof, slice.merkle_proof);
    }
}
