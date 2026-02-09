//! Distributed uploader for parallel slice uploads.
//!
//! Uploads slices to storage nodes based on spool assignments from the
//! on-chain committee. Each slice goes to the node that owns that spool.

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tape_core::erasure::spool_for_slice;
use tape_core::system::Committee;
use tape_core::spooler::SpoolGroup;
use tape_crypto::Hash;
use tape_node_api::SlicePayload;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::communication::NodeCommunicationFactory;
use crate::encoder::SliceMerkleProof;
use crate::error::UploadError;
use crate::routing::SliceRouter;

// Re-export erasure coding constants from tape-core
pub use tape_core::erasure::SPOOL_COUNT;
// Re-export spool types for convenience
pub use tape_core::spooler::{SpoolAssignment, SpoolIndex, SpoolMapping};

/// Default concurrency limit for uploads.
const DEFAULT_CONCURRENCY: usize = 32;

/// Default number of retries per node.
const DEFAULT_RETRY_COUNT: usize = 3;

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
        SlicePayload::new(self.data.clone(), self.leaf_hash, self.merkle_proof)
    }
}

/// Distributed uploader for parallel slice uploads to storage nodes.
///
/// Uses proper spool-based routing from the on-chain committee. Each slice
/// is sent to the node that owns that slice's spool according to the
/// SpoolAssignment.
pub struct DistributedUploader<const MEMBERS: usize> {
    track_id: String,
    spool_group: SpoolGroup,
    slices: Vec<SliceWithProof>,
    router: SliceRouter<MEMBERS>,
    factory: NodeCommunicationFactory,
    concurrency_limit: Arc<Semaphore>,
    retry_count: usize,
}

impl<const MEMBERS: usize> DistributedUploader<MEMBERS> {
    /// Create a new uploader with group-aware spool-based routing.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `spool_group` - The spool group for this track
    /// * `slices` - The encoded slices with merkle proofs (should be SPOOL_GROUP_SIZE)
    /// * `router` - SliceRouter with committee and spool assignments
    /// * `factory` - Factory for creating node clients
    pub fn new(
        track_id: String,
        spool_group: SpoolGroup,
        slices: Vec<SliceWithProof>,
        router: SliceRouter<MEMBERS>,
        factory: NodeCommunicationFactory,
    ) -> Self {
        Self {
            track_id,
            spool_group,
            slices,
            router,
            factory,
            concurrency_limit: Arc::new(Semaphore::new(DEFAULT_CONCURRENCY)),
            retry_count: DEFAULT_RETRY_COUNT,
        }
    }

    /// Set the concurrency limit.
    pub fn with_concurrency(mut self, limit: usize) -> Self {
        self.concurrency_limit = Arc::new(Semaphore::new(limit));
        self
    }

    /// Set the retry count for failed uploads.
    pub fn with_retry_count(mut self, count: usize) -> Self {
        self.retry_count = count;
        self
    }

    /// Upload all slices to the network.
    ///
    /// Sends each slice to the correct spool owner based on the committee's
    /// spool assignment. Returns when all nodes have been attempted.
    /// Failed uploads are left for the recovery worker to handle.
    pub async fn upload_all(&self) -> Result<(), UploadError> {
        if self.router.committee_size() == 0 {
            return Err(UploadError::NoNodesAvailable);
        }

        // Group slices by the member that owns them within the spool group
        let member_groups = self.router.group_slices_by_member_for_group(self.spool_group);

        // Build a lookup: global spool index → slice data
        // Each slice's index is its local position (0..SPOOL_GROUP_SIZE-1),
        // map to global spool index for routing
        let slice_map: std::collections::HashMap<SpoolIndex, &SliceWithProof> = self
            .slices
            .iter()
            .map(|s| {
                let global_spool = spool_for_slice(self.spool_group, s.index as usize);
                (global_spool, s)
            })
            .collect();

        // Upload to each member in parallel
        let upload_futures: Vec<_> = member_groups
            .into_iter()
            .map(|(member_idx, slice_spool_pairs)| {
                let factory = self.factory.clone();
                let track_id = self.track_id.clone();
                let permit = self.concurrency_limit.clone();
                let retry_count = self.retry_count;

                // Get the address for this member using the first spool index
                let first_spool = slice_spool_pairs.first().map(|(_, s)| *s).unwrap_or(0);
                let addr_result = self.router.socket_addr_for_slice(first_spool);

                // Collect slices for this member
                let slices: Vec<SliceWithProof> = slice_spool_pairs
                    .iter()
                    .filter_map(|(_, spool)| slice_map.get(spool).map(|s| (*s).clone()))
                    .collect();

                async move {
                    let addr = addr_result.map_err(|e| {
                        UploadError::Network(format!("address resolution: {}", e))
                    })?;

                    let _permit = permit
                        .acquire()
                        .await
                        .map_err(|_| UploadError::Semaphore)?;

                    let address = format!("http://{}", addr);
                    let client = factory.client_for_address(&address)?;

                    let mut failed_slices = Vec::new();

                    for slice in slices {
                        let mut last_error = None;

                        for attempt in 0..retry_count {
                            let payload = slice.to_payload();
                            match client.put_slice(&track_id, slice.index, &payload).await {
                                Ok(_) => {
                                    last_error = None;
                                    break;
                                }
                                Err(e) => {
                                    if attempt < retry_count - 1 {
                                        debug!(
                                            slice = slice.index,
                                            attempt = attempt + 1,
                                            "Retrying slice upload"
                                        );
                                    }
                                    last_error = Some(e);
                                }
                            }
                        }

                        if let Some(e) = last_error {
                            warn!(
                                slice = slice.index,
                                member = member_idx,
                                error = %e,
                                "Slice upload failed after retries, left for recovery"
                            );
                            failed_slices.push(slice.index);
                        }
                    }

                    Ok::<_, UploadError>((member_idx, failed_slices))
                }
            })
            .collect();

        // Wait for all uploads
        let results: Vec<Result<(SpoolMapping, Vec<SpoolIndex>), UploadError>> = stream::iter(upload_futures)
            .buffer_unordered(DEFAULT_CONCURRENCY)
            .collect()
            .await;

        // Count successful members (those with no errors, may have failed individual slices)
        let mut total_failed_slices = 0;
        let mut member_failures = 0;

        for result in &results {
            match result {
                Ok((_, failed)) => {
                    total_failed_slices += failed.len();
                }
                Err(_) => {
                    member_failures += 1;
                }
            }
        }

        // Check quorum - need 2f+1 of group members to acknowledge
        let group_members = self.router.unique_members_in_group(self.spool_group).len();
        let successful_members = group_members - member_failures;
        let required = tape_core::bft::min_correct(group_members as u64) as usize;

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

/// Builder for constructing a SliceRouter from system state.
pub fn build_router<const MEMBERS: usize>(
    committee: Committee<MEMBERS>,
    spool_assignment: SpoolAssignment<SPOOL_COUNT>,
) -> SliceRouter<MEMBERS> {
    SliceRouter::new(spool_assignment, committee)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::system::CommitteeMember;
    use tape_core::types::{Coin, NodeId, TAPE};
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

    fn make_test_router<const N: usize>(member_count: usize) -> SliceRouter<N> {
        let mut committee = Committee::new();
        for i in 0..member_count.min(N) {
            let member = CommitteeMember::new(
                NodeId::new(i as u64 + 1),
                Coin::<TAPE>::new(1000 - i as u64),
            );
            let _ = committee.try_join(&member);
        }

        // Create uniform spool assignment
        let mut spools = [0u8; SPOOL_COUNT];
        for i in 0..SPOOL_COUNT {
            spools[i] = (i % member_count) as u8;
        }
        let assignment = SpoolAssignment::new(spools);

        SliceRouter::new(assignment, committee)
    }

    #[test]
    fn test_uploader_creation() {
        let factory = NodeCommunicationFactory::new();
        let slices = make_test_slices(10);
        let router: SliceRouter<10> = make_test_router(2);

        let uploader = DistributedUploader::new(
            "track_123".to_string(),
            0, // spool group 0
            slices,
            router,
            factory,
        );

        assert_eq!(uploader.slice_count(), 10);
    }

    #[test]
    fn test_slice_with_proof_to_payload() {
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

    #[test]
    fn test_build_router() {
        let mut committee: Committee<10> = Committee::new();
        let member = CommitteeMember::new(NodeId::new(1), Coin::<TAPE>::new(1000));
        let _ = committee.try_join(&member);

        let spools = [0u8; SPOOL_COUNT];
        let assignment = SpoolAssignment::new(spools);

        let router = build_router(committee, assignment);
        assert_eq!(router.committee_size(), 1);
    }
}
