//! Distributed uploader for parallel slice uploads.
//!
//! Uploads slices to storage nodes based on spool assignments from the
//! on-chain committee. Each slice goes to the node that owns that spool.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::stream::{self, StreamExt};
use tape_core::bft::{max_faulty, min_correct};
use tape_core::erasure::{GROUP_SIZE, spool_for_slice};
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_protocol::api::{Api, ApiError, SlicePayload, PutSliceReq};
use tape_protocol::ProtocolState;
use tape_retry::{Backoff, RetryConfig, Retryable};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::codec::encoder::SliceMerkleProof;
use crate::error::UploadError;

/// Default concurrency limit for uploads.
const DEFAULT_CONCURRENCY: usize = 8;

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
    track: Address,
    spool_group: SpoolGroup,
    slices: Vec<SliceWithProof>,
    /// Spool-to-node map for this group, built from ProtocolState at construction.
    group_peers: Vec<(SpoolIndex, NodeId)>,
    /// Unique member count in this group (for quorum checks).
    group_member_count: usize,
    concurrency_limit: Arc<Semaphore>,
}

struct NodeUploadResult {
    stored: Vec<SpoolIndex>,
    failed: Vec<SpoolIndex>,
    not_responsible: Vec<SpoolIndex>,
}

impl DistributedUploader {
    /// Create a new uploader with group-aware spool-based routing.
    pub fn new(
        track: Address,
        spool_group: SpoolGroup,
        slices: Vec<SliceWithProof>,
        state: &ProtocolState,
    ) -> Result<Self, UploadError> {
        if slices.len() != GROUP_SIZE {
            return Err(UploadError::InvalidSliceCount {
                expected: GROUP_SIZE,
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

        let required_members = min_correct(self.group_member_count as u64) as usize;
        let required_slices = min_correct(GROUP_SIZE as u64) as usize;
        let quorum_reached = Arc::new(AtomicBool::new(false));

        // Upload to each node in parallel. Before quorum, slice uploads use the
        // full retry budget; after quorum, remaining uploads get one attempt.
        let upload_futures: Vec<_> = node_groups
            .into_iter()
            .map(|(node_id, spools)| {
                let track = self.track;
                let permit = self.concurrency_limit.clone();
                let quorum_reached = quorum_reached.clone();

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

                    let mut stored = Vec::new();
                    let mut failed = Vec::new();
                    let mut not_responsible = Vec::new();

                    for (global_spool, slice) in slices {
                        let payload = slice.to_payload();
                        let payload_bytes = payload.data.len();
                        let req = PutSliceReq {
                            track: track.into(),
                            spool: global_spool,
                            payload,
                        };

                        if let Err(e) = upload_slice_with_retry(
                            peer_client,
                            node_id,
                            track,
                            req,
                            payload_bytes,
                            quorum_reached.as_ref(),
                        ).await {
                            warn!(
                                track = %track,
                                slice = global_spool,
                                node = %node_id,
                                error = %e,
                                "Slice upload failed, left for recovery"
                            );
                            if matches!(e, ApiError::NotResponsible) {
                                not_responsible.push(global_spool);
                            } else {
                                failed.push(global_spool);
                            }
                        } else {
                            stored.push(global_spool);
                        }
                    }

                    Ok::<_, UploadError>(NodeUploadResult { stored, failed, not_responsible })
                }
            })
            .collect();

        // Count members that stored all assigned slices and total landed slices.
        let mut total_failed_slices = 0;
        let mut not_responsible_count = 0usize;
        let mut member_failures = 0;
        let mut fully_successful_members = 0;
        let mut stored_slices: HashSet<SpoolIndex> = HashSet::new();

        let mut results = stream::iter(upload_futures).buffer_unordered(DEFAULT_CONCURRENCY);
        while let Some(result) = results.next().await {
            match result {
                Ok(node) => {
                    total_failed_slices += node.failed.len();
                    not_responsible_count += node.not_responsible.len();
                    stored_slices.extend(node.stored);
                    if node.failed.is_empty() && node.not_responsible.is_empty() {
                        fully_successful_members += 1;
                    }
                }
                Err(error) => {
                    warn!(error = %error, "member upload task failed");
                    member_failures += 1;
                }
            }

            if !quorum_reached.load(Ordering::Relaxed)
                && fully_successful_members >= required_members
                && stored_slices.len() >= required_slices
            {
                quorum_reached.store(true, Ordering::Relaxed);
                info!(
                    track = %self.track,
                    members = fully_successful_members,
                    required_members,
                    slices = stored_slices.len(),
                    required_slices,
                    "slice upload quorum reached, remaining uploads will not retry"
                );
            }
        }

        // If more than f slices were rejected as NotResponsible, the epoch
        // has changed. A Byzantine minority (at most f nodes) cannot fake this.
        let f = max_faulty(GROUP_SIZE as u64) as usize;
        if not_responsible_count > f {
            return Err(UploadError::EpochChanged {
                not_responsible: not_responsible_count,
            });
        }

        // Check quorum - need 2f+1 fully successful group members and 2f+1 landed slices.
        let successful_members = self.group_member_count - member_failures;

        if successful_members < required_members || fully_successful_members < required_members {
            return Err(UploadError::InsufficientQuorum {
                got: fully_successful_members.min(successful_members),
                need: required_members,
            });
        }

        if stored_slices.len() < required_slices {
            return Err(UploadError::InsufficientSlices {
                got: stored_slices.len(),
                need: required_slices,
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

async fn upload_slice_with_retry<P: Api>(
    peer_client: &P,
    node_id: NodeId,
    track: Address,
    req: PutSliceReq,
    payload_bytes: usize,
    quorum_reached: &AtomicBool,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let mut backoff = Backoff::new(RetryConfig::ten());

    loop {
        match peer_client.put_slice(node_id, &req).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                if !error.is_retryable() {
                    warn!(
                        track = %track,
                        node = %node_id,
                        slice = req.spool,
                        bytes = payload_bytes,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        error = %error,
                        "slice upload failed with non-retryable error"
                    );
                    return Err(error);
                }

                if quorum_reached.load(Ordering::Relaxed) {
                    warn!(
                        track = %track,
                        node = %node_id,
                        slice = req.spool,
                        bytes = payload_bytes,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        error = %error,
                        "slice upload failed after quorum, leaving for recovery"
                    );
                    return Err(error);
                }

                let Some(delay) = backoff.next_delay() else {
                    warn!(
                        track = %track,
                        node = %node_id,
                        slice = req.spool,
                        bytes = payload_bytes,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        error = %error,
                        "slice upload exhausted retries"
                    );
                    return Err(error);
                };

                warn!(
                    track = %track,
                    node = %node_id,
                    slice = req.spool,
                    bytes = payload_bytes,
                    attempt = backoff.attempt(),
                    delay_ms = delay.as_millis() as u64,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    error = %error,
                    "slice upload failed, retrying after backoff"
                );

                sleep(delay).await;

                if quorum_reached.load(Ordering::Relaxed) {
                    warn!(
                        track = %track,
                        node = %node_id,
                        slice = req.spool,
                        bytes = payload_bytes,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        error = %error,
                        "slice upload retry skipped after quorum"
                    );
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::CommitteeMember;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_slicer::SLICE_TREE_HEIGHT;
    use tape_crypto::address::Address;

    fn make_test_slices(count: usize) -> Vec<SliceWithProof> {
        (0..count)
            .map(|i| SliceWithProof {
                index: i as u16,
                data: vec![i as u8; 100],
                leaf_hash: Hash::default(),
                merkle_proof: [Hash::default(); SLICE_TREE_HEIGHT],
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
        let slices = make_test_slices(GROUP_SIZE);
        let state = make_test_state(2);

        let uploader = DistributedUploader::new(
            Address::new_unique(),
            SpoolGroup(0),
            slices,
            &state,
        )
        .unwrap();

        assert_eq!(uploader.slice_count(), GROUP_SIZE);
    }

    #[test]
    fn slice_with_proof_to_payload() {
        let slice = SliceWithProof {
            index: 42,
            data: vec![0xAB; 500],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); SLICE_TREE_HEIGHT],
        };

        let payload = slice.to_payload();

        assert_eq!(payload.data, slice.data);
        assert_eq!(payload.leaf_hash, slice.leaf_hash);
        assert_eq!(payload.merkle_proof, slice.merkle_proof);
    }
}
