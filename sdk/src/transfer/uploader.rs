//! Distributed uploader for parallel slice uploads.
//!
//! Uploads slices to storage nodes based on spool assignments from the
//! on-chain committee. Each slice goes to the node that owns that spool.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::future::join_all;
use tape_core::bft::{max_faulty, min_correct};
use tape_core::erasure::{GROUP_SIZE, spool_for_slice};
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_protocol::api::{Api, ApiError, SlicePayload, PutSliceReq};
use tape_protocol::ProtocolState;
use tape_retry::{Backoff, RetryConfig, Retryable};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::codec::encoder::SliceMerkleProof;
use crate::error::UploadError;

/// Default concurrency limit for uploads: one in-flight slice per group member.
const DEFAULT_CONCURRENCY: usize = GROUP_SIZE;

/// A slice with its merkle proof, ready for upload.
///
/// Slice bytes are shared so cloning a slice into per-node upload tasks and
/// retry attempts never copies the payload.
#[derive(Clone)]
pub struct SliceWithProof {
    pub index: SpoolIndex,
    pub data: Arc<Vec<u8>>,
    pub leaf_hash: Hash,
    pub merkle_proof: SliceMerkleProof,
}

impl SliceWithProof {
    /// Create a new slice with proof.
    pub fn new(index: SpoolIndex, data: Vec<u8>, leaf_hash: Hash, merkle_proof: SliceMerkleProof) -> Self {
        Self { index, data: Arc::new(data), leaf_hash, merkle_proof }
    }

    /// Convert to SlicePayload for network transmission.
    pub fn to_payload(&self) -> SlicePayload {
        SlicePayload::new(self.data.as_ref().clone(), self.leaf_hash, self.merkle_proof.to_vec())
    }
}

/// Distributed uploader for parallel slice uploads to storage nodes.
///
/// Uses proper spool-based routing from the on-chain committee. Each slice
/// is sent to the node that owns that slice's spool according to the
/// SpoolAssignment.
pub struct DistributedUploader {
    track: Address,
    group: GroupIndex,
    slices: Vec<SliceWithProof>,
    group_peers: Vec<(SpoolIndex, Address)>,
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
        group: GroupIndex,
        slices: Vec<SliceWithProof>,
        state: &ProtocolState,
    ) -> Result<Self, UploadError> {
        if slices.len() != GROUP_SIZE {
            return Err(UploadError::InvalidSliceCount {
                expected: GROUP_SIZE,
                got: slices.len(),
            });
        }

        let group_peers = state.group_peers(group);
        let group_member_count = state.group_member_count(group);

        Ok(Self {
            track,
            group,
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
    /// spool assignment. Returns as soon as a certification quorum of members
    /// and slices has landed; the remaining uploads keep running as detached
    /// tasks and any that fail are left for the recovery worker to handle.
    pub async fn upload_all<P: Api>(&self, peer_client: Arc<P>) -> Result<(), UploadError> {
        if self.group_peers.is_empty() {
            return Err(UploadError::NoNodesAvailable);
        }

        // Group spools by node account address.
        let mut node_groups: HashMap<Address, Vec<SpoolIndex>> = HashMap::new();
        for &(spool, node) in &self.group_peers {
            node_groups.entry(node).or_default().push(spool);
        }

        // Build a lookup: global spool index → slice data
        let slice_map: HashMap<SpoolIndex, &SliceWithProof> = self
            .slices
            .iter()
            .map(|s| {
                let global_spool = spool_for_slice(self.group, s.index.as_usize());
                (global_spool, s)
            })
            .collect();

        let required_members = min_correct(self.group_member_count as u64) as usize;
        let required_slices = min_correct(GROUP_SIZE as u64) as usize;
        let quorum_reached = Arc::new(AtomicBool::new(false));

        // Upload to each node in a detached task so a quorum can complete the
        // call while stragglers keep going. Before quorum, slice uploads use
        // the full retry budget; after quorum, remaining uploads get one
        // attempt.
        let (result_sender, mut result_receiver) = mpsc::unbounded_channel();
        for (node, spools) in node_groups {
            let track = self.track;
            let concurrency_limit = self.concurrency_limit.clone();
            let quorum_reached = quorum_reached.clone();
            let peer_client = peer_client.clone();
            let result_sender = result_sender.clone();

            // Collect slices for this node
            let slices: Vec<(SpoolIndex, SliceWithProof)> = spools
                .iter()
                .filter_map(|spool| slice_map.get(spool).map(|s| (*spool, (*s).clone())))
                .collect();

            // Deliberately unjoined: once quorum returns this call, straggler
            // tasks finish on their own and anything they fail to land is
            // repaired by the recovery worker. The send fails harmlessly after
            // the receiver is dropped at quorum.
            tokio::spawn(async move {
                let result = upload_node_slices(
                    peer_client.as_ref(),
                    node,
                    track,
                    slices,
                    quorum_reached.as_ref(),
                    concurrency_limit,
                )
                .await;
                let _ = result_sender.send(result);
            });
        }
        drop(result_sender);

        // Count members that stored all assigned slices and total landed slices.
        let mut total_failed_slices = 0;
        let mut not_responsible_count = 0usize;
        let mut member_failures = 0;
        let mut fully_successful_members = 0;
        let mut stored_slices: HashSet<SpoolIndex> = HashSet::new();

        while let Some(result) = result_receiver.recv().await {
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

            if fully_successful_members >= required_members
                && stored_slices.len() >= required_slices
            {
                quorum_reached.store(true, Ordering::Relaxed);
                if total_failed_slices > 0 {
                    warn!(
                        failed_slices = total_failed_slices,
                        "Some slices failed to upload, left for recovery worker"
                    );
                }
                info!(
                    track = %self.track,
                    members = fully_successful_members,
                    required_members,
                    slices = stored_slices.len(),
                    required_slices,
                    "slice upload quorum reached, draining remaining uploads in the background"
                );
                return Ok(());
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

        // Quorum was never reached - report whichever bound fell short.
        let successful_members = self.group_member_count - member_failures;

        if successful_members < required_members || fully_successful_members < required_members {
            return Err(UploadError::InsufficientQuorum {
                got: fully_successful_members.min(successful_members),
                need: required_members,
            });
        }

        Err(UploadError::InsufficientSlices {
            got: stored_slices.len(),
            need: required_slices,
        })
    }

    /// Get the number of slices.
    pub fn slice_count(&self) -> usize {
        self.slices.len()
    }
}

/// Upload one node's slices concurrently under a single member permit.
async fn upload_node_slices<P: Api>(
    peer_client: &P,
    node: Address,
    track: Address,
    slices: Vec<(SpoolIndex, SliceWithProof)>,
    quorum_reached: &AtomicBool,
    concurrency_limit: Arc<Semaphore>,
) -> Result<NodeUploadResult, UploadError> {
    let _permit = concurrency_limit
        .acquire()
        .await
        .map_err(|_| UploadError::Semaphore)?;

    let uploads = slices.into_iter().map(|(global_spool, slice)| async move {
        let payload = slice.to_payload();
        let payload_bytes = payload.data.len();
        let req = PutSliceReq {
            track: track.into(),
            spool: global_spool,
            payload,
        };

        let result = upload_slice_with_retry(
            peer_client,
            node,
            track,
            req,
            payload_bytes,
            quorum_reached,
        )
        .await;
        (global_spool, result)
    });

    let mut stored = Vec::new();
    let mut failed = Vec::new();
    let mut not_responsible = Vec::new();

    for (global_spool, result) in join_all(uploads).await {
        match result {
            Ok(()) => stored.push(global_spool),
            Err(e) => {
                warn!(
                    track = %track,
                    slice = %global_spool,
                    node = %node,
                    error = %e,
                    "Slice upload failed, left for recovery"
                );
                if matches!(e, ApiError::NotResponsible) {
                    not_responsible.push(global_spool);
                } else {
                    failed.push(global_spool);
                }
            }
        }
    }

    Ok(NodeUploadResult { stored, failed, not_responsible })
}

async fn upload_slice_with_retry<P: Api>(
    peer_client: &P,
    node: Address,
    track: Address,
    req: PutSliceReq,
    payload_bytes: usize,
    quorum_reached: &AtomicBool,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let mut backoff = Backoff::new(RetryConfig::ten());

    loop {
        match peer_client.put_slice(node, &req).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                if !error.is_retryable() {
                    warn!(
                        track = %track,
                        node = %node,
                        slice = %req.spool,
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
                        node = %node,
                        slice = %req.spool,
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
                        node = %node,
                        slice = %req.spool,
                        bytes = payload_bytes,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        error = %error,
                        "slice upload exhausted retries"
                    );
                    return Err(error);
                };

                warn!(
                    track = %track,
                    node = %node,
                    slice = %req.spool,
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
                        node = %node,
                        slice = %req.spool,
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
    use bytemuck::Zeroable;
    use tape_api::state::Group;
    use tape_core::bls::BlsPubkey;
    use tape_core::system::{Member, Spool};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_slicer::SLICE_TREE_HEIGHT;
    use tape_crypto::address::Address;

    fn make_test_slices(count: usize) -> Vec<SliceWithProof> {
        (0..count)
            .map(|i| {
                SliceWithProof::new(
                    SpoolIndex::from(i as u64),
                    vec![i as u8; 100],
                    Hash::default(),
                    [Hash::default(); SLICE_TREE_HEIGHT],
                )
            })
            .collect()
    }

    fn make_test_state(member_count: usize) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.current.epoch.id = EpochNumber(1);
        for i in 0..member_count {
            let mut bytes = [0u8; 32];
            bytes[0] = i as u8 + 1;
            state.current.committee.push(Member::new(
                Address::new(bytes),
                TAPE(1000 - i as u64),
            ));
        }

        let mut group = Group {
            id: GroupIndex(0),
            epoch: EpochNumber(1),
            size: StorageUnits::mb(1),
            ..Group::zeroed()
        };
        for i in 0..GROUP_SIZE {
            let owner = state.current.committee[i % member_count].node;
            group.spools[i] = Spool::new(owner, BlsPubkey::zeroed());
        }
        state.current.groups.push(group);
        state
    }

    #[test]
    fn uploader_creation() {
        let slices = make_test_slices(GROUP_SIZE);
        let state = make_test_state(2);

        let uploader = DistributedUploader::new(
            Address::new_unique(),
            GroupIndex(0),
            slices,
            &state,
        )
        .unwrap();

        assert_eq!(uploader.slice_count(), GROUP_SIZE);
    }

    #[test]
    fn slice_with_proof_to_payload() {
        let slice = SliceWithProof::new(
            SpoolIndex::from(42),
            vec![0xAB; 500],
            Hash::default(),
            [Hash::default(); SLICE_TREE_HEIGHT],
        );

        let payload = slice.to_payload();

        assert_eq!(payload.data, *slice.data);
        assert_eq!(payload.leaf_hash, slice.leaf_hash);
        assert_eq!(payload.merkle_proof, slice.merkle_proof);
    }
}
