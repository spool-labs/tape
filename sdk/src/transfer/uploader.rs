//! Distributed uploader for parallel slice uploads.
//!
//! Uploads slices to storage nodes based on spool assignments from the
//! on-chain committee. Each slice goes to the node that owns that spool.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tape_core::bft::{max_faulty, min_correct};
use tape_core::erasure::{GROUP_SIZE, spool_for_slice};
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use bytes::Bytes;
use tape_protocol::api::{Api, ApiError, CertifyRes, PutSliceReq, SlicePayload};
use tape_protocol::ProtocolState;
use tape_retry::{Backoff, RetryConfig, Retryable};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::bootstrap::Reputation;
use crate::codec::encoder::SliceMerkleProof;
use crate::error::UploadError;

/// Longest a rate limited upload sleeps, whatever the server advertises.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(60);

/// The wire body puts a u64 length ahead of the slice bytes
const BODY_DATA_OFFSET: usize = 8;

/// A slice with its merkle proof, held as the wire body every push attempt shares
#[derive(Clone)]
pub struct SliceWithProof {
    pub index: SpoolIndex,
    pub leaf_hash: Hash,
    pub merkle_proof: SliceMerkleProof,
    pub body: Bytes,
    data_len: usize,
}

impl SliceWithProof {
    /// Create a new slice with proof.
    pub fn new(
        index: SpoolIndex,
        data: Vec<u8>,
        leaf_hash: Hash,
        merkle_proof: SliceMerkleProof,
    ) -> Result<Self, UploadError> {
        let data_len = data.len();
        let payload = SlicePayload::new(data, leaf_hash, merkle_proof.to_vec());
        let body = wincode::serialize(&payload)
            .map_err(|error| UploadError::Encoding(format!("slice body: {error}")))?;
        Ok(Self {
            index,
            leaf_hash,
            merkle_proof,
            body: Bytes::from(body),
            data_len,
        })
    }

    /// The slice bytes inside the wire body.
    pub fn data(&self) -> &[u8] {
        &self.body[BODY_DATA_OFFSET..BODY_DATA_OFFSET + self.data_len]
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
    epoch: EpochNumber,
    concurrency_limit: Arc<Semaphore>,
    reputation: Option<Arc<Reputation>>,
}

/// Earlier upload attempts of one track landed these spools and receipts
#[derive(Default)]
pub struct UploadProgress {
    epoch: Option<EpochNumber>,
    stored: HashSet<SpoolIndex>,
    receipts: HashMap<Address, CertifyRes>,
}

struct NodeUploadResult {
    stored: Vec<SpoolIndex>,
    failed: Vec<SpoolIndex>,
    not_responsible: Vec<SpoolIndex>,
    receipt: Option<CertifyRes>,
}

impl DistributedUploader {
    /// Create a new uploader with group-aware spool-based routing.
    pub fn new(
        track: Address,
        group: GroupIndex,
        slices: Vec<SliceWithProof>,
        state: &ProtocolState,
        concurrency: usize,
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
            epoch: state.current.epoch.id,
            concurrency_limit: Arc::new(Semaphore::new(concurrency.max(1))),
            reputation: None,
        })
    }

    /// Spend less time on owners already known to be down.
    pub fn with_reputation(mut self, reputation: Arc<Reputation>) -> Self {
        self.reputation = Some(reputation);
        self
    }

    /// Upload all slices to the network via the Api trait.
    ///
    /// Sends each slice to the correct spool owner based on the committee's
    /// spool assignment. Returns as soon as a certification quorum of members
    /// and slices has landed; the remaining uploads keep running as detached
    /// tasks and any that fail are left for the recovery worker to handle.
    /// Spools in `progress` from this epoch are not sent again.
    pub async fn upload_all<P: Api>(
        &self,
        peer_client: Arc<P>,
        progress: &mut UploadProgress,
    ) -> Result<Vec<CertifyRes>, UploadError> {
        if self.group_peers.is_empty() {
            return Err(UploadError::NoNodesAvailable);
        }

        // Group spools by node account address.
        let mut node_groups: HashMap<Address, Vec<SpoolIndex>> = HashMap::new();
        for &(spool, node) in &self.group_peers {
            node_groups.entry(node).or_default().push(spool);
        }

        // A new epoch moves spools to new owners, so nothing landed before counts.
        if progress.epoch != Some(self.epoch) {
            *progress = UploadProgress {
                epoch: Some(self.epoch),
                ..UploadProgress::default()
            };
        }
        let mut fully_successful_members = 0;
        let mut stored_slices: HashSet<SpoolIndex> = HashSet::new();
        let mut missing_groups: HashMap<Address, Vec<SpoolIndex>> = HashMap::new();
        for (node, spools) in node_groups {
            let mut missing = Vec::new();
            for spool in spools {
                match progress.stored.contains(&spool) {
                    true => {
                        stored_slices.insert(spool);
                    }
                    false => missing.push(spool),
                }
            }
            match missing.is_empty() {
                true => fully_successful_members += 1,
                false => {
                    missing_groups.insert(node, missing);
                }
            }
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
        // Straggler retries watch this: the send at quorum wakes any backoff
        // sleep immediately, and the sender dropping on return means the
        // outcome is decided either way, so no retry outlives its purpose.
        let (quorum_tx, quorum_rx) = watch::channel(false);

        // Upload to each node in a detached task so a quorum can complete the
        // call while stragglers keep going. Before quorum, slice uploads use
        // the full retry budget; after quorum, remaining uploads get one
        // attempt.
        let (result_sender, mut result_receiver) = mpsc::unbounded_channel();
        for (node, spools) in missing_groups {
            let track = self.track;
            let concurrency_limit = self.concurrency_limit.clone();
            let quorum_rx = quorum_rx.clone();
            let peer_client = peer_client.clone();
            let result_sender = result_sender.clone();

            // Collect slices for this node
            let slices: Vec<(SpoolIndex, SliceWithProof)> = spools
                .iter()
                .filter_map(|spool| slice_map.get(spool).map(|s| (*spool, (*s).clone())))
                .collect();

            // An owner already known to be down gets the same budget a
            // straggler gets after quorum: one attempt, then recovery takes
            // it. Upload routing is fixed, so the only saving available is not
            // spending the full ladder on a node that will not answer.
            let is_known_down = self
                .reputation
                .as_ref()
                .map(|reputation| reputation.is_quarantined(&node))
                .unwrap_or(false);
            // A channel that starts decided. The retry loop reads it before
            // ever sleeping, so it returns after one attempt and never waits
            // on a sender that nobody holds.
            let budget = match is_known_down {
                true => watch::channel(true).1,
                false => quorum_rx,
            };
            let reputation = self.reputation.clone();

            // Detached: stragglers finish after quorum returns, and anything they
            // fail to land is picked up by the recovery worker.
            tokio::spawn(async move {
                let started = Instant::now();
                let result = upload_node_slices(
                    peer_client.as_ref(),
                    node,
                    track,
                    slices,
                    budget,
                    concurrency_limit,
                )
                .await;
                if let Some(reputation) = reputation {
                    match result.as_ref().map(|outcome| outcome.failed.is_empty()) {
                        Ok(true) => reputation.record_success(node, started.elapsed()),
                        Ok(false) | Err(_) => reputation.record_failure(node),
                    }
                }
                let _ = result_sender.send(result);
            });
        }
        drop(result_sender);

        // Count members that stored all assigned slices and total landed slices.
        let mut total_failed_slices = 0;
        let mut not_responsible_count = 0usize;
        let mut member_failures = 0;

        loop {
            if fully_successful_members >= required_members
                && stored_slices.len() >= required_slices
            {
                let _ = quorum_tx.send(true);
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
                // One receipt per owner: every slice that node stored signs the same track hash.
                return Ok(progress.receipts.values().cloned().collect());
            }

            let Some(result) = result_receiver.recv().await else {
                break;
            };
            match result {
                Ok(node) => {
                    total_failed_slices += node.failed.len();
                    not_responsible_count += node.not_responsible.len();
                    progress.stored.extend(node.stored.iter().copied());
                    stored_slices.extend(node.stored);
                    if let Some(receipt) = node.receipt {
                        progress.receipts.insert(receipt.node, receipt);
                    }
                    if node.failed.is_empty() && node.not_responsible.is_empty() {
                        fully_successful_members += 1;
                    }
                }
                Err(error) => {
                    warn!(error = %error, "member upload task failed");
                    member_failures += 1;
                }
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
    quorum: watch::Receiver<bool>,
    concurrency_limit: Arc<Semaphore>,
) -> Result<NodeUploadResult, UploadError> {
    let _permit = concurrency_limit
        .acquire()
        .await
        .map_err(|_| UploadError::Semaphore)?;

    let uploads = slices.into_iter().map(|(global_spool, slice)| {
        let quorum = quorum.clone();
        async move {
            let payload_bytes = slice.data().len();
            let req = PutSliceReq {
                track,
                spool: global_spool,
                body: slice.body.clone(),
            };

            let result = upload_slice_with_retry(
                peer_client,
                node,
                track,
                req,
                payload_bytes,
                quorum,
            )
            .await;
            (global_spool, result)
        }
    });

    let mut stored = Vec::new();
    let mut failed = Vec::new();
    let mut not_responsible = Vec::new();
    let mut receipt = None;

    for (global_spool, result) in join_all(uploads).await {
        match result {
            Ok(signed) => {
                stored.push(global_spool);
                receipt.get_or_insert(signed);
            }
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

    Ok(NodeUploadResult { stored, failed, not_responsible, receipt })
}

/// Whether a slice push should retry
///
/// Not-found is retryable here because the node rejects slices until it has
/// ingested our confirmed register. It stays terminal everywhere else.
fn should_retry_put_slice(error: &ApiError) -> bool {
    matches!(error, ApiError::NotFound) || error.is_retryable()
}

/// Retry cadence for a slice a node has not accepted yet.
///
/// The eager push lands milliseconds after the register confirms, before any
/// owner has ingested the block, so the first attempt reliably returns
/// `not found` and the retry decides the phase. A one second base oversleeps
/// it: measured, five slices drew 522-887 ms of backoff to wait out a node
/// ingest tip poll of 400 ms, and quorum completed on the fourth-shortest draw
/// at 856 ms, which was the entire `store` phase.
///
/// Ramping from 60 ms catches the ingest as soon as it happens and still
/// settles to a polite cadence if a node is genuinely down rather than behind.
fn slice_retry_config() -> RetryConfig {
    RetryConfig {
        base_delay: Duration::from_millis(60),
        max_delay: Duration::from_secs(2),
        max_retries: Some(10),
    }
}

async fn upload_slice_with_retry<P: Api>(
    peer_client: &P,
    node: Address,
    track: Address,
    req: PutSliceReq,
    payload_bytes: usize,
    mut quorum: watch::Receiver<bool>,
) -> Result<CertifyRes, ApiError> {
    let started = Instant::now();
    let mut backoff = Backoff::new(slice_retry_config());

    loop {
        let attempt_started = Instant::now();
        match peer_client.put_slice(node, &req).await {
            Ok(response) => {
                // The successful push was never timed, only the failures, so a
                // slow store phase could not be told apart from a waiting one.
                debug!(
                    track = %track,
                    node = %node,
                    slice = %req.spool,
                    bytes = payload_bytes,
                    push_ms = attempt_started.elapsed().as_millis() as u64,
                    total_ms = started.elapsed().as_millis() as u64,
                    attempts = backoff.attempt() + 1,
                    "slice accepted"
                );
                return Ok(response.receipt);
            }
            Err(error) => {
                if !should_retry_put_slice(&error) {
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

                if *quorum.borrow() {
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

                let Some(mut delay) = backoff.next_delay() else {
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

                // A server-advertised retry window beats the backoff guess, so
                // no request lands inside a known-closed window.
                if let ApiError::RateLimited { retry_after: Some(wait) } = &error {
                    delay = delay.max((*wait).min(MAX_RATE_LIMIT_WAIT));
                }

                // The quorum watch wakes the sleep the moment the outcome is
                // decided; a closed sender means the upload call has already
                // returned, which decides it just the same.
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = quorum.changed() => {
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
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use bytemuck::Zeroable;
    use peer_memory::MemoryApi;
    use tape_core::bls::BlsSignature;
    use tape_protocol::api::{PeerReq, PeerRes, PutSliceRes};
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
                .expect("slice")
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

    fn receipt(node: Address) -> CertifyRes {
        CertifyRes {
            signature: BlsSignature::zeroed(),
            node,
            epoch: EpochNumber(1),
        }
    }

    // a retry after a failed quorum pushes only the spools that never landed
    #[tokio::test]
    async fn missing_spools() {
        let state = make_test_state(4);
        let failing = [state.current.committee[0].node, state.current.committee[1].node];
        let is_healed = Arc::new(AtomicBool::new(false));
        let pushes = Arc::new(AtomicUsize::new(0));
        let api = Arc::new(MemoryApi::new({
            let is_healed = is_healed.clone();
            let pushes = pushes.clone();
            move |node, request| match request {
                PeerReq::PutSlice(_) => {
                    pushes.fetch_add(1, Ordering::SeqCst);
                    if failing.contains(&node) && !is_healed.load(Ordering::SeqCst) {
                        return PeerRes::PutSlice(Err(ApiError::Other("down".into())));
                    }
                    PeerRes::PutSlice(Ok(PutSliceRes { receipt: receipt(node) }))
                }
                _ => PeerRes::GetHealth(Err(ApiError::Other("unexpected".into()))),
            }
        }));
        let uploader = DistributedUploader::new(
            Address::new_unique(),
            GroupIndex(0),
            make_test_slices(GROUP_SIZE),
            &state,
            GROUP_SIZE,
        )
        .expect("uploader");
        let mut progress = UploadProgress::default();

        let first = uploader.upload_all(api.clone(), &mut progress).await;
        assert!(matches!(first, Err(UploadError::InsufficientQuorum { .. })));
        assert_eq!(pushes.load(Ordering::SeqCst), GROUP_SIZE);

        is_healed.store(true, Ordering::SeqCst);
        let receipts = uploader
            .upload_all(api, &mut progress)
            .await
            .expect("second attempt");

        // Stragglers past the quorum finish in the background.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(pushes.load(Ordering::SeqCst), GROUP_SIZE + GROUP_SIZE / 2);
        assert!(receipts.len() >= min_correct(4) as usize);
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
            GROUP_SIZE,
        )
        .unwrap();

        assert_eq!(uploader.slice_count(), GROUP_SIZE);
    }

    // the wire body decodes back to the original slice
    #[test]
    fn wire_body() {
        let data: Vec<u8> = (0..500).map(|byte| (byte % 251) as u8).collect();
        let proof = [Hash([7u8; 32]); SLICE_TREE_HEIGHT];
        let slice = SliceWithProof::new(SpoolIndex::from(42), data.clone(), Hash([9u8; 32]), proof)
            .expect("slice");

        let payload: SlicePayload = wincode::deserialize(&slice.body).expect("decode");

        assert_eq!(payload.data, data);
        assert_eq!(slice.data(), data.as_slice());
        assert_eq!(payload.leaf_hash, slice.leaf_hash);
        assert_eq!(payload.merkle_proof, proof.to_vec());
    }
}
