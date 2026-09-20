//! Distributed uploader for parallel slice uploads.
//!
//! Uploads slices to storage nodes based on spool assignments from the
//! on-chain committee. Each slice goes to the node that owns that spool.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tape_core::bft::{max_faulty, min_correct};
use tape_core::erasure::{GROUP_SIZE, spool_for_slice};
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_protocol::api::{Api, ApiError, CertifyRes, PutSliceReq, SlicePayload};
use tape_protocol::ProtocolState;
use tape_retry::{Backoff, RetryConfig, Retryable};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use crate::bootstrap::{counts_against_peer, Reputation};
use crate::codec::encoder::SliceMerkleProof;
use crate::error::UploadError;

/// Longest a rate limited upload sleeps, whatever the server advertises.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(60);

/// How long a slice keeps pushing while its owner answers that it is not ready yet.
const STRAGGLER_LIMIT: Duration = Duration::from_secs(60);

/// The beat a straggler pushes at while its owner ingests the register.
const STRAGGLER_STEP: Duration = Duration::from_millis(250);

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
    reputation: Option<Arc<Reputation>>,
}

struct NodeUploadResult {
    stored: Vec<SpoolIndex>,
    failed: Vec<(SpoolIndex, ApiError)>,
    not_responsible: Vec<SpoolIndex>,
    receipt: Option<CertifyRes>,
}

impl NodeUploadResult {
    fn health(&self) -> NodeHealth {
        if self.failed.is_empty() && self.not_responsible.is_empty() {
            return NodeHealth::Stored;
        }
        for (_, error) in &self.failed {
            if counts_against_peer(error) {
                return NodeHealth::Unreachable;
            }
        }
        NodeHealth::Unknown
    }
}

/// What one owner's answers say about it.
enum NodeHealth {
    Stored,
    Unreachable,
    Unknown,
}

/// Attempts one slice gets on its owner.
#[derive(Clone, Copy)]
enum AttemptBudget {
    Once,
    Full,
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
    pub async fn upload_all<P: Api>(&self, peer_client: Arc<P>) -> Result<Vec<CertifyRes>, UploadError> {
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

        // Each owner uploads in a detached task so a quorum can complete the call while stragglers keep pushing.
        let (result_sender, mut result_receiver) = mpsc::unbounded_channel();
        let node_count = node_groups.len();
        for (node, spools) in node_groups {
            let track = self.track;
            let concurrency_limit = self.concurrency_limit.clone();
            let peer_client = peer_client.clone();
            let result_sender = result_sender.clone();

            // Collect slices for this node
            let slices: Vec<(SpoolIndex, SliceWithProof)> = spools
                .iter()
                .filter_map(|spool| slice_map.get(spool).map(|s| (*spool, (*s).clone())))
                .collect();

            // An owner already known to be down gets one attempt, then recovery takes its slices.
            let is_known_down = self
                .reputation
                .as_ref()
                .map(|reputation| reputation.is_quarantined(&node))
                .unwrap_or(false);
            let budget = match is_known_down {
                true => AttemptBudget::Once,
                false => AttemptBudget::Full,
            };
            let reputation = self.reputation.clone();

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
                    if let Ok(outcome) = result.as_ref() {
                        record_node_health(&reputation, node, started.elapsed(), outcome);
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
        let mut fully_successful_members = 0;
        let mut stored_slices: HashSet<SpoolIndex> = HashSet::new();
        // One receipt per owner: every slice that node stored signs the same
        // track hash, so the rest are duplicates.
        let mut receipts: Vec<CertifyRes> = Vec::with_capacity(node_count);

        while let Some(result) = result_receiver.recv().await {
            match result {
                Ok(node) => {
                    total_failed_slices += node.failed.len();
                    not_responsible_count += node.not_responsible.len();
                    stored_slices.extend(node.stored);
                    receipts.extend(node.receipt);
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
                return Ok(receipts);
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

/// Record what one owner's answers say about it.
fn record_node_health(
    reputation: &Reputation,
    node: Address,
    elapsed: Duration,
    outcome: &NodeUploadResult,
) {
    match outcome.health() {
        NodeHealth::Stored => reputation.record_success(node, elapsed),
        NodeHealth::Unreachable => reputation.record_failure(node),
        NodeHealth::Unknown => {}
    }
}

/// Upload one node's slices concurrently under a single member permit.
async fn upload_node_slices<P: Api>(
    peer_client: &P,
    node: Address,
    track: Address,
    slices: Vec<(SpoolIndex, SliceWithProof)>,
    budget: AttemptBudget,
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
            track,
            spool: global_spool,
            payload,
        };

        let result =
            upload_slice_with_retry(peer_client, node, track, req, payload_bytes, budget).await;
        (global_spool, result)
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
            Err(error) => {
                warn!(
                    track = %track,
                    slice = %global_spool,
                    node = %node,
                    error = %error,
                    "slice upload failed, left for recovery"
                );
                match matches!(error, ApiError::NotResponsible) {
                    true => not_responsible.push(global_spool),
                    false => failed.push((global_spool, error)),
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

/// How long until the next push, or nothing when this slice is done trying.
fn next_push_delay(
    backoff: &mut Backoff,
    error: &ApiError,
    elapsed: Duration,
) -> Option<Duration> {
    match counts_against_peer(error) {
        true => backoff.next_delay(),
        false => match elapsed < STRAGGLER_LIMIT {
            true => Some(STRAGGLER_STEP),
            false => None,
        },
    }
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
    budget: AttemptBudget,
) -> Result<CertifyRes, ApiError> {
    let started = Instant::now();
    let mut backoff = Backoff::new(slice_retry_config());
    let mut pushes: u32 = 0;

    loop {
        let attempt_started = Instant::now();
        pushes = pushes.saturating_add(1);
        match peer_client.put_slice(node, &req).await {
            Ok(response) => {
                debug!(
                    track = %track,
                    node = %node,
                    slice = %req.spool,
                    bytes = payload_bytes,
                    push_ms = attempt_started.elapsed().as_millis() as u64,
                    total_ms = started.elapsed().as_millis() as u64,
                    attempts = pushes,
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

                match budget {
                    AttemptBudget::Once => {
                        warn!(
                            track = %track,
                            node = %node,
                            slice = %req.spool,
                            bytes = payload_bytes,
                            error = %error,
                            "slice upload failed on its one attempt, leaving for recovery"
                        );
                        return Err(error);
                    }
                    AttemptBudget::Full => {}
                }

                let mut delay = match next_push_delay(&mut backoff, &error, started.elapsed()) {
                    Some(delay) => delay,
                    None => {
                        warn!(
                            track = %track,
                            node = %node,
                            slice = %req.spool,
                            bytes = payload_bytes,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            error = %error,
                            "slice upload out of attempts, leaving for recovery"
                        );
                        return Err(error);
                    }
                };

                // A server-advertised retry window beats the backoff guess.
                if let ApiError::RateLimited { retry_after: Some(wait) } = &error {
                    delay = delay.max((*wait).min(MAX_RATE_LIMIT_WAIT));
                }

                debug!(
                    track = %track,
                    node = %node,
                    slice = %req.spool,
                    bytes = payload_bytes,
                    attempts = pushes,
                    delay_ms = delay.as_millis() as u64,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    error = %error,
                    "slice upload failed, pushing again"
                );

                sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use bytemuck::Zeroable;
    use peer_memory::MemoryApi;
    use tape_api::state::Group;
    use tape_core::bls::{BlsPubkey, BlsSignature};
    use tape_core::system::{Member, Spool};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_protocol::api::{PeerReq, PeerRes, PutSliceRes};
    use tape_slicer::SLICE_TREE_HEIGHT;
    use tape_crypto::address::Address;

    use crate::bootstrap::{NetworkKey, PeerRecord, QUARANTINE_THRESHOLD};

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
            GROUP_SIZE,
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

    fn ephemeral_reputation() -> Arc<Reputation> {
        Arc::new(Reputation::ephemeral(NetworkKey {
            program_id: Address::new_unique(),
            genesis: Hash([9u8; 32]),
        }))
    }

    fn accepted(node: Address) -> PeerRes {
        PeerRes::PutSlice(Ok(PutSliceRes {
            receipt: CertifyRes {
                signature: BlsSignature::zeroed(),
                node,
                epoch: EpochNumber(1),
            },
        }))
    }

    fn refused(error: ApiError) -> PeerRes {
        PeerRes::PutSlice(Err(error))
    }

    /// One owner answers what the script says for its attempt number, every other owner accepts at once.
    fn api_with_straggler(
        straggler: Address,
        script: impl Fn(usize) -> PeerRes + Send + Sync + 'static,
    ) -> (Arc<MemoryApi>, Arc<AtomicUsize>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let api = MemoryApi::new(move |node, req| {
            let PeerReq::PutSlice(_) = req else {
                return refused(ApiError::Other("unexpected call".into()));
            };
            match node == straggler {
                true => script(counter.fetch_add(1, Ordering::Relaxed) + 1),
                false => accepted(node),
            }
        });
        (Arc::new(api), attempts)
    }

    fn peer_record(reputation: &Reputation, node: Address) -> Option<PeerRecord> {
        reputation
            .snapshot()
            .and_then(|state| state.peer(&node).copied())
    }

    fn uploader_for(state: &ProtocolState, reputation: Arc<Reputation>) -> DistributedUploader {
        DistributedUploader::new(
            Address::new_unique(),
            GroupIndex(0),
            make_test_slices(GROUP_SIZE),
            state,
            GROUP_SIZE,
        )
        .expect("uploader built")
        .with_reputation(reputation)
    }

    // an owner that answers not found and then accepts stores its slice at no reputation cost
    #[tokio::test(start_paused = true)]
    async fn straggler_accepts_late() {
        let state = make_test_state(GROUP_SIZE);
        let straggler = state.current.committee[0].node;
        let (api, attempts) = api_with_straggler(straggler, move |attempt| match attempt < 5 {
            true => refused(ApiError::NotFound),
            false => accepted(straggler),
        });
        let reputation = ephemeral_reputation();

        let receipts = uploader_for(&state, reputation.clone())
            .upload_all(api)
            .await
            .expect("quorum reached");
        sleep(STRAGGLER_LIMIT * 2).await;

        assert!(receipts.len() >= min_correct(GROUP_SIZE as u64) as usize);
        assert_eq!(attempts.load(Ordering::Relaxed), 5);
        let record = peer_record(&reputation, straggler).expect("straggler recorded");
        assert_eq!(record.consecutive_failures, 0);
    }

    // an owner that never ingests the register is left for recovery at no reputation cost
    #[tokio::test(start_paused = true)]
    async fn straggler_gives_up() {
        let state = make_test_state(GROUP_SIZE);
        let straggler = state.current.committee[0].node;
        let (api, attempts) = api_with_straggler(straggler, |_| refused(ApiError::NotFound));
        let reputation = ephemeral_reputation();

        uploader_for(&state, reputation.clone())
            .upload_all(api)
            .await
            .expect("quorum reached");
        sleep(STRAGGLER_LIMIT * 2).await;
        let pushed = attempts.load(Ordering::Relaxed);
        sleep(STRAGGLER_LIMIT).await;

        let window = (STRAGGLER_LIMIT.as_millis() / STRAGGLER_STEP.as_millis()) as usize;
        assert!(pushed >= window && pushed <= window + 1);
        assert_eq!(attempts.load(Ordering::Relaxed), pushed);
        assert!(peer_record(&reputation, straggler).is_none());
    }

    // an owner that keeps refusing the connection spends its ladder and counts as a failure
    #[tokio::test(start_paused = true)]
    async fn transport_failure_counts() {
        let state = make_test_state(GROUP_SIZE);
        let straggler = state.current.committee[0].node;
        let (api, attempts) =
            api_with_straggler(straggler, |_| refused(ApiError::ConnectionFailed("refused".into())));
        let reputation = ephemeral_reputation();

        uploader_for(&state, reputation.clone())
            .upload_all(api)
            .await
            .expect("quorum reached");
        sleep(STRAGGLER_LIMIT * 2).await;

        assert!(attempts.load(Ordering::Relaxed) > 1);
        let record = peer_record(&reputation, straggler).expect("straggler recorded");
        assert_eq!(record.consecutive_failures, 1);
    }

    // a quarantined owner costs exactly one push
    #[tokio::test(start_paused = true)]
    async fn quarantined_pushed_once() {
        let state = make_test_state(GROUP_SIZE);
        let down = state.current.committee[0].node;
        let (api, attempts) = api_with_straggler(down, |_| refused(ApiError::NotFound));
        let reputation = ephemeral_reputation();
        for _ in 0..QUARANTINE_THRESHOLD {
            reputation.record_failure(down);
        }

        uploader_for(&state, reputation.clone())
            .upload_all(api)
            .await
            .expect("quorum reached");
        sleep(STRAGGLER_LIMIT * 2).await;

        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        let record = peer_record(&reputation, down).expect("owner recorded");
        assert_eq!(record.consecutive_failures, QUARANTINE_THRESHOLD);
    }
}
