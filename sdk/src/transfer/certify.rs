//! Track certification module.
//!
//! Collects BLS signatures from committee members and builds certification
//! transactions for tracks.
//!
//! # Certification Flow
//!
//! 1. **Collect Signatures**: Query committee members in parallel for BLS signatures
//! 2. **Verify Supermajority**: Ensure we have at least 2f+1 signatures
//! 3. **Aggregate Signatures**: Combine individual BLS signatures into one
//! 4. **Build Transaction**: Create the CertifyTrack instruction with bitmap + aggregated signature

use std::collections::{HashMap, HashSet};

use futures::stream::{self, StreamExt};
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use tape_api::instruction::build_certify_track_ix;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_api::state::System;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::erasure::{spool_for_slice, SPOOL_GROUP_SIZE};
use tape_core::spooler::SpoolGroup;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::{EpochNumber, NodeId};
use tape_retry::{retry, RetryConfig};
use tape_protocol::api::{Api, ApiError, CertifyReq, CertifyRes};

/// Errors that can occur during certification.
#[derive(Debug, Error)]
pub enum CertificationError {
    /// Not enough signatures collected for supermajority.
    #[error("insufficient signatures: got {got} of {total}, need supermajority")]
    InsufficientSignatures { got: usize, total: usize },

    /// No committee members available.
    #[error("no committee members available")]
    NoCommitteeMembers,

    /// Failed to aggregate signatures.
    #[error("failed to aggregate signatures: {0}")]
    AggregationFailed(String),

    /// Failed to connect to node.
    #[error("failed to connect to node {node_id}: {message}")]
    ConnectionFailed { node_id: NodeId, message: String },

    /// RPC error.
    #[error("RPC error: {0}")]
    Rpc(String),

    /// System state error.
    #[error("system state error: {0}")]
    SystemState(String),

    /// Collection was cancelled.
    #[error("signature collection cancelled")]
    Cancelled,

    /// Nodes returned signatures for different epochs.
    #[error("epoch mismatch: expected {expected}, got {got} from node {node_id}")]
    EpochMismatch {
        expected: u64,
        got: u64,
        node_id: NodeId,
    },
}

/// Reason why a specific node failed to provide a signature.
#[derive(Debug, Clone)]
pub enum NodeSignError {
    /// Node is not in the current committee.
    NotInCommittee,
    /// Track not found on this node.
    NotFound,
    /// Connection or network error.
    Network(String),
    /// Request timed out after all retries.
    Timeout,
    /// Not responsible for this spool.
    NotResponsible,
    /// Other error.
    Other(String),
}

impl From<&ApiError> for NodeSignError {
    fn from(err: &ApiError) -> Self {
        match err {
            ApiError::NotFound => NodeSignError::NotFound,
            ApiError::NotInCommittee => NodeSignError::NotInCommittee,
            ApiError::Timeout => NodeSignError::Timeout,
            ApiError::NotResponsible => NodeSignError::NotResponsible,
            ApiError::ConnectionFailed(msg) => NodeSignError::Network(msg.clone()),
            ApiError::NodeUnresolved(id) => NodeSignError::Network(format!("node {:?} unresolved", id)),
            _ => NodeSignError::Other(err.to_string()),
        }
    }
}

impl From<ApiError> for NodeSignError {
    fn from(err: ApiError) -> Self {
        NodeSignError::from(&err)
    }
}

/// Configuration for certification collection.
#[derive(Clone, Debug)]
pub struct CertificationConfig {
    /// Maximum concurrent signature requests.
    pub max_concurrent: usize,
    /// Maximum retries per node.
    pub max_retries: usize,
    /// Whether to exit early once supermajority is reached.
    pub early_exit: bool,
}

impl Default for CertificationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 32,
            max_retries: 3,
            early_exit: true,
        }
    }
}

impl CertificationConfig {
    /// Create config optimized for fast networks.
    pub fn fast() -> Self {
        Self {
            max_concurrent: 64,
            max_retries: 3,
            early_exit: true,
        }
    }

    /// Create config optimized for unreliable networks.
    pub fn resilient() -> Self {
        Self {
            max_concurrent: 16,
            max_retries: 10,
            early_exit: true,
        }
    }
}

/// Result of a successful signature collection.
#[derive(Debug)]
pub struct CollectedSignatures {
    /// The aggregated BLS signature.
    pub aggregated_signature: BlsSignature,
    /// Bitmap indicating which committee members signed.
    pub bitmap: CommitteeBitmap,
    /// Number of signatures collected.
    pub signature_count: usize,
    /// Total committee size.
    pub committee_size: usize,
    /// The epoch that was signed (all responses must agree).
    pub epoch: u64,
    /// Individual responses (for debugging/verification).
    pub responses: Vec<CertifyRes>,
    /// Nodes that failed and why (for diagnostics).
    pub failures: Vec<(NodeId, NodeSignError)>,
    /// Whether collection exited early (supermajority reached before all responses).
    pub early_exit: bool,
}

/// Collector for gathering BLS signatures from committee members.
pub struct CertificationCollector {
    config: CertificationConfig,
}

impl CertificationCollector {
    /// Create a new certification collector with the given configuration.
    pub fn new(config: CertificationConfig) -> Self {
        Self { config }
    }

    /// Create a new certification collector with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CertificationConfig::default())
    }

    /// Collect signatures from committee members for a track via the Api trait.
    pub async fn collect_signatures<P: Api>(
        &self,
        peer_client: &P,
        track_address: &Pubkey,
        spool_group: SpoolGroup,
        system: &System,
    ) -> Result<CollectedSignatures, CertificationError> {
        let committee = &system.committee;
        let committee_size = committee.size();
        let group_total_weight = SPOOL_GROUP_SIZE as u64;

        if committee_size == 0 {
            return Err(CertificationError::NoCommitteeMembers);
        }

        let group_members = collect_group_members(spool_group, system);

        // Build signature requests
        let mut remaining_node_weight = 0u64;
        let mut signature_requests: Vec<SignatureRequest> = Vec::new();

        for (member_idx, member) in system.committee.iter().enumerate() {
            if !group_members.contains(&(member_idx as u8)) {
                continue;
            }

            let node_id = member.id;
            let member_bitmap = CommitteeBitmap::from_indices(&[member_idx], committee_size);
            let node_weight = system.spools.group_weight(spool_group, &member_bitmap);
            if node_weight == 0 {
                continue;
            }

            remaining_node_weight += node_weight;
            signature_requests.push(SignatureRequest {
                node_id,
                member_idx: member_idx as u8,
                weight: node_weight,
            });
        }

        if signature_requests.is_empty() {
            return Err(CertificationError::InsufficientSignatures {
                got: 0,
                total: group_total_weight as usize,
            });
        }

        let track_pubkey = track_address.into();
        let max_retries = self.config.max_retries;

        let mut requests = stream::iter(signature_requests.into_iter().map(|request| {
            let track = track_pubkey;
            async move {
                let req = CertifyReq { track };
                let config = RetryConfig {
                    max_retries: Some(max_retries as u32),
                    ..RetryConfig::ten()
                };
                let result = retry(config, None, || peer_client.certify(request.node_id, &req)).await;
                NodeResult {
                    node_id: request.node_id,
                    member_idx: request.member_idx,
                    weight: request.weight,
                    result: result.map_err(NodeSignError::from),
                }
            }
        }))
        .buffer_unordered(self.config.max_concurrent);

        // Collect results, potentially exiting early
        let mut epoch_buckets: HashMap<u64, SignatureBucket> = HashMap::new();
        let mut failures: Vec<(NodeId, NodeSignError)> = Vec::new();
        let mut early_exit_triggered = false;

        while let Some(node_result) = requests.next().await {
            let remaining_after = remaining_node_weight.saturating_sub(node_result.weight);
            remaining_node_weight = remaining_after;

            match node_result.result {
                Ok(response) => {
                    let epoch_key = response.epoch.0;
                    tracing::debug!(
                        node_id = node_result.node_id.as_u64(),
                        member_idx = node_result.member_idx,
                        epoch = epoch_key,
                        "Got signature from node"
                    );

                    record_signature_response(
                        &mut epoch_buckets,
                        epoch_key,
                        response,
                        node_result.member_idx as usize,
                        node_result.weight,
                    );
                    let bucket = epoch_buckets
                        .get(&epoch_key)
                        .expect("epoch bucket inserted");

                    if self.config.early_exit
                        && is_supermajority(bucket.weight, group_total_weight)
                    {
                        let bitmap = CommitteeBitmap::from_indices(&bucket.member_indices, committee_size);
                        let responses = bucket.responses.clone();
                        tracing::info!(
                            signatures = bucket.responses.len(),
                            weight = bucket.weight,
                            group_size = SPOOL_GROUP_SIZE,
                            remaining_weight = remaining_node_weight,
                            "Spool group supermajority reached, exiting early"
                        );
                        early_exit_triggered = true;

                        let signatures: Vec<BlsSignature> = responses
                            .iter()
                            .map(|response| response.signature)
                            .collect();
                        let aggregated_signature = BlsSignature::aggregate(&signatures)
                            .map_err(|e| CertificationError::AggregationFailed(format!("{:?}", e)))?;
                        return Ok(CollectedSignatures {
                            aggregated_signature,
                            bitmap,
                            signature_count: signatures.len(),
                            committee_size,
                            epoch: epoch_key,
                            responses,
                            failures,
                            early_exit: early_exit_triggered,
                        });
                    }
                }
                Err(e) => {
                    match &e {
                        NodeSignError::NotFound => {
                            tracing::debug!(
                                node_id = node_result.node_id.as_u64(),
                                "Node doesn't have track data"
                            );
                        }
                        _ => {
                            tracing::warn!(
                                node_id = node_result.node_id.as_u64(),
                                error = ?e,
                                "Failed to get signature from node"
                            );
                        }
                    }
                    failures.push((node_result.node_id, e));
                }
            }

            let can_reach_quorum = can_reach_supermajority(
                &epoch_buckets,
                remaining_node_weight,
                group_total_weight,
            );
            if !can_reach_quorum {
                break;
            }
        }

        let selected_epoch = select_supermajority_epoch(&epoch_buckets, group_total_weight);

        if selected_epoch.is_none() {
            let (_, best_weight, _) = select_best_epoch(&epoch_buckets);
            return Err(CertificationError::InsufficientSignatures {
                got: best_weight,
                total: group_total_weight as usize,
            });
        }

        let selected_epoch = selected_epoch.expect("selected epoch");
        let selected_bucket = epoch_buckets.remove(&selected_epoch).expect("selected epoch should exist");
        let signature_count = selected_bucket.responses.len();
        let member_indices = selected_bucket.member_indices.clone();
        let check_bitmap = CommitteeBitmap::from_indices(&member_indices, committee_size);
        let final_weight = system.spools.group_weight(spool_group, &check_bitmap);
        if !is_supermajority(final_weight, group_total_weight) {
            return Err(CertificationError::InsufficientSignatures {
                got: final_weight as usize,
                total: group_total_weight as usize,
            });
        }

        let bitmap = check_bitmap;
        let signatures: Vec<BlsSignature> = selected_bucket
            .responses
            .iter()
            .map(|response| response.signature)
            .collect();

        let aggregated_signature = BlsSignature::aggregate(&signatures)
            .map_err(|e| CertificationError::AggregationFailed(format!("{:?}", e)))?;

        let responses = selected_bucket.responses;

        Ok(CollectedSignatures {
            aggregated_signature,
            bitmap,
            signature_count,
            committee_size,
            epoch: selected_epoch,
            responses,
            failures,
            early_exit: early_exit_triggered,
        })
    }

    /// Build a CertifyTrack instruction from collected signatures.
    pub fn build_certify_instruction(
        fee_payer: Pubkey,
        authority: Pubkey,
        track: CompressedTrackProof,
        collected: &CollectedSignatures,
    ) -> solana_sdk::instruction::Instruction {
        build_certify_track_ix(
            fee_payer,
            authority,
            track,
            EpochNumber(collected.epoch),
            collected.bitmap,
            collected.aggregated_signature,
        )
    }
}

struct NodeResult {
    node_id: NodeId,
    member_idx: u8,
    weight: u64,
    result: Result<CertifyRes, NodeSignError>,
}

struct SignatureBucket {
    responses: Vec<CertifyRes>,
    weight: u64,
    member_indices: Vec<usize>,
}

struct SignatureRequest {
    node_id: NodeId,
    member_idx: u8,
    weight: u64,
}

fn collect_group_members(spool_group: SpoolGroup, system: &System) -> HashSet<u8> {
    let mut members = HashSet::new();
    for i in 0..SPOOL_GROUP_SIZE {
        let spool = spool_for_slice(spool_group, i);
        let member = system.spools.0[spool as usize];
        members.insert(member);
    }
    members
}

fn record_signature_response(
    epoch_buckets: &mut HashMap<u64, SignatureBucket>,
    epoch_key: u64,
    response: CertifyRes,
    member_idx: usize,
    node_weight: u64,
) {
    let bucket = epoch_buckets.entry(epoch_key).or_insert(SignatureBucket {
        responses: Vec::new(),
        weight: 0,
        member_indices: Vec::new(),
    });
    bucket.responses.push(response);
    bucket.weight += node_weight;
    bucket.member_indices.push(member_idx);
}

fn can_reach_supermajority(
    epoch_buckets: &HashMap<u64, SignatureBucket>,
    remaining_node_weight: u64,
    group_total_weight: u64,
) -> bool {
    is_supermajority(remaining_node_weight, group_total_weight)
        || epoch_buckets
            .values()
            .any(|bucket| is_supermajority(bucket.weight + remaining_node_weight, group_total_weight))
}

fn select_supermajority_epoch(
    epoch_buckets: &HashMap<u64, SignatureBucket>,
    group_total_weight: u64,
) -> Option<u64> {
    let mut selected_epoch = None;
    let mut selected_weight = 0u64;

    for (epoch, bucket) in epoch_buckets.iter() {
        if !is_supermajority(bucket.weight, group_total_weight) {
            continue;
        }

        match selected_epoch {
            Some(selected) => {
                if bucket.weight > selected_weight
                    || (bucket.weight == selected_weight && *epoch > selected)
                {
                    selected_epoch = Some(*epoch);
                    selected_weight = bucket.weight;
                }
            }
            None => {
                selected_epoch = Some(*epoch);
                selected_weight = bucket.weight;
            }
        }
    }

    selected_epoch
}

fn select_best_epoch(epoch_buckets: &HashMap<u64, SignatureBucket>) -> (Option<u64>, usize, usize) {
    let mut best_epoch = None;
    let mut best_weight = 0usize;
    let mut best_signatures = 0usize;

    for (epoch, bucket) in epoch_buckets.iter() {
        if bucket.weight as usize > best_weight
            || (bucket.weight as usize == best_weight && bucket.responses.len() > best_signatures)
        {
            best_weight = bucket.weight as usize;
            best_signatures = bucket.responses.len();
            best_epoch = Some(*epoch);
        }
    }

    (best_epoch, best_weight, best_signatures)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certification_config_default() {
        let config = CertificationConfig::default();
        assert_eq!(config.max_concurrent, 32);
        assert_eq!(config.max_retries, 3);
        assert!(config.early_exit);
    }

    #[test]
    fn certification_config_fast() {
        let config = CertificationConfig::fast();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.max_concurrent, 64);
    }

    #[test]
    fn certification_config_resilient() {
        let config = CertificationConfig::resilient();
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.max_concurrent, 16);
    }

    #[test]
    fn collector_creation() {
        let collector = CertificationCollector::with_defaults();
        assert_eq!(collector.config.max_concurrent, 32);
    }

    #[test]
    fn collector_with_custom_config() {
        let config = CertificationConfig {
            max_concurrent: 16,
            max_retries: 2,
            early_exit: false,
        };
        let collector = CertificationCollector::new(config);
        assert_eq!(collector.config.max_concurrent, 16);
        assert_eq!(collector.config.max_retries, 2);
        assert!(!collector.config.early_exit);
    }

    #[test]
    fn certification_error_display() {
        let err = CertificationError::InsufficientSignatures { got: 5, total: 10 };
        let msg = format!("{}", err);
        assert!(msg.contains("5"));
        assert!(msg.contains("10"));

        let err = CertificationError::NoCommitteeMembers;
        assert!(format!("{}", err).contains("committee"));

        let err = CertificationError::AggregationFailed("test".to_string());
        assert!(format!("{}", err).contains("test"));
    }

    #[test]
    fn node_sign_error_from_peer_error() {
        let err = ApiError::NotFound;
        assert!(matches!(NodeSignError::from(&err), NodeSignError::NotFound));

        let err = ApiError::Timeout;
        assert!(matches!(NodeSignError::from(&err), NodeSignError::Timeout));

        let err = ApiError::NotInCommittee;
        assert!(matches!(
            NodeSignError::from(&err),
            NodeSignError::NotInCommittee
        ));
    }

    fn mock_certify_res(epoch: u64, node_id: u64) -> CertifyRes {
        CertifyRes {
            signature: BlsSignature(tape_crypto::bls12254::min_sig::G1CompressedPoint([1u8; 32])),
            node_id: NodeId::new(node_id),
            epoch: EpochNumber(epoch),
        }
    }

    #[test]
    fn reach_supermajority_by_remaining() {
        let buckets = HashMap::new();
        assert!(can_reach_supermajority(&buckets, 14, 20));
    }

    #[test]
    fn reach_supermajority_by_bucket() {
        let buckets = HashMap::from([
            (8, SignatureBucket {
                responses: Vec::new(),
                weight: 9,
                member_indices: Vec::new(),
            }),
            (9, SignatureBucket {
                responses: Vec::new(),
                weight: 6,
                member_indices: Vec::new(),
            }),
        ]);

        assert!(can_reach_supermajority(&buckets, 5, 20));
        assert!(!can_reach_supermajority(&buckets, 4, 20));
    }

    #[test]
    fn reach_supermajority_impossible() {
        let buckets = HashMap::from([
            (8, SignatureBucket {
                responses: Vec::new(),
                weight: 6,
                member_indices: Vec::new(),
            }),
            (9, SignatureBucket {
                responses: Vec::new(),
                weight: 5,
                member_indices: Vec::new(),
            }),
        ]);

        assert!(!can_reach_supermajority(&buckets, 4, 20));
    }

    #[test]
    fn test_select_supermajority_epoch() {
        let buckets = HashMap::from([
            (7, SignatureBucket {
                responses: Vec::new(),
                weight: 13,
                member_indices: Vec::new(),
            }),
            (8, SignatureBucket {
                responses: Vec::new(),
                weight: 14,
                member_indices: Vec::new(),
            }),
            (9, SignatureBucket {
                responses: Vec::new(),
                weight: 14,
                member_indices: Vec::new(),
            }),
        ]);

        assert_eq!(select_supermajority_epoch(&buckets, 20), Some(9));
    }

    #[test]
    fn select_best_epoch_weight_then_count() {
        let buckets = HashMap::from([
            (7, SignatureBucket {
                responses: vec![mock_certify_res(7, 1), mock_certify_res(7, 2)],
                weight: 12,
                member_indices: Vec::new(),
            }),
            (8, SignatureBucket {
                responses: vec![mock_certify_res(8, 3)],
                weight: 13,
                member_indices: Vec::new(),
            }),
        ]);

        let (selected, weight, count) = select_best_epoch(&buckets);
        assert_eq!(selected, Some(8));
        assert_eq!(weight, 13);
        assert_eq!(count, 1);
    }

    #[test]
    fn select_best_epoch_tie_count() {
        let buckets = HashMap::from([
            (7, SignatureBucket {
                responses: vec![mock_certify_res(7, 1)],
                weight: 12,
                member_indices: Vec::new(),
            }),
            (8, SignatureBucket {
                responses: vec![mock_certify_res(8, 2), mock_certify_res(8, 3)],
                weight: 12,
                member_indices: Vec::new(),
            }),
        ]);

        let (selected, weight, count) = select_best_epoch(&buckets);
        assert_eq!(selected, Some(8));
        assert_eq!(weight, 12);
        assert_eq!(count, 2);
    }
}
