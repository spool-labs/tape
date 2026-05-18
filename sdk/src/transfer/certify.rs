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

use std::collections::HashMap;

use futures::stream::{self, StreamExt};
use tape_crypto::address::Address;
use thiserror::Error;

use tape_api::instruction::build_certify_track_ix;
use tape_core::bft::is_supermajority;
use tape_core::bls::BlsSignature;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::{BitmapRead, EpochNumber, SpoolBitmap};
use tape_protocol::api::{Api, ApiError, CertifyReq, CertifyRes};
use tape_protocol::ProtocolState;
use tape_retry::{retry, RetryConfig};

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
    #[error("failed to connect to node {node}: {message}")]
    ConnectionFailed { node: Address, message: String },

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
    #[error("epoch mismatch: expected {expected}, got {got} from node {node}")]
    EpochMismatch {
        expected: u64,
        got: u64,
        node: Address,
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
    /// Bitmap indicating which group spool positions signed.
    pub bitmap: SpoolBitmap,
    /// Number of signatures collected.
    pub signature_count: usize,
    /// Total spool positions in the group.
    pub spool_count: usize,
    /// The epoch that was signed (all responses must agree).
    pub epoch: u64,
    /// Individual responses (for debugging/verification).
    pub responses: Vec<CertifyRes>,
    /// Nodes that failed and why (for diagnostics).
    pub failures: Vec<(Address, NodeSignError)>,
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
        track_address: &Address,
        group: GroupIndex,
        state: &ProtocolState,
    ) -> Result<CollectedSignatures, CertificationError> {
        let group_total_weight = GROUP_SIZE as u64;

        if state.current.committee.is_empty() {
            return Err(CertificationError::NoCommitteeMembers);
        }

        let signature_requests = collect_signature_requests(group, state)?;
        let mut remaining_node_weight = signature_requests
            .iter()
            .map(|request| request.weight)
            .sum::<u64>();

        if signature_requests.is_empty() {
            return Err(CertificationError::InsufficientSignatures {
                got: 0,
                total: group_total_weight as usize,
            });
        }

        let track = *track_address;
        let max_retries = self.config.max_retries;

        let mut requests = stream::iter(signature_requests.into_iter().map(|request| {
            let track = track;
            async move {
                let req = CertifyReq { track };
                let config = RetryConfig {
                    max_retries: Some(max_retries as u32),
                    ..RetryConfig::ten()
                };
                let result = retry(config, None, || peer_client.certify(request.node, &req)).await;
                NodeResult {
                    node: request.node,
                    positions: request.positions,
                    weight: request.weight,
                    result: result.map_err(NodeSignError::from),
                }
            }
        }))
        .buffer_unordered(self.config.max_concurrent);

        // Collect results, potentially exiting early
        let mut epoch_buckets: HashMap<u64, SignatureBucket> = HashMap::new();
        let mut failures: Vec<(Address, NodeSignError)> = Vec::new();
        let mut early_exit_triggered = false;

        while let Some(node_result) = requests.next().await {
            let remaining_after = remaining_node_weight.saturating_sub(node_result.weight);
            remaining_node_weight = remaining_after;

            match node_result.result {
                Ok(response) => {
                    let epoch_key = response.epoch.0;
                    tracing::debug!(
                        node = %node_result.node,
                        positions = ?node_result.positions,
                        epoch = epoch_key,
                        "Got signature from node"
                    );

                    record_signature_response(
                        &mut epoch_buckets,
                        epoch_key,
                        response,
                        &node_result.positions,
                    );
                    let bucket = epoch_buckets
                        .get(&epoch_key)
                        .expect("epoch bucket inserted");

                    if self.config.early_exit
                        && is_supermajority(bucket.weight, group_total_weight)
                    {
                        let bitmap = SpoolBitmap::from_indices(&bucket.positions);
                        let responses = bucket.responses.clone();
                        tracing::info!(
                            signatures = bucket.responses.len(),
                            weight = bucket.weight,
                            group_size = GROUP_SIZE,
                            remaining_weight = remaining_node_weight,
                            "Spool group supermajority reached, exiting early"
                        );
                        early_exit_triggered = true;

                        let aggregated_signature = BlsSignature::aggregate(&bucket.signatures)
                            .map_err(|e| CertificationError::AggregationFailed(format!("{:?}", e)))?;
                        return Ok(CollectedSignatures {
                            aggregated_signature,
                            bitmap,
                            signature_count: responses.len(),
                            spool_count: GROUP_SIZE,
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
                                node = %node_result.node,
                                "Node doesn't have track data"
                            );
                        }
                        _ => {
                            tracing::warn!(
                                node = %node_result.node,
                                error = ?e,
                                "Failed to get signature from node"
                            );
                        }
                    }
                    failures.push((node_result.node, e));
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
        let check_bitmap = SpoolBitmap::from_indices(&selected_bucket.positions);
        let final_weight = check_bitmap.count_ones() as u64;
        if !is_supermajority(final_weight, group_total_weight) {
            return Err(CertificationError::InsufficientSignatures {
                got: final_weight as usize,
                total: group_total_weight as usize,
            });
        }

        let bitmap = check_bitmap;
        let aggregated_signature = BlsSignature::aggregate(&selected_bucket.signatures)
            .map_err(|e| CertificationError::AggregationFailed(format!("{:?}", e)))?;

        let responses = selected_bucket.responses;

        Ok(CollectedSignatures {
            aggregated_signature,
            bitmap,
            signature_count,
            spool_count: GROUP_SIZE,
            epoch: selected_epoch,
            responses,
            failures,
            early_exit: early_exit_triggered,
        })
    }

    /// Build a CertifyTrack instruction from collected signatures.
    pub fn build_certify_instruction(
        fee_payer: Address,
        authority: Address,
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
    node: Address,
    positions: Vec<usize>,
    weight: u64,
    result: Result<CertifyRes, NodeSignError>,
}

struct SignatureBucket {
    responses: Vec<CertifyRes>,
    signatures: Vec<BlsSignature>,
    weight: u64,
    positions: Vec<usize>,
}

struct SignatureRequest {
    node: Address,
    positions: Vec<usize>,
    weight: u64,
}

fn collect_signature_requests(
    group: GroupIndex,
    state: &ProtocolState,
) -> Result<Vec<SignatureRequest>, CertificationError> {
    let spools = state
        .spools_in_group(group)
        .ok_or_else(|| CertificationError::SystemState(format!("group {group} not found")))?;

    let mut by_node = HashMap::<Address, SignatureRequest>::new();
    for (spool_index, spool) in spools {
        if spool.node == Address::default() {
            continue;
        }
        let position = group
            .position_of(spool_index)
            .ok_or_else(|| CertificationError::SystemState(format!("spool {spool_index} is outside group {group}")))?;
        let request = by_node.entry(spool.node).or_insert_with(|| SignatureRequest {
            node: spool.node,
            positions: Vec::new(),
            weight: 0,
        });
        request.positions.push(position);
        request.weight += 1;
    }

    Ok(by_node.into_values().collect())
}

fn record_signature_response(
    epoch_buckets: &mut HashMap<u64, SignatureBucket>,
    epoch_key: u64,
    response: CertifyRes,
    positions: &[usize],
) {
    let signature = response.signature;
    let bucket = epoch_buckets.entry(epoch_key).or_insert(SignatureBucket {
        responses: Vec::new(),
        signatures: Vec::new(),
        weight: 0,
        positions: Vec::new(),
    });
    bucket.responses.push(response);
    bucket.weight += positions.len() as u64;
    bucket.positions.extend_from_slice(positions);
    bucket
        .signatures
        .extend(std::iter::repeat(signature).take(positions.len()));
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
        let mut bytes = [0u8; 32];
        bytes[0] = node_id as u8;
        CertifyRes {
            signature: BlsSignature(tape_crypto::bls12254::min_sig::G1CompressedPoint([1u8; 32])),
            node: Address::new(bytes),
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
                signatures: Vec::new(),
                weight: 9,
                positions: Vec::new(),
            }),
            (9, SignatureBucket {
                responses: Vec::new(),
                signatures: Vec::new(),
                weight: 6,
                positions: Vec::new(),
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
                signatures: Vec::new(),
                weight: 6,
                positions: Vec::new(),
            }),
            (9, SignatureBucket {
                responses: Vec::new(),
                signatures: Vec::new(),
                weight: 5,
                positions: Vec::new(),
            }),
        ]);

        assert!(!can_reach_supermajority(&buckets, 4, 20));
    }

    #[test]
    fn test_select_supermajority_epoch() {
        let buckets = HashMap::from([
            (7, SignatureBucket {
                responses: Vec::new(),
                signatures: Vec::new(),
                weight: 13,
                positions: Vec::new(),
            }),
            (8, SignatureBucket {
                responses: Vec::new(),
                signatures: Vec::new(),
                weight: 14,
                positions: Vec::new(),
            }),
            (9, SignatureBucket {
                responses: Vec::new(),
                signatures: Vec::new(),
                weight: 14,
                positions: Vec::new(),
            }),
        ]);

        assert_eq!(select_supermajority_epoch(&buckets, 20), Some(9));
    }

    #[test]
    fn select_best_epoch_weight_then_count() {
        let buckets = HashMap::from([
            (7, SignatureBucket {
                responses: vec![mock_certify_res(7, 1), mock_certify_res(7, 2)],
                signatures: Vec::new(),
                weight: 12,
                positions: Vec::new(),
            }),
            (8, SignatureBucket {
                responses: vec![mock_certify_res(8, 3)],
                signatures: Vec::new(),
                weight: 13,
                positions: Vec::new(),
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
                signatures: Vec::new(),
                weight: 12,
                positions: Vec::new(),
            }),
            (8, SignatureBucket {
                responses: vec![mock_certify_res(8, 2), mock_certify_res(8, 3)],
                signatures: Vec::new(),
                weight: 12,
                positions: Vec::new(),
            }),
        ]);

        let (selected, weight, count) = select_best_epoch(&buckets);
        assert_eq!(selected, Some(8));
        assert_eq!(weight, 12);
        assert_eq!(count, 2);
    }
}
