//! Track certification module.
//!
//! This module provides functionality for collecting BLS signatures from committee
//! members and building certification transactions for tracks.
//!
//! # Certification Flow
//!
//! 1. **Collect Signatures**: Query committee members in parallel for BLS signatures
//! 2. **Verify Supermajority**: Ensure we have at least 2f+1 signatures
//! 3. **Aggregate Signatures**: Combine individual BLS signatures into one
//! 4. **Build Transaction**: Create the CertifyTrack instruction with bitmap + aggregated signature
//!
//! # Features
//!
//! - **Bounded concurrency**: Limits parallel requests to avoid overwhelming nodes
//! - **Early exit**: Stops collecting once supermajority is reached
//! - **Retry with backoff**: Retries transient failures with exponential backoff
//! - **Detailed errors**: Specific error types for different failure modes
//!
//! # Example
//!
//! ```rust,ignore
//! use tape_sdk::certification::{CertificationCollector, CertificationConfig};
//!
//! let collector = CertificationCollector::new(config);
//! let result = collector.collect_signatures(&track_address, &system, &node_addresses).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use thiserror::Error;
use tokio::sync::mpsc;

use tape_api::instruction::build_certify_track_ix;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_api::state::System;
use tape_core::bft::is_supermajority;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::types::NodeId;
use tape_crypto::Hash;
use tape_node_client::{with_retry, NodeClient, NodeError, RetryConfig, SignResponse};

use crate::communication::NodeCommunicationFactory;

// ============================================================================
// Error Types
// ============================================================================

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
    /// Node hasn't stored all its assigned slices for this track.
    MissingSlices { have: u16, need: u16 },
    /// Track not found on this node.
    NotFound,
    /// Connection or network error.
    Network(String),
    /// Request timed out after all retries.
    Timeout,
    /// Other error.
    Other(String),
}

impl From<&NodeError> for NodeSignError {
    fn from(err: &NodeError) -> Self {
        match err {
            NodeError::NotFound => NodeSignError::NotFound,
            NodeError::NotInCommittee => NodeSignError::NotInCommittee,
            NodeError::MissingSlices { have, need } => {
                NodeSignError::MissingSlices { have: *have, need: *need }
            }
            NodeError::Timeout => NodeSignError::Timeout,
            NodeError::Connection(msg) => NodeSignError::Network(msg.clone()),
            NodeError::Request(e) => {
                if e.is_timeout() {
                    NodeSignError::Timeout
                } else if e.is_connect() {
                    NodeSignError::Network(e.to_string())
                } else {
                    NodeSignError::Network(e.to_string())
                }
            }
            _ => NodeSignError::Other(err.to_string()),
        }
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for certification collection.
#[derive(Clone, Debug)]
pub struct CertificationConfig {
    /// Connection timeout for node requests.
    pub connect_timeout: Duration,
    /// Request timeout for node requests.
    pub request_timeout: Duration,
    /// Maximum concurrent signature requests.
    pub max_concurrent: usize,
    /// Retry configuration for transient failures.
    pub retry: RetryConfig,
    /// Whether to exit early once supermajority is reached.
    pub early_exit: bool,
}

impl Default for CertificationConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_concurrent: 32,
            retry: RetryConfig::default(),
            early_exit: true,
        }
    }
}

impl CertificationConfig {
    /// Create config optimized for fast networks (lower timeouts, fewer retries).
    pub fn fast() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            max_concurrent: 64,
            retry: RetryConfig::fast(),
            early_exit: true,
        }
    }

    /// Create config optimized for unreliable networks (higher timeouts, more retries).
    pub fn resilient() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_concurrent: 16,
            retry: RetryConfig::resilient(),
            early_exit: true,
        }
    }
}

// ============================================================================
// Result Types
// ============================================================================

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
    pub responses: Vec<SignResponse>,
    /// Nodes that failed and why (for diagnostics).
    pub failures: Vec<(NodeId, NodeSignError)>,
    /// Whether collection exited early (supermajority reached before all responses).
    pub early_exit: bool,
}

/// Result from a single node signature request.
struct NodeResult {
    node_id: NodeId,
    member_idx: u8,
    pubkey: BlsPubkey,
    result: Result<SignResponse, NodeSignError>,
}

// ============================================================================
// Collector
// ============================================================================

/// Collector for gathering BLS signatures from committee members.
pub struct CertificationCollector {
    config: CertificationConfig,
    factory: NodeCommunicationFactory,
}

impl CertificationCollector {
    /// Create a new certification collector with the given configuration.
    pub fn new(config: CertificationConfig) -> Self {
        let factory = NodeCommunicationFactory::new()
            .with_connect_timeout(config.connect_timeout)
            .with_request_timeout(config.request_timeout);

        Self { config, factory }
    }

    /// Create a new certification collector with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CertificationConfig::default())
    }

    /// Collect signatures from committee members for a track.
    ///
    /// # Arguments
    /// * `track_address` - The on-chain track address (used as message for signing)
    /// * `system` - Current on-chain system state (for committee info)
    /// * `node_addresses` - Map of NodeId -> network address
    ///
    /// # Returns
    /// * `Ok(CollectedSignatures)` - Aggregated signature and bitmap if supermajority achieved
    /// * `Err(CertificationError)` - If insufficient signatures or other error
    ///
    /// # Features
    /// * Bounded concurrency via semaphore
    /// * Early exit when supermajority reached (if enabled)
    /// * Retry with exponential backoff for transient failures
    pub async fn collect_signatures(
        &self,
        track_address: &Pubkey,
        spool_group: SpoolGroup,
        system: &System,
        node_addresses: &HashMap<NodeId, String>,
    ) -> Result<CollectedSignatures, CertificationError> {
        let committee = &system.committee;
        let committee_size = committee.size();

        if committee_size == 0 {
            return Err(CertificationError::NoCommitteeMembers);
        }

        // Determine which members own spools in this group (deduplicated)
        let group_members: std::collections::HashSet<u8> = {
            let mut members = std::collections::HashSet::new();
            for i in 0..SPOOL_GROUP_SIZE {
                let spool = tape_core::erasure::spool_for_slice(spool_group, i);
                let member = system.spools.0[spool as usize];
                members.insert(member);
            }
            members
        };

        // Track ID is the base58 string of the track address
        let track_id = track_address.to_string();

        let target_count = group_members.len();
        // Channel for streaming results as they complete
        let (tx, mut rx) = mpsc::channel::<NodeResult>(target_count.max(1));

        // Semaphore for bounded concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent));

        // Spawn tasks only for members who own spools in this group
        let mut task_count = 0;
        for (member_idx, member) in committee.iter().enumerate() {
            if !group_members.contains(&(member_idx as u8)) {
                continue;
            }
            // Look up network address
            let address = match node_addresses.get(&member.id) {
                Some(addr) => addr.clone(),
                None => {
                    tracing::warn!(node_id = member.id.as_u64(), "No address found for node");
                    continue;
                }
            };

            let client = match self.factory.client_for_address(&address) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        node_id = member.id.as_u64(),
                        error = %e,
                        "Failed to create client for node"
                    );
                    continue;
                }
            };

            let track_id_clone = track_id.clone();
            let node_id = member.id;
            let pubkey = member.key;
            let sem_clone = semaphore.clone();
            let tx_clone = tx.clone();
            let retry_config = self.config.retry.clone();

            tokio::spawn(async move {
                let _permit = sem_clone.acquire_owned().await;
                let result = request_signature_with_retry(&client, &track_id_clone, &retry_config).await;

                // Send result through channel (ignore error if receiver dropped - early exit)
                let _ = tx_clone
                    .send(NodeResult {
                        node_id,
                        member_idx: member_idx as u8,
                        pubkey,
                        result,
                    })
                    .await;
            });

            task_count += 1;
        }

        // Drop our sender so channel closes when all tasks complete
        drop(tx);

        // Collect results, potentially exiting early
        let mut successful: Vec<(u8, BlsPubkey, SignResponse)> = Vec::new();
        let mut failures: Vec<(NodeId, NodeSignError)> = Vec::new();
        let mut received = 0;
        let mut early_exit_triggered = false;
        let mut expected_epoch: Option<u64> = None;

        while let Some(node_result) = rx.recv().await {
            received += 1;

            match node_result.result {
                Ok(response) => {
                    // Verify epoch consistency - all signatures must be for the same epoch
                    match expected_epoch {
                        None => {
                            expected_epoch = Some(response.epoch);
                            tracing::debug!(epoch = response.epoch, "First signature epoch");
                        }
                        Some(expected) if response.epoch != expected => {
                            // Epoch mismatch - this could happen during epoch transitions
                            tracing::warn!(
                                node_id = node_result.node_id.as_u64(),
                                expected_epoch = expected,
                                got_epoch = response.epoch,
                                "Node returned signature for different epoch, skipping"
                            );
                            failures.push((
                                node_result.node_id,
                                NodeSignError::Other(format!(
                                    "epoch mismatch: expected {}, got {}",
                                    expected, response.epoch
                                )),
                            ));
                            continue;
                        }
                        Some(_) => {} // Epoch matches, continue
                    }

                    tracing::debug!(
                        node_id = node_result.node_id.as_u64(),
                        member_idx = node_result.member_idx,
                        epoch = response.epoch,
                        "Got signature from node"
                    );
                    successful.push((node_result.member_idx, node_result.pubkey, response));

                    // Check for early exit using spool-group-weighted supermajority
                    // Weight = number of spools in the group owned by signers
                    let signer_weight = system.spools.group_weight(spool_group, &{
                        let indices: Vec<usize> = successful.iter().map(|(idx, _, _)| *idx as usize).collect();
                        tape_api::program::tapedrive::CommitteeBitmap::from_indices(&indices, committee_size)
                    });
                    if self.config.early_exit
                        && is_supermajority(signer_weight, SPOOL_GROUP_SIZE as u64)
                    {
                        tracing::info!(
                            signatures = successful.len(),
                            weight = signer_weight,
                            group_size = SPOOL_GROUP_SIZE,
                            remaining = task_count - received,
                            "Spool group supermajority reached, exiting early"
                        );
                        early_exit_triggered = true;
                        break;
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
        }

        // Check if we have spool-weighted supermajority within the group
        let signature_count = successful.len();
        let member_indices: Vec<usize> = successful.iter().map(|(idx, _, _)| *idx as usize).collect();
        let check_bitmap = tape_api::program::tapedrive::CommitteeBitmap::from_indices(&member_indices, committee_size);
        let final_weight = system.spools.group_weight(spool_group, &check_bitmap);
        if !is_supermajority(final_weight, SPOOL_GROUP_SIZE as u64) {
            return Err(CertificationError::InsufficientSignatures {
                got: final_weight as usize,
                total: SPOOL_GROUP_SIZE,
            });
        }

        // Build bitmap from member indices (reuse from supermajority check above)
        let bitmap = CommitteeBitmap::from_indices(&member_indices, committee_size);

        // Extract signatures and aggregate
        let signatures: Vec<BlsSignature> = successful
            .iter()
            .map(|(_, _, resp)| {
                BlsSignature(tape_crypto::bls12254::min_sig::G1CompressedPoint(
                    resp.signature,
                ))
            })
            .collect();

        let aggregated_signature = BlsSignature::aggregate(&signatures)
            .map_err(|e| CertificationError::AggregationFailed(format!("{:?}", e)))?;

        // Collect responses for return
        let responses: Vec<SignResponse> =
            successful.into_iter().map(|(_, _, resp)| resp).collect();

        // Epoch should always be Some at this point since we have signatures
        let epoch = expected_epoch.expect("should have epoch from successful signatures");

        Ok(CollectedSignatures {
            aggregated_signature,
            bitmap,
            signature_count,
            committee_size,
            epoch,
            responses,
            failures,
            early_exit: early_exit_triggered,
        })
    }

    /// Build a CertifyTrack instruction from collected signatures.
    ///
    /// # Arguments
    /// * `fee_payer` - The fee payer for the transaction
    /// * `authority` - The track authority (owner)
    /// * `track_key` - The track key hash (used to derive track PDA)
    /// * `collected` - The collected signatures from `collect_signatures()`
    pub fn build_certify_instruction(
        fee_payer: Pubkey,
        authority: Pubkey,
        track_key: Hash,
        collected: &CollectedSignatures,
    ) -> solana_sdk::instruction::Instruction {
        build_certify_track_ix(
            fee_payer,
            authority,
            track_key,
            collected.bitmap,
            collected.aggregated_signature,
        )
    }
}

// ============================================================================
// Retry Logic
// ============================================================================

/// Request a signature from a node with retry logic.
async fn request_signature_with_retry(
    client: &NodeClient,
    track_id: &str,
    retry_config: &RetryConfig,
) -> Result<SignResponse, NodeSignError> {
    with_retry(retry_config, || client.get_signature(track_id))
        .await
        .map_err(|e| NodeSignError::from(&e))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_certification_config_default() {
        let config = CertificationConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.max_concurrent, 32);
        assert_eq!(config.retry.max_retries, 3);
        assert!(config.early_exit);
    }

    #[test]
    fn test_certification_config_fast() {
        let config = CertificationConfig::fast();
        assert_eq!(config.retry.max_retries, 1);
        assert_eq!(config.max_concurrent, 64);
    }

    #[test]
    fn test_certification_config_resilient() {
        let config = CertificationConfig::resilient();
        assert_eq!(config.retry.max_retries, 5);
        assert_eq!(config.max_concurrent, 16);
    }

    #[test]
    fn test_collector_creation() {
        let collector = CertificationCollector::with_defaults();
        assert_eq!(collector.config.max_concurrent, 32);
    }

    #[test]
    fn test_collector_with_custom_config() {
        let config = CertificationConfig {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            max_concurrent: 16,
            retry: RetryConfig {
                max_retries: 2,
                base_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(1),
            },
            early_exit: false,
        };
        let collector = CertificationCollector::new(config);
        assert_eq!(collector.config.max_concurrent, 16);
        assert_eq!(collector.config.retry.max_retries, 2);
        assert!(!collector.config.early_exit);
    }

    #[test]
    fn test_certification_error_display() {
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
    fn test_node_sign_error_from_node_error() {
        let err = NodeError::NotFound;
        assert!(matches!(NodeSignError::from(&err), NodeSignError::NotFound));

        let err = NodeError::Timeout;
        assert!(matches!(NodeSignError::from(&err), NodeSignError::Timeout));

        let err = NodeError::NotInCommittee;
        assert!(matches!(
            NodeSignError::from(&err),
            NodeSignError::NotInCommittee
        ));
    }

    #[test]
    fn test_node_error_is_retryable() {
        // Transient errors should be retryable
        assert!(NodeError::Timeout.is_retryable());
        assert!(NodeError::Connection("test".into()).is_retryable());

        // Non-transient errors should not be retried
        assert!(!NodeError::NotFound.is_retryable());
        assert!(!NodeError::NotInCommittee.is_retryable());
        assert!(!NodeError::MissingSlices { have: 5, need: 10 }.is_retryable());
    }
}
