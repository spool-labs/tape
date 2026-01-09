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
//! # Example
//!
//! ```rust,ignore
//! use tape_sdk::certification::{CertificationCollector, CertificationConfig};
//!
//! let collector = CertificationCollector::new(config);
//! let result = collector.collect_and_certify(&track_address, &rpc).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use tape_api::instruction::build_certify_track_ix;
use tape_api::program::tapedrive::{CommitteeBitmap, MEMBER_COUNT};
use tape_api::state::System;
use tape_core::bft::is_supermajority;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::types::NodeId;
use tape_crypto::Hash;
use tape_node_client::{NodeError, SignResponse};

use crate::communication::NodeCommunicationFactory;

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
}

/// Configuration for certification collection.
#[derive(Clone, Debug)]
pub struct CertificationConfig {
    /// Connection timeout for node requests.
    pub connect_timeout: Duration,
    /// Request timeout for node requests.
    pub request_timeout: Duration,
    /// Maximum concurrent signature requests.
    pub max_concurrent: usize,
}

impl Default for CertificationConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_concurrent: 32,
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
    /// Individual responses (for debugging/verification).
    pub responses: Vec<SignResponse>,
}

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
    pub async fn collect_signatures(
        &self,
        track_address: &Pubkey,
        system: &System,
        node_addresses: &HashMap<NodeId, String>,
    ) -> Result<CollectedSignatures, CertificationError> {
        let committee = &system.committee;
        let committee_size = committee.size();

        if committee_size == 0 {
            return Err(CertificationError::NoCommitteeMembers);
        }

        // Track ID is the base58 string of the track address
        let track_id = track_address.to_string();

        // Collect signatures in parallel with bounded concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent));
        let mut handles = Vec::new();

        for (member_idx, member) in committee.iter().enumerate() {
            // Skip empty slots
            if member.id == NodeId(0) {
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

            let handle = tokio::spawn(async move {
                let _permit = sem_clone.acquire_owned().await;
                let result = client.get_signature(&track_id_clone).await;
                (node_id, member_idx as u8, pubkey, result)
            });

            handles.push(handle);
        }

        // Wait for all requests and collect results
        let mut successful_responses: Vec<(u8, BlsPubkey, SignResponse)> = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((node_id, member_idx, pubkey, Ok(response))) => {
                    tracing::debug!(
                        node_id = node_id.as_u64(),
                        member_idx = member_idx,
                        "Got signature from node"
                    );
                    successful_responses.push((member_idx, pubkey, response));
                }
                Ok((node_id, _member_idx, _pubkey, Err(NodeError::NotFound))) => {
                    // Node doesn't have data - this is expected during sync
                    tracing::debug!(
                        node_id = node_id.as_u64(),
                        "Node doesn't have track data"
                    );
                }
                Ok((node_id, _member_idx, _pubkey, Err(e))) => {
                    tracing::warn!(
                        node_id = node_id.as_u64(),
                        error = %e,
                        "Failed to get signature from node"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Signature request task panicked");
                }
            }
        }

        // Check if we have supermajority
        let got = successful_responses.len();
        if !is_supermajority(got as u64, committee_size as u64) {
            return Err(CertificationError::InsufficientSignatures {
                got,
                total: committee_size,
            });
        }

        // Build bitmap from member indices
        let member_indices: Vec<usize> = successful_responses.iter().map(|(idx, _, _)| *idx as usize).collect();
        let bitmap = CommitteeBitmap::from_indices(&member_indices, MEMBER_COUNT);

        // Extract signatures and aggregate
        let signatures: Vec<BlsSignature> = successful_responses
            .iter()
            .map(|(_, _, resp)| {
                BlsSignature(tape_crypto::bls12254::min_sig::G1CompressedPoint(resp.signature))
            })
            .collect();

        let aggregated_signature = BlsSignature::aggregate(&signatures)
            .map_err(|e| CertificationError::AggregationFailed(format!("{:?}", e)))?;

        // Collect responses for return
        let responses: Vec<SignResponse> = successful_responses
            .into_iter()
            .map(|(_, _, resp)| resp)
            .collect();

        Ok(CollectedSignatures {
            aggregated_signature,
            bitmap,
            signature_count: got,
            committee_size,
            responses,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_certification_config_default() {
        let config = CertificationConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.max_concurrent, 32);
    }

    #[test]
    fn test_certification_config_custom() {
        let config = CertificationConfig {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_concurrent: 64,
        };
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.max_concurrent, 64);
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
        };
        let collector = CertificationCollector::new(config);
        assert_eq!(collector.config.max_concurrent, 16);
        assert_eq!(collector.config.connect_timeout, Duration::from_secs(1));
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
}
