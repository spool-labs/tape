//! Epoch management for the storage node.
//!
//! Handles epoch transitions including:
//! - Polling for epoch changes on chain
//! - Computing spool assignment changes
//! - Syncing data from previous spool owners
//! - Submitting sync completion proofs

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_api::instruction::build_epoch_sync_ix;
use tape_api::program::tapedrive::node_pda;
use tape_client::TapeClient;
use tape_core::prelude::*;
use tape_core::spooler::SpoolIndex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::spool_sync::{SpoolSyncHandler, SyncError};
use crate::StorageService;

/// Default polling interval for epoch changes.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Error type for epoch operations.
#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    #[error("sync error: {0}")]
    Sync(#[from] SyncError),

    #[error("failed to fetch epoch: {0}")]
    FetchEpoch(String),

    #[error("failed to compute spool changes: {0}")]
    SpoolChanges(String),

    #[error("failed to submit sync completion: {0}")]
    SubmitSyncDone(String),
}

/// Manages epoch transitions for the storage node.
///
/// Polls the chain for epoch changes and coordinates:
/// 1. Detecting new epochs
/// 2. Computing spool assignment changes
/// 3. Syncing data from previous owners
/// 4. Submitting sync completion proofs
pub struct EpochManager<S: store::Store = store_rocks::RocksStore> {
    /// Shared tape client for chain interactions.
    client: Arc<TapeClient>,
    /// This node's authority keypair for signing transactions.
    authority_keypair: Arc<Keypair>,
    /// This node's authority pubkey.
    authority: tape_crypto::Pubkey,
    /// Current epoch number (stored as u64 for atomic operations).
    current_epoch: AtomicU64,
    /// Handler for syncing spools from other nodes.
    spool_sync: Arc<SpoolSyncHandler>,
    /// Storage service for persisting synced slices.
    storage: Arc<StorageService<S>>,
    /// Polling interval for epoch changes.
    poll_interval: Duration,
}

impl<S: store::Store> EpochManager<S> {
    /// Load current epoch as EpochNumber.
    fn load_epoch(&self) -> EpochNumber {
        EpochNumber::new(self.current_epoch.load(Ordering::SeqCst))
    }

    /// Store epoch from EpochNumber.
    fn store_epoch(&self, epoch: EpochNumber) {
        self.current_epoch.store(epoch.as_u64(), Ordering::SeqCst);
    }
}

impl<S: store::Store + Send + Sync + 'static> EpochManager<S> {
    /// Create a new epoch manager.
    pub fn new(
        client: Arc<TapeClient>,
        authority_keypair: Arc<Keypair>,
        storage: Arc<StorageService<S>>,
    ) -> Self {
        let authority = authority_keypair.pubkey();
        Self {
            client,
            authority_keypair,
            authority,
            current_epoch: AtomicU64::new(0),
            spool_sync: Arc::new(SpoolSyncHandler::new()),
            storage,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Set a custom polling interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set a custom spool sync handler.
    pub fn with_spool_sync(mut self, handler: Arc<SpoolSyncHandler>) -> Self {
        self.spool_sync = handler;
        self
    }

    /// Get the current local epoch.
    pub fn current_epoch(&self) -> EpochNumber {
        self.load_epoch()
    }

    /// Run the epoch manager until shutdown.
    pub async fn run(&self, shutdown: CancellationToken) {
        info!("Epoch manager starting");

        // Fetch initial epoch
        match self.fetch_current_epoch().await {
            Ok(epoch) => {
                self.store_epoch(epoch);
                info!(epoch = epoch.as_u64(), "Initialized with current epoch");
            }
            Err(e) => {
                error!(error = %e, "Failed to fetch initial epoch, starting from 0");
            }
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Epoch manager shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    if let Err(e) = self.poll_epoch_change().await {
                        error!(error = %e, "Epoch poll error");
                    }
                }
            }
        }
    }

    /// Poll for epoch changes and handle them.
    async fn poll_epoch_change(&self) -> Result<(), EpochError> {
        let on_chain_epoch = self.fetch_current_epoch().await?;
        let local_epoch = self.load_epoch();

        if on_chain_epoch > local_epoch {
            info!(
                from_epoch = local_epoch.as_u64(),
                to_epoch = on_chain_epoch.as_u64(),
                "Epoch change detected"
            );
            self.handle_epoch_change(local_epoch, on_chain_epoch).await?;
        }

        Ok(())
    }

    /// Handle an epoch transition.
    async fn handle_epoch_change(
        &self,
        from_epoch: EpochNumber,
        to_epoch: EpochNumber,
    ) -> Result<(), EpochError> {
        // 1. Compute spool changes
        let (released_spools, new_spools) = self.compute_spool_changes(from_epoch, to_epoch).await?;

        info!(
            released = released_spools.len(),
            new = new_spools.len(),
            "Computed spool changes"
        );

        // 2. Sync new spools from previous owners
        if !new_spools.is_empty() {
            info!(count = new_spools.len(), "Syncing new spools");

            let storage = Arc::clone(&self.storage);
            let store_slice = Arc::new(move |track_id: String, idx: SpoolIndex, data: Vec<u8>| {
                // Parse track ID (base58 pubkey string) to Pubkey
                use std::str::FromStr;
                let track = tape_crypto::Pubkey::from_str(&track_id)
                    .map_err(|e| crate::spool_sync::SyncError::Storage(format!("Invalid track ID: {}", e)))?;

                // Create minimal metadata for synced slices
                let meta = crate::storage_service::SliceMeta {
                    len: data.len() as u32,
                    leaf_hash: tape_crypto::Hash::default(), // TODO: compute from data
                    merkle_proof: [tape_crypto::Hash::default(); crate::storage_service::MERKLE_HEIGHT],
                    compression: crate::storage_service::Compression::None,
                    received_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                };
                storage.put_slice(idx, track, data, meta)
                    .map_err(|e| crate::spool_sync::SyncError::Storage(e.to_string()))
            });

            let total_slices = self
                .spool_sync
                .sync_spools(new_spools, from_epoch, store_slice)
                .await?;

            info!(slices = total_slices, "Spool sync complete");
        }

        // 3. Submit sync completion proof
        let assigned_spools = self.get_assigned_spools().await?;
        self.submit_sync_completion(to_epoch, &assigned_spools).await?;

        // 4. Update local epoch
        self.store_epoch(to_epoch);

        info!(epoch = to_epoch.as_u64(), "Epoch transition complete");

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Chain interaction methods
    // -------------------------------------------------------------------------

    /// Fetch the current epoch number from the chain.
    async fn fetch_current_epoch(&self) -> Result<EpochNumber, EpochError> {
        let epoch = self
            .client
            .get_epoch()
            .await
            .map_err(|e| EpochError::FetchEpoch(format!("Failed to fetch epoch: {}", e)))?;

        debug!(epoch_id = epoch.id.as_u64(), "Fetched epoch from chain");
        Ok(epoch.id)
    }

    /// Compute spool assignment changes between epochs.
    ///
    /// Returns:
    /// - `released_spools`: Spools we no longer own (can clean up)
    /// - `new_spools`: Spools we now own with the network address of the previous owner
    async fn compute_spool_changes(
        &self,
        _from_epoch: EpochNumber,
        _to_epoch: EpochNumber,
    ) -> Result<(Vec<SpoolIndex>, Vec<(SpoolIndex, String)>), EpochError> {
        // Get system state with committee and spool assignments
        let system = self
            .client
            .get_system()
            .await
            .map_err(|e| EpochError::SpoolChanges(format!("Failed to fetch system: {}", e)))?;

        // Get our node to find our NodeId
        let node = self
            .client
            .get_node(&self.authority)
            .await
            .map_err(|e| EpochError::SpoolChanges(format!("Failed to fetch node: {}", e)))?;

        let our_node_id = node.id;

        // Find our index in the previous and current committees
        let prev_index = system.committee_prev.index_of(&our_node_id);
        let curr_index = system.committee.index_of(&our_node_id);

        debug!(
            node_id = ?our_node_id,
            prev_index = ?prev_index,
            curr_index = ?curr_index,
            "Found node in committees"
        );

        // Get spools we owned before
        let prev_spools: Vec<SpoolIndex> = prev_index
            .map(|idx| system.spools_prev.spools_for_member(idx))
            .unwrap_or_default();

        // Get spools we own now
        let curr_spools: Vec<SpoolIndex> = curr_index
            .map(|idx| system.spools.spools_for_member(idx))
            .unwrap_or_default();

        // Find released spools (we had them, now we don't)
        let released_spools: Vec<SpoolIndex> = prev_spools
            .iter()
            .filter(|s| !curr_spools.contains(s))
            .copied()
            .collect();

        // Find new spools (we didn't have them, now we do)
        let new_spool_indices: Vec<SpoolIndex> = curr_spools
            .iter()
            .filter(|s| !prev_spools.contains(s))
            .copied()
            .collect();

        // For new spools, find the previous owner's network address
        let mut new_spools: Vec<(SpoolIndex, String)> = Vec::new();

        for spool_idx in new_spool_indices {
            // Find who owned this spool before
            let prev_owner_member_idx = system.spools_prev.0[spool_idx as usize] as usize;

            // Get the previous owner's NodeId from the previous committee
            if let Some(prev_member) = system.committee_prev.member_at(prev_owner_member_idx) {
                // Look up the previous owner's node to get their network address
                match self.client.get_node_by_id(prev_member.id).await {
                    Ok((_pubkey, prev_node)) => {
                        // Convert NetworkAddress to string via SocketAddr
                        let addr = match prev_node.metadata.network_address.to_socket_addr() {
                            Ok(sa) => sa.to_string(),
                            Err(e) => {
                                warn!(
                                    spool = spool_idx,
                                    prev_owner = ?prev_member.id,
                                    error = %e,
                                    "Invalid network address for previous owner"
                                );
                                continue;
                            }
                        };
                        debug!(
                            spool = spool_idx,
                            prev_owner = ?prev_member.id,
                            addr = %addr,
                            "Found previous owner for spool"
                        );
                        new_spools.push((spool_idx, addr));
                    }
                    Err(e) => {
                        warn!(
                            spool = spool_idx,
                            prev_owner = ?prev_member.id,
                            error = %e,
                            "Failed to get previous owner node, skipping spool"
                        );
                    }
                }
            } else {
                // No previous owner (new spool allocation), skip syncing
                debug!(spool = spool_idx, "Spool has no previous owner");
            }
        }

        info!(
            released = released_spools.len(),
            new = new_spools.len(),
            "Computed spool changes"
        );

        Ok((released_spools, new_spools))
    }

    /// Get the list of spools currently assigned to this node.
    async fn get_assigned_spools(&self) -> Result<Vec<SpoolIndex>, EpochError> {
        let system = self
            .client
            .get_system()
            .await
            .map_err(|e| EpochError::FetchEpoch(format!("Failed to fetch system: {}", e)))?;

        let node = self
            .client
            .get_node(&self.authority)
            .await
            .map_err(|e| EpochError::FetchEpoch(format!("Failed to fetch node: {}", e)))?;

        let our_index = system.committee.index_of(&node.id);

        let spools = our_index
            .map(|idx| system.spools.spools_for_member(idx))
            .unwrap_or_default();

        Ok(spools)
    }

    /// Submit a sync completion proof to the chain.
    async fn submit_sync_completion(
        &self,
        epoch: EpochNumber,
        synced_spools: &[SpoolIndex],
    ) -> Result<(), EpochError> {
        let (node_address, _) = node_pda(self.authority);

        // Build the sync instruction
        let ix = build_epoch_sync_ix(
            self.authority,
            node_address,
            epoch,
            synced_spools,
        );

        info!(
            epoch = epoch.as_u64(),
            spools = synced_spools.len(),
            "Submitting sync completion to chain"
        );

        // Submit the transaction
        self.client
            .send_instructions(&self.authority_keypair, vec![ix])
            .await
            .map_err(|e| EpochError::SubmitSyncDone(format!("Failed to submit sync: {}", e)))?;

        info!(epoch = epoch.as_u64(), "Sync completion submitted successfully");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full tests would require mocking TapeClient
    // These are basic structural tests

    #[test]
    fn test_default_poll_interval() {
        assert_eq!(DEFAULT_POLL_INTERVAL, Duration::from_secs(10));
    }
}
