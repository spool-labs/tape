//! Data fetching from Solana RPC and storage nodes.
//!
//! The [`DataFetcher`] provides async methods for:
//! - Fetching on-chain state (System, Epoch, Archive, Nodes)
//! - Performing health checks against storage nodes
//! - Parallel health checking for all committee members

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::future::join_all;
use rpc_client::{RpcClient, RpcConfig, SolanaRpc};
use solana_sdk::pubkey::Pubkey;
use tape_api::state::{Archive, Epoch, Node, System};

use super::{HealthStatus, NodeState};

/// Default timeout for node health checks in milliseconds.
const DEFAULT_HEALTH_TIMEOUT_MS: u64 = 5000;

/// Data fetcher for Tapedrive network state.
///
/// Wraps an RPC client and provides convenience methods for fetching
/// on-chain state and performing node health checks.
pub struct DataFetcher {
    /// The RPC client for Solana queries.
    rpc_client: RpcClient<SolanaRpc>,
    /// HTTP client for node health checks.
    http_client: reqwest::Client,
    /// Timeout for health checks.
    health_timeout: Duration,
}

impl DataFetcher {
    /// Create a new DataFetcher with the given RPC URL.
    ///
    /// # Arguments
    /// * `rpc_url` - The Solana RPC endpoint URL
    ///
    /// # Errors
    /// Returns an error if the RPC client cannot be initialized.
    pub fn new(rpc_url: &str) -> Result<Self> {
        let config = RpcConfig {
            endpoints: vec![rpc_url.to_string()],
            ..Default::default()
        };

        let rpc_client = RpcClient::new(config)
            .context("Failed to create RPC client")?;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(DEFAULT_HEALTH_TIMEOUT_MS))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            rpc_client,
            http_client,
            health_timeout: Duration::from_millis(DEFAULT_HEALTH_TIMEOUT_MS),
        })
    }

    /// Create a new DataFetcher with custom configuration.
    ///
    /// # Arguments
    /// * `rpc_config` - RPC client configuration
    /// * `health_timeout_ms` - Timeout for health checks in milliseconds
    pub fn with_config(rpc_config: RpcConfig, health_timeout_ms: u64) -> Result<Self> {
        let rpc_client = RpcClient::new(rpc_config)
            .context("Failed to create RPC client")?;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(health_timeout_ms))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            rpc_client,
            http_client,
            health_timeout: Duration::from_millis(health_timeout_ms),
        })
    }

    /// Set the health check timeout.
    pub fn set_health_timeout(&mut self, timeout_ms: u64) {
        self.health_timeout = Duration::from_millis(timeout_ms);
    }

    // ========================================================================
    // On-Chain State Fetching
    // ========================================================================

    /// Fetch the System singleton account.
    ///
    /// Contains committee information and spool assignments.
    pub async fn fetch_system(&self) -> Result<System> {
        self.rpc_client
            .get_system()
            .await
            .context("Failed to fetch System account")
    }

    /// Fetch the Epoch singleton account.
    ///
    /// Contains current epoch number, phase, and timing information.
    pub async fn fetch_epoch(&self) -> Result<Epoch> {
        self.rpc_client
            .get_epoch()
            .await
            .context("Failed to fetch Epoch account")
    }

    /// Fetch the Archive singleton account.
    ///
    /// Contains storage capacity, pricing, and reward information.
    pub async fn fetch_archive(&self) -> Result<Archive> {
        self.rpc_client
            .get_archive()
            .await
            .context("Failed to fetch Archive account")
    }

    /// Fetch all registered Node accounts.
    ///
    /// **Warning:** This is an expensive operation on mainnet.
    /// Returns a vector of (account address, Node data) tuples.
    pub async fn fetch_all_nodes(&self) -> Result<Vec<(Pubkey, Node)>> {
        self.rpc_client
            .get_all_nodes()
            .await
            .context("Failed to fetch all Node accounts")
    }

    /// Fetch the current Solana slot.
    pub async fn fetch_slot(&self) -> Result<u64> {
        self.rpc_client
            .get_slot()
            .await
            .context("Failed to fetch current slot")
    }

    /// Fetch all tapes and compute aggregate statistics.
    ///
    /// Returns (tape_count, track_count, active_tape_count) where:
    /// - tape_count: total number of tapes
    /// - track_count: sum of track_count across all tapes
    /// - active_tape_count: tapes where expiry_epoch > current_epoch
    pub async fn fetch_tape_stats(&self, current_epoch: tape_core::types::EpochNumber) -> Result<(u64, u64, u64)> {
        let tapes = self.rpc_client
            .get_all_tapes()
            .await
            .context("Failed to fetch all Tape accounts")?;

        let tape_count = tapes.len() as u64;
        let mut track_count = 0u64;
        let mut active_count = 0u64;

        for (_pubkey, tape) in &tapes {
            track_count += tape.track_count;
            if tape.expiry_epoch > current_epoch {
                active_count += 1;
            }
        }

        Ok((tape_count, track_count, active_count))
    }

    // ========================================================================
    // Node Health Checks
    // ========================================================================

    /// Check the health of a single storage node.
    ///
    /// Performs an HTTP GET to the node's `/v1/health` endpoint and measures
    /// the response latency.
    ///
    /// # Arguments
    /// * `address` - The network address of the node (e.g., "192.168.1.1:8080")
    ///
    /// # Returns
    /// A tuple of (health status, optional latency in milliseconds).
    /// Returns (Offline, None) if the request fails or times out.
    pub async fn fetch_node_health(&self, address: &str) -> (HealthStatus, Option<u32>) {
        let url = format!("http://{}/v1/health", address);
        let start = Instant::now();

        match self.http_client.get(&url).send().await {
            Ok(response) => {
                let latency = start.elapsed().as_millis() as u32;

                if response.status().is_success() {
                    // Try to parse the response body to check for syncing state
                    match response.json::<serde_json::Value>().await {
                        Ok(json) => {
                            // Check if the node reports it's syncing
                            let syncing = json
                                .get("syncing")
                                .and_then(serde_json::Value::as_bool)
                                .unwrap_or(false);

                            if syncing {
                                (HealthStatus::Syncing, Some(latency))
                            } else {
                                (HealthStatus::Online, Some(latency))
                            }
                        }
                        Err(_) => {
                            // Response was successful but couldn't parse JSON
                            // Assume online since we got a 200
                            (HealthStatus::Online, Some(latency))
                        }
                    }
                } else {
                    // Non-2xx response means node is having issues
                    (HealthStatus::Offline, None)
                }
            }
            Err(_) => {
                // Request failed (timeout, connection refused, etc.)
                (HealthStatus::Offline, None)
            }
        }
    }

    /// Check the health of all provided nodes in parallel.
    ///
    /// # Arguments
    /// * `nodes` - Slice of (account address, Node data) tuples
    /// * `timeout_ms` - Timeout for each health check in milliseconds
    ///
    /// # Returns
    /// A vector of [`NodeState`] with updated health information.
    pub async fn fetch_all_node_health(
        &self,
        nodes: &[(Pubkey, Node)],
        timeout_ms: u64,
    ) -> Vec<NodeState> {
        // Create a client with the specified timeout for this batch
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
        {
            Ok(c) => c,
            Err(_) => {
                // If we can't create a client, return all nodes with Unknown status
                return nodes
                    .iter()
                    .map(|(addr, node)| NodeState::new(*addr, *node))
                    .collect();
            }
        };

        // Create futures for all health checks
        let health_futures: Vec<_> = nodes
            .iter()
            .map(|(address, node)| {
                let client = client.clone();
                let node = *node;
                let address = *address;

                async move {
                    let mut state = NodeState::new(address, node);

                    // Get the network address from the node metadata
                    let network_addr = match node.metadata.network_address.to_socket_addr() {
                        Ok(addr) => addr.to_string(),
                        Err(_) => {
                            // Can't determine network address, leave as Unknown
                            return state;
                        }
                    };

                    let url = format!("http://{}/v1/health", network_addr);
                    let start = Instant::now();

                    match client.get(&url).send().await {
                        Ok(response) => {
                            let latency = start.elapsed().as_millis() as u32;
                            state.last_check = Instant::now();

                            if response.status().is_success() {
                                match response.json::<serde_json::Value>().await {
                                    Ok(json) => {
                                        let syncing = json
                                            .get("syncing")
                                            .and_then(serde_json::Value::as_bool)
                                            .unwrap_or(false);

                                        if syncing {
                                            state.health = HealthStatus::Syncing;
                                        } else {
                                            state.health = HealthStatus::Online;
                                        }
                                        state.latency_ms = Some(latency);
                                    }
                                    Err(_) => {
                                        state.health = HealthStatus::Online;
                                        state.latency_ms = Some(latency);
                                    }
                                }

                                // If node is online, also fetch stats
                                if state.health != HealthStatus::Offline {
                                    let stats_url = format!("http://{}/v1/stats", network_addr);
                                    if let Ok(stats_resp) = client.get(&stats_url).send().await {
                                        if stats_resp.status().is_success() {
                                            if let Ok(stats) = stats_resp.json::<tape_node_api::NodeStats>().await {
                                                state.stats = Some(stats);
                                            }
                                        }
                                    }
                                }
                            } else {
                                state.health = HealthStatus::Offline;
                            }
                        }
                        Err(_) => {
                            state.health = HealthStatus::Offline;
                            state.last_check = Instant::now();
                        }
                    }

                    state
                }
            })
            .collect();

        // Execute all health checks in parallel
        join_all(health_futures).await
    }

    /// Fetch all data needed for the monitor dashboard.
    ///
    /// This performs all fetches in parallel for efficiency.
    ///
    /// # Returns
    /// A tuple of (System, Epoch, Archive, Vec<NodeState>).
    pub async fn fetch_all_dashboard_data(
        &self,
        health_timeout_ms: u64,
    ) -> Result<(System, Epoch, Archive, Vec<NodeState>)> {
        // Fetch on-chain state in parallel
        let (system_result, epoch_result, archive_result, nodes_result) = tokio::join!(
            self.fetch_system(),
            self.fetch_epoch(),
            self.fetch_archive(),
            self.fetch_all_nodes(),
        );

        let system = system_result?;
        let epoch = epoch_result?;
        let archive = archive_result?;
        let nodes = nodes_result?;

        // Now fetch health for all nodes in parallel
        let node_states = self.fetch_all_node_health(&nodes, health_timeout_ms).await;

        Ok((system, epoch, archive, node_states))
    }

    /// Fetch dashboard data with graceful handling of missing/partial accounts.
    ///
    /// This is useful during system initialization when accounts may not exist yet
    /// or may be partially created. Returns None for components that couldn't be fetched.
    ///
    /// # Returns
    /// A tuple of (Option<System>, Option<Epoch>, Option<Archive>, Vec<NodeState>, TapeStats, u64, Vec<String>).
    /// The u64 is the current slot number.
    pub async fn fetch_dashboard_data_graceful(
        &self,
        health_timeout_ms: u64,
    ) -> (Option<System>, Option<Epoch>, Option<Archive>, Vec<NodeState>, TapeStats, u64, Vec<String>) {
        let mut errors = Vec::new();

        // Fetch on-chain state in parallel (including slot)
        let (system_result, epoch_result, archive_result, nodes_result, tapes_result, slot_result) = tokio::join!(
            self.fetch_system(),
            self.fetch_epoch(),
            self.fetch_archive(),
            self.fetch_all_nodes(),
            self.rpc_client.get_all_tapes(),
            self.fetch_slot(),
        );

        // Slot is best-effort, default to 0 if unavailable
        let current_slot = slot_result.unwrap_or(0);

        let system = match system_result {
            Ok(s) => Some(s),
            Err(e) => {
                errors.push(format!("System: {}", e));
                None
            }
        };

        let epoch = match epoch_result {
            Ok(e) => Some(e),
            Err(e) => {
                errors.push(format!("Epoch: {}", e));
                None
            }
        };

        let archive = match archive_result {
            Ok(a) => Some(a),
            Err(e) => {
                errors.push(format!("Archive: {}", e));
                None
            }
        };

        let nodes = match nodes_result {
            Ok(n) => n,
            Err(e) => {
                errors.push(format!("Nodes: {}", e));
                Vec::new()
            }
        };

        // Compute tape stats from tapes
        let tape_stats = match tapes_result {
            Ok(tapes) => {
                let current_epoch = epoch.as_ref().map(|e| e.id).unwrap_or(tape_core::types::EpochNumber(0));
                let total_tapes = tapes.len() as u64;
                let mut track_count = 0u64;
                let mut active_tapes = 0u64;

                for (_pubkey, tape) in &tapes {
                    track_count += tape.track_count;
                    if tape.expiry_epoch > current_epoch {
                        active_tapes += 1;
                    }
                }

                TapeStats {
                    total_tapes,
                    active_tapes,
                    track_count,
                }
            }
            Err(e) => {
                errors.push(format!("Tapes: {}", e));
                TapeStats::default()
            }
        };

        // Only fetch health if we have nodes
        let node_states = if !nodes.is_empty() {
            self.fetch_all_node_health(&nodes, health_timeout_ms).await
        } else {
            Vec::new()
        };

        (system, epoch, archive, node_states, tape_stats, current_slot, errors)
    }
}

/// Aggregated tape and track statistics.
#[derive(Debug, Clone, Default)]
pub struct TapeStats {
    /// Total number of tapes.
    pub total_tapes: u64,
    /// Number of active (non-expired) tapes.
    pub active_tapes: u64,
    /// Total number of tracks across all tapes.
    pub track_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_display() {
        assert_eq!(HealthStatus::Online.symbol(), "●");
        assert_eq!(HealthStatus::Offline.symbol(), "○");
        assert_eq!(HealthStatus::Syncing.symbol(), "◐");
        assert_eq!(HealthStatus::Unknown.symbol(), "◌");

        assert!(HealthStatus::Online.is_healthy());
        assert!(HealthStatus::Syncing.is_healthy());
        assert!(!HealthStatus::Offline.is_healthy());
        assert!(!HealthStatus::Unknown.is_healthy());
    }
}
