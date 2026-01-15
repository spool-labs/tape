//! RPC client helpers for e2e testing.
//!
//! Provides wrappers for using the rpc-client library to fetch
//! blockchain state in e2e tests. This allows tests to use CLI
//! for mutations while using rpc-client for state verification.

use std::time::Duration;

use anyhow::{Context as _, Result};
use rpc_client::{RpcClient, RpcConfig};
use rpc_solana::SolanaRpc;
use solana_sdk::pubkey::Pubkey;

use tape_api::prelude::{Epoch, Node, System};
use tape_core::types::EpochNumber;

/// Create an RPC client connected to the specified URL.
///
/// # Arguments
/// * `rpc_url` - The Solana RPC URL (e.g., "http://127.0.0.1:8899")
///
/// # Example
/// ```ignore
/// let rpc = create_rpc_client("http://127.0.0.1:8899").await?;
/// let system = rpc.get_system().await?;
/// ```
pub async fn create_rpc_client(rpc_url: &str) -> Result<RpcClient<SolanaRpc>> {
    let config = RpcConfig {
        endpoints: vec![rpc_url.to_string()],
        ..Default::default()
    };
    let solana_rpc = SolanaRpc::new(config).context("Failed to create SolanaRpc")?;
    Ok(RpcClient::from_rpc(solana_rpc))
}

/// Wrapper around RpcClient with convenience methods for e2e tests.
pub struct E2eRpcClient {
    inner: RpcClient<SolanaRpc>,
}

impl E2eRpcClient {
    /// Create a new E2eRpcClient connected to the specified URL.
    pub async fn new(rpc_url: &str) -> Result<Self> {
        let inner = create_rpc_client(rpc_url).await?;
        Ok(Self { inner })
    }

    /// Get the inner RpcClient for direct access.
    pub fn inner(&self) -> &RpcClient<SolanaRpc> {
        &self.inner
    }

    /// Get the System account state.
    pub async fn get_system(&self) -> Result<System> {
        self.inner
            .get_system()
            .await
            .context("Failed to get System account")
    }

    /// Get the Epoch account state.
    pub async fn get_epoch(&self) -> Result<Epoch> {
        self.inner
            .get_epoch()
            .await
            .context("Failed to get Epoch account")
    }

    /// Get a Node account by authority pubkey.
    pub async fn get_node(&self, authority: &Pubkey) -> Result<Node> {
        self.inner
            .get_node(authority)
            .await
            .context("Failed to get Node account")
    }

    /// Get the current epoch ID.
    pub async fn get_epoch_id(&self) -> Result<EpochNumber> {
        let epoch = self.get_epoch().await?;
        Ok(epoch.id)
    }

    /// Get the current epoch phase as a string.
    pub async fn get_epoch_phase(&self) -> Result<String> {
        let epoch = self.get_epoch().await?;
        let phase = if epoch.state.is_syncing() {
            "Syncing"
        } else if epoch.state.is_settling() {
            "Settling"
        } else if epoch.state.is_active() {
            "Active"
        } else {
            "Unknown"
        };
        Ok(phase.to_string())
    }

    /// Get the current committee size.
    pub async fn get_committee_size(&self) -> Result<usize> {
        let system = self.get_system().await?;
        Ok(system.committee.size())
    }

    /// Get the committee_next size.
    pub async fn get_committee_next_size(&self) -> Result<usize> {
        let system = self.get_system().await?;
        Ok(system.committee_next.size())
    }

    /// Check if system is in bootstrap mode (committee_prev empty).
    pub async fn is_bootstrap_mode(&self) -> Result<bool> {
        let system = self.get_system().await?;
        Ok(system.committee_prev_empty())
    }

    /// Check if AdvanceEpoch would be blocked due to insufficient committee_next.
    pub async fn would_block_advance(&self) -> Result<bool> {
        let system = self.get_system().await?;
        Ok(system.will_be_low_quorum())
    }
}

/// Wait for epoch phase using RPC client.
pub async fn wait_for_epoch_phase_rpc(
    rpc: &E2eRpcClient,
    phase: &str,
    timeout: Duration,
) -> Result<()> {
    use crate::wait::wait_for_with_desc;

    wait_for_with_desc(
        &format!("epoch phase = {}", phase),
        || async {
            match rpc.get_epoch_phase().await {
                Ok(current_phase) => Ok(current_phase == phase),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for epoch ID to reach a specific value using RPC client.
pub async fn wait_for_epoch_id_rpc(
    rpc: &E2eRpcClient,
    epoch_id: EpochNumber,
    timeout: Duration,
) -> Result<()> {
    use crate::wait::wait_for_with_desc;

    wait_for_with_desc(
        &format!("epoch id = {}", epoch_id.as_u64()),
        || async {
            match rpc.get_epoch_id().await {
                Ok(current_id) => Ok(current_id >= epoch_id),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for committee size to reach a minimum value using RPC client.
pub async fn wait_for_committee_size_rpc(
    rpc: &E2eRpcClient,
    min_size: usize,
    timeout: Duration,
) -> Result<()> {
    use crate::wait::wait_for_with_desc;

    wait_for_with_desc(
        &format!("committee size >= {}", min_size),
        || async {
            match rpc.get_committee_size().await {
                Ok(size) => Ok(size >= min_size),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for committee_next to reach a minimum value using RPC client.
pub async fn wait_for_committee_next_size_rpc(
    rpc: &E2eRpcClient,
    min_size: usize,
    timeout: Duration,
) -> Result<()> {
    use crate::wait::wait_for_with_desc;

    wait_for_with_desc(
        &format!("committee_next size >= {}", min_size),
        || async {
            match rpc.get_committee_next_size().await {
                Ok(size) => Ok(size >= min_size),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Debug print current state from RPC.
pub async fn debug_rpc_state(rpc: &E2eRpcClient, label: &str) {
    println!("\n[{}]", label);

    match rpc.get_epoch().await {
        Ok(epoch) => {
            let phase = if epoch.state.is_syncing() {
                "Syncing"
            } else if epoch.state.is_settling() {
                "Settling"
            } else if epoch.state.is_active() {
                "Active"
            } else {
                "Unknown"
            };
            println!(
                "  Epoch: {} | Phase: {} | Weight: {}",
                epoch.id.as_u64(),
                phase,
                epoch.state.weight
            );
        }
        Err(e) => {
            println!("  Epoch: ERROR - {}", e);
        }
    }

    match rpc.get_system().await {
        Ok(system) => {
            println!(
                "  Committees: prev={} curr={} next={}",
                system.committee_prev.size(),
                system.committee.size(),
                system.committee_next.size()
            );
            println!(
                "  Bootstrap: {} | Would block: {}",
                system.committee_prev_empty(),
                system.will_be_low_quorum()
            );
        }
        Err(e) => {
            println!("  System: ERROR - {}", e);
        }
    }
    println!();
}

/// Debug print node state from RPC.
pub async fn debug_node_state(rpc: &E2eRpcClient, authority: &Pubkey, label: &str) {
    match rpc.get_node(authority).await {
        Ok(node) => {
            println!(
                "[{}] Node {}: stake={} sync_epoch={} advance_epoch={}",
                label,
                node.id.as_u64(),
                node.pool.stake.as_u64(),
                node.latest_sync_epoch.as_u64(),
                node.latest_advance_epoch.as_u64()
            );
        }
        Err(e) => {
            println!("[{}] Node ERROR: {}", label, e);
        }
    }
}
