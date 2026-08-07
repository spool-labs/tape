use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::join_all;
use rpc::RpcError;
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use solana_pubkey::Pubkey;
use tape_api::program::tapedrive::node_pda;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_crypto::address::Address;
use tape_protocol::api::NodeStats;

use crate::view::{ClusterView, NodeView, SpoolView, LocalnetView};

/// Lightweight node reference, copied under lock, used outside it.
#[derive(Clone)]
pub struct NodeRef {
    pub id: usize,
    pub port: u16,
    pub plaintext_port: u16,
    pub authority: Pubkey,
}

/// Internal chain state model, not exposed via API.
struct ObservedChainState {
    slot: SlotNumber,
    epoch: EpochNumber,
    phase: EpochPhase,
    phase_weight: Option<u64>,
    live_group_count: u64,
    committee_prev_size: usize,
    committee_size: usize,
    committee_next_size: usize,
    total_nodes_registered: u64,
    spool_owners: Vec<Option<Address>>,
}

/// Per-node scrape result.
struct NodeScrape {
    healthy: bool,
    stats: Option<NodeStats>,
    metrics_available: bool,
}

pub struct Observer {
    rpc: RpcClient<SolanaRpc>,
    http: reqwest::Client,
}

impl Observer {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let rpc = RpcClient::new(RpcConfig {
            endpoints: vec![rpc_url.to_string()],
            ..Default::default()
        })
        .context("create observer rpc client")?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("create http client")?;

        Ok(Self { rpc, http })
    }

    async fn chain_state(&self) -> Result<ObservedChainState> {
        let slot_raw = self.rpc.get_slot().await.context("get_slot")?;
        let system = self.rpc.get_system().await.context("get_system")?;
        let epoch = self
            .rpc
            .get_epoch(system.current_epoch)
            .await
            .context("get_epoch")?;

        let phase = epoch.state.phase().unwrap_or(EpochPhase::Unknown);
        let phase_weight = match phase {
            EpochPhase::Sync => Some(epoch.state.synced_count),
            _ => None,
        };
        let total_nodes_registered = system.total_nodes;
        let live_group_count = system.live_group_count;
        let committee_prev_size = self
            .committee_size(system.current_epoch.prev())
            .await
            .context("get previous committee")?;
        let committee_size = self
            .committee_size(system.current_epoch)
            .await
            .context("get current committee")?;
        let committee_next_size = self
            .committee_size(system.current_epoch.next())
            .await
            .context("get next committee")?;

        let total_spools = usize::try_from(live_group_count)
            .ok()
            .and_then(|groups| groups.checked_mul(GROUP_SIZE))
            .unwrap_or(0);
        let mut spool_owners = vec![None; total_spools];
        if live_group_count > 0 {
            let groups = self
                .rpc
                .get_groups(system.current_epoch, live_group_count)
                .await
                .context("get_groups")?;
            for group in groups {
                for (position, spool) in group.spools.iter().enumerate() {
                    let spool_index = group.id.spool_at(position).as_usize();
                    if let Some(owner) = spool_owners.get_mut(spool_index) {
                        if *spool.node.as_bytes() != [0u8; 32] {
                            *owner = Some(spool.node);
                        }
                    }
                }
            }
        }

        Ok(ObservedChainState {
            slot: SlotNumber(slot_raw),
            epoch: epoch.id,
            phase,
            phase_weight,
            live_group_count,
            committee_prev_size,
            committee_size,
            committee_next_size,
            total_nodes_registered,
            spool_owners,
        })
    }

    async fn committee_size(&self, epoch: EpochNumber) -> Result<usize> {
        match self.rpc.get_committee(epoch).await {
            Ok(members) => Ok(members.len()),
            Err(RpcError::AccountNotFound(_)) => Ok(0),
            Err(error) => Err(error).with_context(|| format!("get_committee({})", epoch.0)),
        }
    }

    /// Scrape a single node concurrently: health, stats, and metrics
    /// fire in parallel so a dead node costs one timeout (~2s), not three.
    ///
    /// Uses the node's loopback plain-HTTP listener so we don't have to do
    /// a TLS handshake + pinning dance for every scrape tick. The main TLS
    /// port is for peer traffic; the loopback port is for local ops tooling.
    async fn scrape_node(&self, port: u16) -> NodeScrape {
        let base = format!("http://{}:{port}", crate::process::LOCAL_HOST);

        let health_fut = self.http.get(format!("{base}/v1/health")).send();
        let stats_fut = self.http.get(format!("{base}/v1/stats")).send();
        let metrics_fut = self.http.get(format!("{base}/v1/metrics")).send();

        let (health_res, stats_res, metrics_res) =
            tokio::join!(health_fut, stats_fut, metrics_fut);

        let healthy = health_res
            .map(|r| r.status().is_success())
            .unwrap_or(false);

        let stats = match stats_res
            .ok()
            .filter(|r| r.status().is_success())
        {
            Some(r) => r.json::<NodeStats>().await.ok(),
            None => None,
        };

        let metrics_available = metrics_res
            .ok()
            .filter(|r| r.status().is_success())
            .is_some();

        NodeScrape {
            healthy,
            stats,
            metrics_available,
        }
    }

    async fn observe_node(&self, node: &NodeRef) -> NodeView {
        let scrape_fut = self.scrape_node(node.plaintext_port);
        let authority = Address::from(node.authority);
        let chain_fut = self.rpc.get_node(&authority);
        let (scrape, onchain) = tokio::join!(scrape_fut, chain_fut);
        let node_address = node_pda(authority).0.to_string();

        let (node_id, pool_stake) = match onchain {
            Ok(onchain_node) => {
                let address = onchain_node
                    .metadata
                    .network_address
                    .authority()
                    .ok();

                return NodeView {
                    local_id: node.id,
                    node_id: Some(onchain_node.id.0),
                    authority: node.authority.to_string(),
                    node_address,
                    address,
                    healthy: scrape.healthy,
                    metrics_available: scrape.metrics_available,
                    pool_stake: Some(onchain_node.pool.stake.as_u64()),
                    stats: scrape.stats,
                };
            }
            Err(_) => (None, None),
        };

        NodeView {
            local_id: node.id,
            node_id,
            authority: node.authority.to_string(),
            node_address,
            address: None,
            healthy: scrape.healthy,
            metrics_available: scrape.metrics_available,
            pool_stake,
            stats: scrape.stats,
        }
    }

    pub async fn snapshot(&self, nodes: Vec<NodeRef>) -> Result<LocalnetView> {
        let chain = self.chain_state().await?;
        let mut node_views = join_all(nodes.iter().map(|node| self.observe_node(node))).await;
        node_views.sort_by_key(|node| node.local_id);

        let node_lookup = nodes
            .iter()
            .map(|node| {
                let authority = Address::from(node.authority);
                let (node_address, _) = node_pda(authority);
                (node_address, node.id)
            })
            .collect::<HashMap<_, _>>();

        let spools = chain
            .spool_owners
            .iter()
            .enumerate()
            .map(|(spool, owner)| {
                let owner_local_id = owner.and_then(|node| node_lookup.get(&node).copied());

                SpoolView {
                    spool: spool as u64,
                    owner_node: owner.map(|node| node.to_string()),
                    owner_local_id,
                }
            })
            .collect();

        let phase_index = u64::from(chain.phase) as u8;
        Ok(LocalnetView {
            cluster: ClusterView {
                epoch: chain.epoch.0,
                phase: tape_observe_api::phase_name(phase_index).to_string(),
                phase_index,
                phase_weight: chain.phase_weight,
                slot: chain.slot.0,
                live_group_count: chain.live_group_count,
                committee_prev_size: chain.committee_prev_size,
                committee_size: chain.committee_size,
                committee_next_size: chain.committee_next_size,
                total_nodes_registered: chain.total_nodes_registered,
            },
            nodes: node_views,
            spools,
            uploads: Vec::new(),
        })
    }
}
