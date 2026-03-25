use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::join_all;
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use solana_sdk::pubkey::Pubkey;
use tape_core::erasure::SPOOL_COUNT;
use tape_core::system::EpochPhase;
use tape_core::types::SlotNumber;
use tape_protocol::api::NodeStats;
use tape_protocol::ProtocolState;

use crate::view::{ClusterView, NodeView, ProdnetView, SpoolView};

/// Lightweight node reference — copied under lock, used outside it.
#[derive(Clone)]
pub struct NodeRef {
    pub id: usize,
    pub port: u16,
    pub authority: Pubkey,
}

/// Internal chain state model — not exposed via API.
struct ObservedChainState {
    protocol: ProtocolState,
    slot: SlotNumber,
    phase_weight: Option<u64>,
    total_nodes_registered: u64,
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

    /// Single-pass chain state fetch. Builds ProtocolState inline from the
    /// same epoch/system values used for phase_weight and total_nodes,
    /// avoiding the double-fetch that `fetch_state()` would cause.
    async fn chain_state(&self) -> Result<ObservedChainState> {
        let slot_raw = self.rpc.get_slot().await.context("get_slot")?;
        let epoch = self.rpc.get_epoch().await.context("get_epoch")?;
        let system = self.rpc.get_system().await.context("get_system")?;

        let phase = EpochPhase::try_from(epoch.state.phase).unwrap_or(EpochPhase::Unknown);
        let phase_weight = epoch.state.weight();
        let total_nodes_registered = system.total_nodes;

        let protocol = ProtocolState {
            epoch: epoch.id,
            phase,
            last_epoch: epoch.last_epoch,
            nonce: epoch.nonce,
            committee: system.committee.iter().cloned().collect(),
            committee_prev: system.committee_prev.iter().cloned().collect(),
            committee_next: system.committee_next.iter().cloned().collect(),
            spools: system.spools,
            spools_prev: system.spools_prev,
        };

        Ok(ObservedChainState {
            protocol,
            slot: SlotNumber(slot_raw),
            phase_weight,
            total_nodes_registered,
        })
    }

    /// Scrape a single node concurrently: health, stats, and metrics
    /// fire in parallel so a dead node costs one timeout (~2s), not three.
    async fn scrape_node(&self, port: u16) -> NodeScrape {
        let base = format!("http://127.0.0.1:{port}");

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
        let scrape_fut = self.scrape_node(node.port);
        let chain_fut = self.rpc.get_node(&node.authority);
        let (scrape, onchain) = tokio::join!(scrape_fut, chain_fut);

        let (node_id, pool_stake) = match onchain {
            Ok(onchain_node) => {
                let address = onchain_node
                    .metadata
                    .network_address
                    .to_socket_addr()
                    .ok()
                    .map(|addr| addr.to_string());

                return NodeView {
                    local_id: node.id,
                    node_id: Some(onchain_node.id.0),
                    authority: node.authority.to_string(),
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
            address: None,
            healthy: scrape.healthy,
            metrics_available: scrape.metrics_available,
            pool_stake,
            stats: scrape.stats,
        }
    }

    pub async fn snapshot(&self, nodes: Vec<NodeRef>) -> Result<ProdnetView> {
        let chain = self.chain_state().await?;
        let mut node_views = join_all(nodes.iter().map(|node| self.observe_node(node))).await;
        node_views.sort_by_key(|node| node.local_id);

        let node_lookup = node_views
            .iter()
            .filter_map(|node| node.node_id.map(|node_id| (node_id, node.local_id)))
            .collect::<std::collections::HashMap<_, _>>();

        let spools = (0..SPOOL_COUNT)
            .map(|spool| {
                let owner_node_id = chain.protocol.spool_owner(spool as _).map(|node_id| node_id.0);
                let owner_local_id =
                    owner_node_id.and_then(|node_id| node_lookup.get(&node_id).copied());

                SpoolView {
                    spool: spool as u16,
                    owner_node_id,
                    owner_local_id,
                }
            })
            .collect();

        Ok(ProdnetView {
            cluster: ClusterView {
                epoch: chain.protocol.epoch.0,
                phase: match chain.protocol.phase {
                    EpochPhase::Syncing => "Syncing",
                    EpochPhase::Settling => "Settling",
                    EpochPhase::Active => "Active",
                    _ => "Unknown",
                }
                .to_string(),
                phase_weight: chain.phase_weight,
                slot: chain.slot.0,
                committee_prev_size: chain.protocol.committee_prev.len(),
                committee_size: chain.protocol.committee.len(),
                committee_next_size: chain.protocol.committee_next.len(),
                total_nodes_registered: chain.total_nodes_registered,
            },
            nodes: node_views,
            spools,
            uploads: Vec::new(),
        })
    }
}
