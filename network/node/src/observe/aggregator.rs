//! Background service that probes registered nodes and records their liveness
//! for the dashboard's network table. Opt-in, since it costs outbound requests
//! each interval.
//!
//! Each node is reached two ways: its full observe board over the mTLS peer
//! link, and its always-on public stats endpoint. A node answering either is
//! up; one that answers neither is down.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use peer_manager::PeerNode;
use rpc::Rpc;
use store::Store;
use tape_observe_api::{LinkStatus, NodeStats, StatsSource};
use tape_protocol::api::GetStatsReq;
use tape_protocol::Api;
use tokio::select;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::peers;
use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Public HTTP monitoring port is the advertised peer port minus this offset.
const PUBLIC_PORT_OFFSET: u16 = 10;
/// Don't refetch a node's public stats more often than this.
const PUBLIC_TTL: Duration = Duration::from_secs(60);
/// Re-attempt the mTLS path this often once a peer has refused it.
const MTLS_RECHECK: Duration = Duration::from_secs(3600);

/// Re-attempt this soon when the probe never got an answer.
const MTLS_RETRY: Duration = Duration::from_secs(30);

pub struct PeerAggregator<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    interval: Duration,
    cancel: CancellationToken,
    http: reqwest::Client,
    cache: Mutex<HashMap<tape_crypto::Address, ProbeState>>,
}

/// Per-node probe bookkeeping so the two paths refresh on their own cadences.
#[derive(Clone, Default)]
struct ProbeState {
    mtls_ok: bool,
    next_mtls: Option<Instant>,
    next_public: Option<Instant>,
    status: LinkStatus,
    source: StatsSource,
    stats: Option<NodeStats>,
}

impl<Db, Cluster, Blockchain> PeerAggregator<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        interval_secs: u64,
        cancel: CancellationToken,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .unwrap_or_default();
        Self {
            context,
            interval: Duration::from_secs(interval_secs.max(1)),
            cancel,
            http,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => self.probe_round().await,
            }
        }
    }

    /// Probe every registered node except ourselves, concurrently, then publish
    /// the whole round at once.
    async fn probe_round(&self) {
        let me = self.context.node_address();
        let now = Instant::now();

        // Decide what to fetch for each node from the cache, without holding the
        // lock across awaits.
        let jobs: Vec<Job> = {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            self.context
                .peer_manager
                .all()
                .into_iter()
                .filter(|p| p.node != me)
                .map(|p| {
                    let st = cache.get(&p.node);
                    let try_mtls = st.map_or(true, |s| s.mtls_ok || due(s.next_mtls, now));
                    let try_public = st.map_or(true, |s| !s.mtls_ok && due(s.next_public, now));
                    Job {
                        node: p.node,
                        url: public_stats_url(&p),
                        try_mtls,
                        try_public,
                    }
                })
                .collect()
        };

        let mut set = tokio::task::JoinSet::new();
        for job in jobs {
            let context = self.context.clone();
            let http = self.http.clone();
            set.spawn(async move {
                let mut observe = None;
                let mut timed_out = false;
                let mut refused = false;
                if job.try_mtls {
                    match context.api.get_stats(job.node, &GetStatsReq).await {
                        Ok(res) => observe = Some(NodeStats::from(&res.stats)),
                        Err(error) => {
                            timed_out = busy(&error);
                            refused = refused_mtls(&error);
                        }
                    }
                }
                let mut public = None;
                if observe.is_none() && job.try_public {
                    if let Some(url) = &job.url {
                        public = fetch_public_stats(&http, url).await;
                    }
                }
                Probe {
                    node: job.node,
                    tried_mtls: job.try_mtls,
                    tried_public: job.try_public,
                    timed_out,
                    refused,
                    observe,
                    public,
                }
            });
        }

        let mut results = Vec::new();
        while let Some(joined) = set.join_next().await {
            if let Ok(probe) = joined {
                results.push(probe);
            }
        }

        let mut round = HashMap::new();
        {
            let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            for probe in results {
                let st = cache.entry(probe.node).or_default();
                if let Some(stats) = probe.observe {
                    st.mtls_ok = true;
                    st.next_mtls = None;
                    st.status = LinkStatus::Up;
                    st.source = StatsSource::Observe;
                    st.stats = Some(stats);
                } else {
                    if probe.tried_mtls {
                        st.mtls_ok = false;
                        st.next_mtls =
                            Some(now + if probe.refused { MTLS_RECHECK } else { MTLS_RETRY });
                    }
                    if let Some(stats) = probe.public {
                        st.next_public = Some(now + PUBLIC_TTL);
                        st.status = LinkStatus::Up;
                        st.source = StatsSource::Public;
                        st.stats = Some(stats);
                    } else if probe.tried_public {
                        st.next_public = Some(now + PUBLIC_TTL);
                        // A timeout is not a refusal, so it reads unknown rather
                        // than down. Keep the last stats so the row still shows
                        // its final figures.
                        st.status = if probe.timed_out {
                            LinkStatus::Unknown
                        } else {
                            LinkStatus::Down
                        };
                        if st.stats.is_none() {
                            st.source = StatsSource::None;
                        }
                    }
                }
                round.insert(probe.node, (st.status, st.source, st.stats.clone()));
            }
        }
        peers::replace(round);
    }
}

struct Job {
    node: tape_crypto::Address,
    url: Option<String>,
    try_mtls: bool,
    try_public: bool,
}

struct Probe {
    node: tape_crypto::Address,
    tried_mtls: bool,
    tried_public: bool,
    timed_out: bool,
    refused: bool,
    observe: Option<NodeStats>,
    public: Option<NodeStats>,
}

/// Whether the probe failed because we were busy rather than the peer unreachable.
fn busy(error: &tape_protocol::api::ApiError) -> bool {
    use tape_protocol::api::ApiError;
    matches!(error, ApiError::Timeout | ApiError::RateLimited { .. })
}

/// Whether the peer answered and would not serve the mTLS path.
fn refused_mtls(error: &tape_protocol::api::ApiError) -> bool {
    use tape_protocol::api::ApiError;
    matches!(
        error,
        ApiError::ServerError { .. } | ApiError::NotFound | ApiError::NodeUnresolved(_)
    )
}

/// True when a deadline is unset (never fetched) or has passed.
fn due(deadline: Option<Instant>, now: Instant) -> bool {
    deadline.map_or(true, |t| now >= t)
}

/// Public stats URL for a node, from its advertised address.
fn public_stats_url(peer: &PeerNode) -> Option<String> {
    let addr = &peer.network_address;
    let port = addr.port().checked_sub(PUBLIC_PORT_OFFSET).filter(|p| *p > 0)?;
    let host = match addr.to_socket_addr() {
        Ok(sa) => sa.ip().to_string(),
        Err(_) => addr.domain()?.to_string(),
    };
    Some(format!("http://{host}:{port}{}", tape_protocol::api::NODE_STATS_PATH))
}

/// Fetch a node's public stats, or nothing on any failure.
async fn fetch_public_stats(http: &reqwest::Client, url: &str) -> Option<NodeStats> {
    let resp = http.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let stats: tape_protocol::api::NodeStats = resp.json().await.ok()?;
    Some(NodeStats::from(&stats))
}
