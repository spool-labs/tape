use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::core::error::NodeError;
use crate::core::types::BlockHeight;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub bind_addr: SocketAddr,
    pub concurrency_limit: usize,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ChannelConfig {
    pub parsed_block_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct BlockIngestorConfig {
    pub start_height: BlockHeight,
    pub fetch_retry: RetryConfig,
}

#[derive(Debug, Clone)]
pub struct EpochManagerConfig {
    pub state_retry: RetryConfig,
}

#[derive(Debug, Clone)]
pub struct SpoolManagerConfig {
    pub max_spools: usize,
    pub max_parallel_spools: usize,
    pub worker_heartbeat: Duration,
    pub peer_retry: RetryConfig,
}

#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub max_snapshot_items: usize,
}

#[derive(Debug, Clone)]
pub struct ReplayConfig;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub node: NodeConfig,
    pub http: HttpConfig,
    pub channels: ChannelConfig,
    pub block: BlockIngestorConfig,
    pub epoch: EpochManagerConfig,
    pub spool: SpoolManagerConfig,
    pub snapshot: SnapshotConfig,
    pub replay: ReplayConfig,
}

impl AppConfig {
    pub fn production() -> Result<Self, NodeError> {
        let available_threads = match std::thread::available_parallelism() {
            Ok(value) => value.get(),
            Err(_) => 4,
        };
        let worker_threads = available_threads.max(4);

        Ok(Self {
            runtime: RuntimeConfig {
                worker_threads,
                max_blocking_threads: worker_threads.saturating_mul(4),
            },
            node: NodeConfig {
            },
            http: HttpConfig {
                bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3000),
                concurrency_limit: 2048,
                request_timeout: Duration::from_secs(5),
            },
            channels: ChannelConfig {
                parsed_block_capacity: 256,
            },
            block: BlockIngestorConfig {
                start_height: BlockHeight(1),
                fetch_retry: RetryConfig::infinite(),
            },
            epoch: EpochManagerConfig {
                state_retry: RetryConfig::ten(),
            },
            spool: SpoolManagerConfig {
                max_spools: 1000,
                max_parallel_spools: worker_threads.clamp(4, 64),
                worker_heartbeat: Duration::from_secs(2),
                peer_retry: RetryConfig::five(),
            },
            snapshot: SnapshotConfig {
                max_snapshot_items: 10_000,
            },
            replay: ReplayConfig,
        })
    }
}
