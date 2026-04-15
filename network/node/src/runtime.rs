use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::fetch::fetch_state;
use tape_protocol::Api;
use tape_retry::{retry_if, RetryConfig};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use tracing_subscriber::EnvFilter;

use crate::config::node::NodeConfig;
use crate::config::logs::{LoggingConfig, LoggingFormat};
use crate::context::NodeContext;
use crate::core::bootstrap::build_context;
use crate::core::channels::{downstream_channels, store_channel};
use crate::core::error::NodeError;
use crate::core::types::ServiceName;
use crate::features::block::ingestor::BlockIngestor;
use crate::features::gc::manager::GcManager;
use crate::features::http::server::HttpServer;
use crate::features::lifecycle::manager::LifecycleManager;
use crate::features::replay::manager::ReplayManager;
#[cfg(any())]
use crate::features::snapshot::manager::SnapshotManager;
use crate::features::spool::manager::SpoolManager;
use crate::features::store::manager::StoreManager;
use crate::features::state::manager::StateManager;
use crate::supervisor::Supervisor;

const MIN_WORKER_THREADS: usize = 4;
const MAX_BLOCKING_THREAD_MULTIPLIER: usize = 4;

pub fn init_tracing(logging: &LoggingConfig) -> Result<(), NodeError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(logging.filter.clone()));

    match logging.format {
        LoggingFormat::Compact => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .compact()
            .try_init()
            .map_err(NodeError::TracingInit),
        LoggingFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .json()
            .try_init()
            .map_err(NodeError::TracingInit),
    }
}

pub fn build_runtime() -> Result<tokio::runtime::Runtime, NodeError> {
    let thread_counter = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&thread_counter);
    let available_threads = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(MIN_WORKER_THREADS);
    let worker_threads = available_threads.max(MIN_WORKER_THREADS);
    let max_blocking_threads = worker_threads.saturating_mul(MAX_BLOCKING_THREAD_MULTIPLIER);

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .thread_name_fn(move || {
            let index = counter.fetch_add(1, Ordering::Relaxed);
            format!("node-{index}")
        })
        .enable_all()
        .build()
        .map_err(NodeError::RuntimeBuild)
}

#[derive(Clone)]
pub struct NodeRuntimeStatus {
    running: Arc<AtomicBool>,
}

impl NodeRuntimeStatus {
    fn new_running() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    fn mark_stopped(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

pub struct NodeRuntimeHandle {
    cancel: CancellationToken,
    task: JoinHandle<Result<(), NodeError>>,
    status: NodeRuntimeStatus,
}

impl NodeRuntimeHandle {
    pub fn is_running(&self) -> bool {
        self.status.is_running()
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub fn status(&self) -> NodeRuntimeStatus {
        self.status.clone()
    }

    pub fn abort(self) {
        self.status.mark_stopped();
        self.task.abort();
    }

    pub async fn wait(self) -> Result<(), NodeError> {
        let status = self.status;
        let result = match self.task.await {
            Ok(result) => result,
            Err(source) => Err(NodeError::ServiceJoin {
                service: ServiceName::Unknown,
                source,
            }),
        };
        status.mark_stopped();
        result
    }

    pub async fn shutdown(self, timeout_duration: Duration) -> Result<(), NodeError> {
        let status = self.status;
        let mut task = self.task;
        self.cancel.cancel();

        let result = match timeout(timeout_duration, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(source)) => Err(NodeError::ServiceJoin {
                service: ServiceName::Unknown,
                source,
            }),
            Err(_) => {
                status.mark_stopped();
                task.abort();
                return Ok(());
            }
        };

        status.mark_stopped();
        result
    }
}

pub async fn initialize_context<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: &CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{

    let state = retry_if(
        RetryConfig::infinite(),
        Some(cancel),
        || fetch_state(&context.rpc),
        |error| error.is_retriable() && !error.is_skipped_slot(),
    )
    .await
    .map_err(NodeError::from)?;

    debug!(
        epoch = state.epoch.0,
        phase = ?state.phase,
        committee_size = state.committee.len(),
        "loaded protocol state from RPC"
    );

    context.set_state(state)?;

    if let Err(error) = context.refresh_peers().await {
        warn!(error = %error, "peer resolution failed during startup");
    }

    Ok(())
}

async fn supervise_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: NodeConfig,
    cancel: CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let (senders, receivers) = downstream_channels();
    let (store_tx, store_rx) = store_channel();
    let mut supervisor = Supervisor::new(cancel.clone());

    supervisor.spawn(
        ServiceName::HttpServer,
        HttpServer::new(
            context.clone(),
            config.http.clone(),
            cfg!(feature = "metrics") && config.metrics.enabled,
            cancel.clone()
        ).run(),
    );

    supervisor.spawn(
        ServiceName::BlockIngestor,
        BlockIngestor::new(
            context.clone(),
            config.solana.block_start_slot(),
            senders,
            cancel.clone()
        ).run(),
    );

    supervisor.spawn(
        ServiceName::StateManager,
        StateManager::new(
            context.clone(),
            receivers.state,
            cancel.clone(),
        ).run(),
    );

    supervisor.spawn(
        ServiceName::LifecycleManager,
        LifecycleManager::new(
            context.clone(),
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::SpoolManager,
        SpoolManager::new(
            context.clone(),
            config.recovery.clone(),
            cancel.clone(),
        )
        .run(),
    );

    #[cfg(any())]
    supervisor.spawn(
        ServiceName::SnapshotManager,
        SnapshotManager::new(
            context.clone(),
            receivers.snapshot,
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::ReplayManager,
        ReplayManager::new(
            context.clone(),
            receivers.replay,
            store_tx,
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::StoreManager,
        StoreManager::new(
            context.clone(),
            store_rx,
            cancel.clone(),
        ).run(),
    );

    supervisor.spawn(
        ServiceName::GcManager,
        GcManager::new(
            context, 
            config.store.gc.clone(),
            cancel
        ).run(),
    );

    supervisor.supervise().await
}

pub async fn run_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: NodeConfig,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let cancel = CancellationToken::new();
    initialize_context(&context, &cancel).await?;
    supervise_with_context(context, config, cancel).await
}

pub async fn start_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: NodeConfig,
) -> Result<NodeRuntimeHandle, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let cancel = CancellationToken::new();
    initialize_context(&context, &cancel).await?;
    let status = NodeRuntimeStatus::new_running();
    let task_status = status.clone();
    let task_cancel = cancel.clone();

    let task = tokio::spawn(async move {
        let result = supervise_with_context(
            context,
            config,
            task_cancel,
        )
        .await;
        task_status.mark_stopped();
        result
    });

    Ok(NodeRuntimeHandle { cancel, task, status })
}

pub async fn run_application(config: NodeConfig) -> Result<(), NodeError> {
    let context = build_context(&config).await?;
    run_with_context(context, config).await
}
