use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::SlotNumber;
use tape_protocol::fetch::fetch_state;
use tape_protocol::Api;
use tape_retry::{retry_if, RetryConfig};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn, Instrument};
use tracing_subscriber::EnvFilter;

use crate::config::node::NodeConfig;
use crate::config::logs::{LoggingConfig, LoggingFormat};
use crate::context::NodeContext;
use crate::core::startup::build_context;
use crate::core::channels::{downstream_channels, store_channel};
use crate::core::error::NodeError;
use crate::core::types::ServiceName;
use crate::features::block::ingest_monitor;
use crate::features::block::ingestor::BlockIngestor;
use crate::features::bootstrap;
use crate::features::assignment::manager::AssignmentManager;
use crate::features::gc::manager::GcManager;
use crate::features::http::server::HttpServer;
use crate::features::lifecycle::manager::LifecycleManager;
use crate::features::replay::manager::ReplayManager;
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
        epoch = state.epoch().0,
        phase = ?state.phase(),
        committee_size = state.current.committee.len(),
        "loaded protocol state from RPC"
    );

    context.set_state(state)?;

    if let Err(error) = context.refresh_peers().await {
        warn!(error = %error, "peer resolution failed during startup");
    }

    Ok(())
}

/// Start the HTTP/HTTPS listeners before bootstrap so health, stats, and
/// metrics are reachable while the node catches up. The returned handle is
/// adopted by the supervisor once live services start.
fn spawn_http_server<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &NodeConfig,
    cancel: &CancellationToken,
) -> JoinHandle<Result<(), NodeError>>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    tokio::spawn(
        HttpServer::new(
            context.clone(),
            config.http.clone(),
            config.https.clone(),
            cfg!(feature = "metrics") && config.metrics.enabled,
            cancel.clone(),
        )
        .run()
        .in_current_span(),
    )
}

/// Run bootstrap with the status listener already serving. A listener that
/// dies during catch-up fails bootstrap immediately rather than hours later;
/// a bootstrap failure shuts the listener down before propagating. Shared
/// with the gateway, which spawns its own listener and bootstrap future.
pub async fn bootstrap_with_status_listener<F>(
    bootstrap: F,
    mut http_server: JoinHandle<Result<(), NodeError>>,
    cancel: &CancellationToken,
) -> Result<(SlotNumber, JoinHandle<Result<(), NodeError>>), NodeError>
where
    F: Future<Output = Result<SlotNumber, NodeError>>,
{
    tokio::select! {
        result = bootstrap => match result {
            Ok(start_slot) => Ok((start_slot, http_server)),
            Err(error) => {
                cancel.cancel();
                let _ = http_server.await;
                Err(error)
            }
        },
        joined = &mut http_server => {
            cancel.cancel();
            Err(match joined {
                Ok(Ok(())) => NodeError::UnexpectedServiceExit {
                    service: ServiceName::HttpServer,
                },
                Ok(Err(error)) => error,
                Err(source) => NodeError::ServiceJoin {
                    service: ServiceName::HttpServer,
                    source,
                },
            })
        }
    }
}

pub async fn join_http_server(handle: JoinHandle<Result<(), NodeError>>) -> Result<(), NodeError> {
    handle.await.unwrap_or_else(|source| {
        Err(NodeError::ServiceJoin {
            service: ServiceName::HttpServer,
            source,
        })
    })
}

async fn supervise_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: NodeConfig,
    start_slot: SlotNumber,
    cancel: CancellationToken,
    http_server: JoinHandle<Result<(), NodeError>>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let (senders, receivers) = downstream_channels();
    let (store_tx, store_rx) = store_channel();
    let mut supervisor = Supervisor::new(cancel.clone());

    supervisor.spawn(ServiceName::HttpServer, join_http_server(http_server));

    supervisor.spawn(
        ServiceName::BlockIngestor,
        BlockIngestor::new(
            context.clone(),
            start_slot,
            senders,
            cancel.clone()
        ).run(),
    );

    supervisor.spawn(
        ServiceName::IngestMonitor,
        ingest_monitor::run(
            context.clone(),
            cancel.clone()
        ),
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

    supervisor.spawn(
        ServiceName::AssignmentManager,
        AssignmentManager::new(
            context.clone(),
            receivers.assignment,
            cancel.clone(),
        )
        .run(),
    );

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
    let http_server = spawn_http_server(&context, &config, &cancel);
    let (start_slot, http_server) = bootstrap_with_status_listener(
        bootstrap::run(&context, &config, &cancel),
        http_server,
        &cancel,
    )
    .await?;
    supervise_with_context(context, config, start_slot, cancel, http_server).await
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
    let http_server = spawn_http_server(&context, &config, &cancel);
    let (start_slot, http_server) = bootstrap_with_status_listener(
        bootstrap::run(&context, &config, &cancel),
        http_server,
        &cancel,
    )
    .await?;
    let status = NodeRuntimeStatus::new_running();
    let task_status = status.clone();
    let task_cancel = cancel.clone();

    let task = tokio::spawn(
        async move {
            let result = supervise_with_context(
                context,
                config,
                start_slot,
                task_cancel,
                http_server,
            )
            .await;
            task_status.mark_stopped();
            result
        }
        .in_current_span(),
    );

    Ok(NodeRuntimeHandle { cancel, task, status })
}

pub async fn run_application(config: NodeConfig) -> Result<(), NodeError> {
    let context = build_context(&config).await?;
    run_with_context(context, config).await
}
