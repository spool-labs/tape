use std::sync::atomic::{AtomicUsize, Ordering};
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

use crate::config::{AppConfig, RuntimeConfig};
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
use crate::features::snapshot::manager::SnapshotManager;
use crate::features::spool::manager::SpoolManager;
use crate::features::store::manager::StoreManager;
use crate::features::state::manager::StateManager;
use crate::supervisor::Supervisor;

pub fn init_tracing() -> Result<(), NodeError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .try_init()
        .map_err(NodeError::TracingInit)
}

pub fn build_runtime(config: &RuntimeConfig) -> Result<tokio::runtime::Runtime, NodeError> {
    let thread_counter = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&thread_counter);

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.worker_threads)
        .max_blocking_threads(config.max_blocking_threads)
        .thread_name_fn(move || {
            let index = counter.fetch_add(1, Ordering::Relaxed);
            format!("node-{index}")
        })
        .enable_all()
        .build()
        .map_err(NodeError::RuntimeBuild)
}

pub struct NodeRuntimeHandle {
    cancel: CancellationToken,
    task: JoinHandle<Result<(), NodeError>>,
}

impl NodeRuntimeHandle {
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub fn abort(self) {
        self.task.abort();
    }

    pub async fn wait(self) -> Result<(), NodeError> {
        match self.task.await {
            Ok(result) => result,
            Err(source) => Err(NodeError::ServiceJoin {
                service: ServiceName::Unknown,
                source,
            }),
        }
    }

    pub async fn shutdown(self, timeout_duration: Duration) -> Result<(), NodeError> {
        let mut task = self.task;
        self.cancel.cancel();

        match timeout(timeout_duration, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(source)) => Err(NodeError::ServiceJoin {
                service: ServiceName::Unknown,
                source,
            }),
            Err(_) => {
                task.abort();
                Ok(())
            }
        }
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
    config: AppConfig,
    cancel: CancellationToken,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let (senders, receivers) = downstream_channels(&config.channels);
    let (store_tx, store_rx) = store_channel(&config.channels);
    let mut supervisor = Supervisor::new(cancel.clone());

    supervisor.spawn(
        ServiceName::HttpServer,
        HttpServer::new(
            context.clone(),
            config.http.clone(),
            cancel.clone()
        ).run(),
    );

    supervisor.spawn(
        ServiceName::BlockIngestor,
        BlockIngestor::new(
            context.clone(),
            config.block.clone(),
            senders,
            cancel.clone()
        ).run(),
    );

    supervisor.spawn(
        ServiceName::StateManager,
        StateManager::new(
            context.clone(),
            config.state.clone(),
            receivers.state,
            cancel.clone(),
        ).run(),
    );

    supervisor.spawn(
        ServiceName::LifecycleManager,
        LifecycleManager::new(
            context.clone(),
            config.epoch_lifecycle.clone(),
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::SpoolManager,
        SpoolManager::new(
            context.clone(),
            config.spool.clone(),
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::SnapshotManager,
        SnapshotManager::new(
            context.clone(),
            config.snapshot.clone(),
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::ReplayManager,
        ReplayManager::new(
            context.clone(),
            config.replay.clone(),
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
            config.store.clone(),
            store_rx,
            cancel.clone(),
        ).run(),
    );

    supervisor.spawn(
        ServiceName::GcManager,
        GcManager::new(
            context, 
            config.gc.clone(),
            cancel
        ).run(),
    );

    supervisor.supervise().await
}

pub async fn run_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: AppConfig,
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
    config: AppConfig,
) -> Result<NodeRuntimeHandle, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let cancel = CancellationToken::new();
    initialize_context(&context, &cancel).await?;

    let task = tokio::spawn(supervise_with_context(
        context,
        config,
        cancel.clone(),
    ));

    Ok(NodeRuntimeHandle { cancel, task })
}

pub async fn run_application(config: AppConfig) -> Result<(), NodeError> {
    let context = build_context(&config).await?;
    run_with_context(context, config).await
}
