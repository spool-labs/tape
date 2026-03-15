use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

pub fn init_tracing() -> Result<(), NodeError> {
    let filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into());

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

pub async fn run_application(config: AppConfig) -> Result<(), NodeError> {
    let cancel = CancellationToken::new();

    // <todo>

    let (senders, receivers) = downstream_channels(&config.channels);

    let mut supervisor = Supervisor::new(cancel.clone());

    supervisor.spawn(
        ServiceName::HttpServer,
        HttpServer::new(config.http.clone(), cancel.clone()).run(),
    );

    supervisor.spawn(
        ServiceName::BlockIngestor,
        BlockIngestor::new(
            context.clone(),
            config.block.clone(),
            senders,
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::EpochManager,
        EpochManager::new(
            context.clone(),
            config.epoch.clone(),
            receivers.epoch,
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::SpoolManager,
        SpoolManager::new(
            context.clone(),
            config.spool.clone(),
            receivers.spool,
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::SnapshotManager,
        SnapshotManager::new(
            context.clone(),
            config.snapshot.clone(),
            receivers.snapshot,
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
            cancel.clone()
        ).run(),
    );

    supervisor.supervise().await
}
