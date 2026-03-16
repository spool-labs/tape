use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tape_protocol::fetch::fetch_state;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use tracing_subscriber::EnvFilter;

use crate::core::bootstrap::build_context;
use crate::core::channels::{downstream_channels, state_channel};
use crate::core::config::{AppConfig, RuntimeConfig};
use crate::core::error::NodeError;
use crate::core::supervisor::Supervisor;
use crate::core::types::ServiceName;
use crate::features::block::ingestor::BlockIngestor;
use crate::features::epoch::manager::EpochManager;
use crate::features::gc::manager::GcManager;
use crate::features::http::server::HttpServer;
use crate::features::replay::manager::ReplayManager;
use crate::features::snapshot::manager::SnapshotManager;
use crate::features::spool::manager::SpoolManager;
use crate::features::state::manager::StateManager;

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

pub async fn run_application(config: AppConfig) -> Result<(), NodeError> {
    let cancel = CancellationToken::new();
    let context = build_context(&config).await?;

    let state = tape_retry::retry_if(
        config.epoch.state_retry.clone(),
        Some(&cancel),
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

    let (senders, receivers) = downstream_channels(&config.channels);
    let (state_tx, state_rx) = state_channel(&config.channels);
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
        ServiceName::EpochManager,
        EpochManager::new(
            context.clone(),
            config.epoch.clone(),
            receivers.epoch,
            cancel.clone()
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::SpoolManager,
        SpoolManager::new(
            context.clone(), 
            config.spool.clone(), 
            receivers.spool,
            cancel.clone()
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
            state_tx,
            cancel.clone(),
        )
        .run(),
    );

    supervisor.spawn(
        ServiceName::StateManager,
        StateManager::new(
            context.clone(),
            config.state.clone(),
            state_rx, 
            cancel.clone()
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
