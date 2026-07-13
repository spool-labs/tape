use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::SlotNumber;
use tape_node::config::node::NodeConfig;
use tape_node::context::{AppContext, NodeContext};
use tape_node::core::startup::build_context;
use tape_node::core::channels::{downstream_channels, store_channel};
use tape_node::core::error::NodeError;
use tape_node::core::types::{ChannelName, ServiceName};
use tape_node::features::block::ingest_monitor;
use tape_node::features::block::ingestor::{BlockIngestor, ParsedBlock};
use tape_node::features::bootstrap;
use tape_node::features::replay::manager::ReplayManager;
use tape_node::features::state::manager::StateManager;
use tape_node::runtime::{bootstrap_with_status_listener, join_http_server};
use tape_node::supervisor::Supervisor;
use tape_protocol::Api;
use tape_store::ops::AuditOps;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, Instrument};

use crate::admission::{AdmitAll, Admission};
use crate::cache::GatewaySliceCache;
use crate::http::handlers::s3::accounting::Accounting;
use crate::http::server::{GatewayHttpServer, GatewayS3AdminServer, GatewayS3Server};
use crate::meter::GatewayMeter;
use crate::store::GatewayStoreManager;

async fn drain_block_channel(
    mut rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    channel: ChannelName,
) -> Result<(), NodeError> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            received = rx.recv() => {
                let Some(block) = received else {
                    return if cancel.is_cancelled() {
                        Ok(())
                    } else {
                        Err(NodeError::ChannelClosed { channel })
                    };
                };

                debug!(
                    slot = block.slot.0,
                    channel = ?channel,
                    "gateway drained unused block channel"
                );
            }
        }
    }
}

async fn supervise_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: NodeConfig,
    admission: Arc<dyn Admission>,
    slice_cache: Arc<GatewaySliceCache<Db>>,
    meter: Arc<GatewayMeter>,
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

    #[cfg(feature = "metrics")]
    if context.config.metrics.enabled {
        tape_node::observe::mark_gateway_boards();
        tape_node::observe::register_block_channels(&senders, &store_tx);
    }

    supervisor.spawn(ServiceName::HttpServer, join_http_server(http_server));

    if config.gateway.s3.enabled {
        // One Accounting instance is shared by the S3 data plane and the admin
        // control plane, so their ledger and audit mutations serialize against a
        // single lock and draw from one monotonic audit-sequence counter.
        let accounting = Arc::new(Accounting::new());
        let next_audit_sequence = context
            .store
            .max_audit_sequence()
            .map_err(|error| NodeError::Store(error.to_string()))?
            .saturating_add(1);
        accounting.seed_audit_sequence(next_audit_sequence);

        let s3_server = GatewayS3Server::new(
            context.clone(),
            slice_cache.clone(),
            meter.clone(),
            accounting.clone(),
            admission,
            config.gateway.s3.clone(),
            cancel.clone(),
        );
        supervisor.spawn(ServiceName::S3Server, s3_server.run());

        // The write-authorization admin control plane runs on its own listener,
        // authenticated by an operator token. It is started only when that token
        // is configured, so an unauthenticated control surface is never exposed.
        if config.gateway.s3.write.admin.operator_token.is_some() {
            let admin_server = GatewayS3AdminServer::new(
                context.clone(),
                accounting.clone(),
                &config.gateway.s3,
                cancel.clone(),
            );
            supervisor.spawn(ServiceName::S3AdminServer, admin_server.run());
        } else {
            tracing::warn!(
                "s3 admin control plane disabled: no operator token configured \
                 (gateway.s3.write.admin.operator_token)"
            );
        }
    }

    supervisor.spawn(
        ServiceName::BlockIngestor,
        BlockIngestor::new(context.clone(), start_slot, senders, cancel.clone()).run(),
    );

    supervisor.spawn(
        ServiceName::IngestMonitor,
        ingest_monitor::run(context.clone(), cancel.clone()),
    );

    supervisor.spawn(
        ServiceName::StateManager,
        StateManager::new(context.clone(), receivers.state, cancel.clone()).run(),
    );

    supervisor.spawn(
        ServiceName::ReplayManager,
        ReplayManager::new(context.clone(), receivers.replay, store_tx, cancel.clone()).run(),
    );

    supervisor.spawn(
        ServiceName::StoreManager,
        GatewayStoreManager::new(context.clone(), store_rx, cancel.clone()).run(),
    );

    supervisor.spawn(
        ServiceName::AssignmentManager,
        drain_block_channel(receivers.assignment, cancel.clone(), ChannelName::AssignmentManager),
    );

    supervisor.spawn(
        ServiceName::SnapshotManager,
        drain_block_channel(receivers.snapshot, cancel.clone(), ChannelName::SnapshotManager),
    );

    supervisor.supervise().await
}

pub async fn run_with_context<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: NodeConfig,
    admission: Arc<dyn Admission>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let cancel = CancellationToken::new();

    // The slice cache and per-IP meter are built once and shared by the native
    // read listener and the S3 listener, so the disk-cache budget and the rate
    // limit are each enforced once across both rather than twice.
    let slice_cache = Arc::new(
        GatewaySliceCache::new(context.store.clone(), context.config.gateway.cache.clone())
            .map_err(|error| NodeError::Store(error.to_string()))?,
    );
    let meter = Arc::new(GatewayMeter::new(context.config.gateway.metering.clone()));

    let http_server = GatewayHttpServer::new(
        context.clone(),
        slice_cache.clone(),
        meter.clone(),
        config.http.clone(),
        cancel.clone(),
    );
    let http_server = tokio::spawn(http_server.run().in_current_span());
    let (start_slot, http_server) = bootstrap_with_status_listener(
        bootstrap::run_with_persist(&context, &config, &cancel, crate::store::persist_batch::<Db>),
        http_server,
        &cancel,
    )
    .await?;
    supervise_with_context(
        context,
        config,
        admission,
        slice_cache,
        meter,
        start_slot,
        cancel,
        http_server,
    )
    .await
}

pub async fn run_application(config: NodeConfig) -> Result<(), NodeError> {
    let context: AppContext = build_context(&config).await?;
    run_with_context(context, config, Arc::new(AdmitAll)).await
}
