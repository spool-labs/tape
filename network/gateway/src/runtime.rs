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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, Instrument};

use crate::http::server::GatewayHttpServer;
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
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let cancel = CancellationToken::new();
    let http_server =
        GatewayHttpServer::new(context.clone(), config.http.clone(), cancel.clone())?;
    let http_server = tokio::spawn(http_server.run().in_current_span());
    let (start_slot, http_server) = bootstrap_with_status_listener(
        bootstrap::run_with_persist(&context, &config, &cancel, crate::store::persist_batch::<Db>),
        http_server,
        &cancel,
    )
    .await?;
    supervise_with_context(context, start_slot, cancel, http_server).await
}

pub async fn run_application(config: NodeConfig) -> Result<(), NodeError> {
    let context: AppContext = build_context(&config).await?;
    run_with_context(context, config).await
}
