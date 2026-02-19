//! Pipeline — wires the ingestor, FSM, scheduler, and supervisor together.
//!
//! The ingestor fetches and parses blocks, sending them over a bounded channel
//! to the FSM. The FSM applies each block and forwards state changes to the
//! scheduler. The scheduler diffs desired vs running tasks and sends directives
//! to the supervisor. Channel backpressure ensures no component outpaces another.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::runtime::NodeContext;
use crate::fsm::{Fsm, StateChange, UserEvent};
use crate::http::HttpServer;
use crate::ingestor::{BlockIngestor, IngestedBlock};
use crate::runtime::PeerService;
use crate::scheduler::{Directive, Scheduler};
use crate::supervisor::{Supervisor, TaskResult};

const INGESTOR_CHANNEL_CAPACITY: usize = 4;
const STATE_CHANGE_CHANNEL_CAPACITY: usize = 16;
const USER_EVENT_CHANNEL_CAPACITY: usize = 256;
const DIRECTIVE_CHANNEL_CAPACITY: usize = 256;
const RESULT_CHANNEL_CAPACITY: usize = 256;

/// Handles for all runtime tasks.
pub struct RuntimeHandles {
    pub ingestor: JoinHandle<()>,
    pub fsm: JoinHandle<()>,
    pub scheduler: JoinHandle<()>,
    pub supervisor: JoinHandle<()>,
    pub peer_service: JoinHandle<()>,
    pub http: JoinHandle<()>,
}

/// Spawn the block processing pipeline.
///
/// Creates bounded channels between the ingestor and FSM, spawning both as
/// tokio tasks. Returns a receiver for state changes and the task handles.
pub async fn spawn_pipeline<S: Store + 'static, R: Rpc + 'static>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> (
    mpsc::Receiver<Vec<StateChange>>,
    mpsc::Sender<UserEvent>,
    JoinHandle<()>,
    JoinHandle<()>,
) {
    let node_id = context.node_id();
    let (block_tx, block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
    let (change_tx, change_rx) = mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);
    let (user_event_tx, user_event_rx) = mpsc::channel::<UserEvent>(USER_EVENT_CHANNEL_CAPACITY);

    let ingestor_context = context.clone();
    let ingestor_cancel = cancel.clone();
    let ingestor_span = tracing::info_span!("", node_id = node_id.0);
    let ingestor_handle = tokio::spawn(
        async move {
            if let Err(e) =
                BlockIngestor::run(ingestor_context, block_tx, ingestor_cancel).await
            {
                tracing::error!("Ingestor error: {e}");
            }
        }
        .instrument(ingestor_span),
    );

    let fsm_cancel = cancel.clone();
    let fsm_context = context.clone();
    let fsm_span = tracing::info_span!("", node_id = node_id.0);
    let fsm_handle = tokio::spawn(
        run_fsm_loop(fsm_context, block_rx, user_event_rx, change_tx, fsm_cancel)
        .instrument(fsm_span),
    );

    (change_rx, user_event_tx, ingestor_handle, fsm_handle)
}

/// Spawn the full runtime: ingestor, FSM, scheduler, and supervisor.
pub async fn spawn_runtime<S: Store + 'static, R: Rpc + 'static>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> RuntimeHandles {
    let (change_rx, user_event_tx, ingestor_handle, fsm_handle) =
        spawn_pipeline(context.clone(), cancel.clone()).await;

    let (directive_tx, directive_rx) = mpsc::channel::<Directive>(DIRECTIVE_CHANNEL_CAPACITY);
    let (result_tx, result_rx) = mpsc::channel::<TaskResult>(RESULT_CHANNEL_CAPACITY);
    let (peer_service, peer_handle) = PeerService::new();

    let scheduler = Scheduler::new(context.clone());
    let scheduler_cancel = cancel.clone();
    let node_id = context.node_id();
    let scheduler_span = tracing::info_span!("", node_id = node_id.0);
    let scheduler_handle = tokio::spawn(
        async move {
            scheduler
                .run(change_rx, result_rx, directive_tx, scheduler_cancel)
                .await;
        }
        .instrument(scheduler_span),
    );

    let supervisor = Supervisor::new(context.clone(), peer_handle, result_tx);
    let supervisor_cancel = cancel.clone();
    let supervisor_span = tracing::info_span!("", node_id = node_id.0);
    let supervisor_handle = tokio::spawn(
        async move {
            supervisor.run(directive_rx, supervisor_cancel).await;
        }
        .instrument(supervisor_span),
    );

    let peer_service_cancel = cancel.clone();
    let peer_service_span = tracing::info_span!("", node_id = node_id.0);
    let peer_service_handle = tokio::spawn(
        async move {
            peer_service.run(peer_service_cancel).await;
        }
        .instrument(peer_service_span),
    );

    let http_ctx = context;
    let http_cancel = cancel;
    let http_span = tracing::info_span!("", node_id = node_id.0);
    let http_handle = tokio::spawn(
        async move {
            let server = HttpServer::new(http_ctx, Some(user_event_tx));
            if let Err(e) = server.serve(http_cancel).await {
                tracing::error!("HTTP server error: {e}");
            }
        }
        .instrument(http_span),
    );

    RuntimeHandles {
        ingestor: ingestor_handle,
        fsm: fsm_handle,
        scheduler: scheduler_handle,
        supervisor: supervisor_handle,
        peer_service: peer_service_handle,
        http: http_handle,
    }
}

enum LoopControl {
    Continue,
    Break,
}

async fn run_fsm_loop<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    mut block_rx: mpsc::Receiver<IngestedBlock>,
    mut user_event_rx: mpsc::Receiver<UserEvent>,
    change_tx: mpsc::Sender<Vec<StateChange>>,
    cancel: CancellationToken,
) {
    let fsm = Fsm::new(context.clone());
    loop {
        tokio::select! {
            maybe_block = block_rx.recv() => {
                let Some(block) = maybe_block else { break };
                if let LoopControl::Break = handle_block(&fsm, &context, block, &change_tx).await {
                    break;
                }
            }
            maybe_event = user_event_rx.recv() => {
                let Some(event) = maybe_event else { break };
                if let Err(e) = fsm.apply_user_event(&event) {
                    tracing::error!("FSM user event error: {e}");
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

async fn handle_block<S: Store, R: Rpc>(
    fsm: &Fsm<S, R>,
    context: &Arc<NodeContext<S, R>>,
    block: IngestedBlock,
    change_tx: &mpsc::Sender<Vec<StateChange>>,
) -> LoopControl {
    match fsm.apply(&block) {
        Ok(changes) => {
            context.stats.inc_blocks();
            if !changes.is_empty() && change_tx.send(changes).await.is_err() {
                return LoopControl::Break;
            }
        }
        Err(e) => tracing::error!("FSM error: {e}"),
    }
    LoopControl::Continue
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tape_api::event::EpochAdvanced;
    use tape_blocks::ParsedInstruction;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
    use tape_crypto::Hash;
    use tape_store::ops::{MetaOps, SpoolOps};
    use tape_store::types::{NodeStatus, SpoolStatus};

    use crate::ingestor::IngestedBlock;
    use crate::runtime::PeerService;
    use crate::runtime::test_utils::test_context;

    async fn spawn_test_fsm<S: Store + 'static, R: Rpc + 'static>(
        context: Arc<NodeContext<S, R>>,
        block_rx: mpsc::Receiver<IngestedBlock>,
        change_tx: mpsc::Sender<Vec<StateChange>>,
        cancel: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let (user_event_tx, user_event_rx) = mpsc::channel::<UserEvent>(USER_EVENT_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let _keepalive = user_event_tx;
            run_fsm_loop(context, block_rx, user_event_rx, change_tx, cancel).await;
        })
    }

    #[tokio::test]
    async fn fsm_blocks() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        let (block_tx, block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
        let (change_tx, mut change_rx) =
            mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);

        let fsm_handle = spawn_test_fsm(ctx.clone(), block_rx, change_tx, cancel.clone()).await;

        // Send blocks directly to the FSM channel
        let block1 = IngestedBlock {
            slot: SlotNumber(10),
            instructions: vec![ParsedInstruction::AdvanceEpoch {
                event: EpochAdvanced {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                    timestamp: [0; 8],
                    committee_size: [0; 8],
                    total_stake: [0; 8],
                    storage_price: [0; 8],
                    storage_capacity: StorageUnits(0),
                    nonce: Hash::default(),
                },
            }],
        };

        block_tx.send(block1).await.unwrap();

        // Receive state changes
        let changes = change_rx.recv().await.unwrap();
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            StateChange::EpochAdvanced { epoch } if *epoch == EpochNumber(1)
        ));

        // Verify store state
        assert_eq!(
            ctx.store.get_chain_epoch().unwrap(),
            Some(EpochNumber(1))
        );
        assert_eq!(
            ctx.store.get_sync_cursor().unwrap(),
            Some(SlotNumber(10))
        );

        // Clean shutdown
        drop(block_tx);
        fsm_handle.await.unwrap();
    }

    #[tokio::test]
    async fn runtime_flow() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Pre-populate spool state so scheduler has work to do
        ctx.store
            .set_spool_status(5, SpoolStatus::ActiveSync)
            .unwrap();

        // We use spawn_pipeline + manual wiring since LiteSvmRpc
        // doesn't produce real blocks with Tapedrive instructions.
        let (change_rx, _user_event_tx, _ingestor_handle, _fsm_handle) =
            spawn_pipeline(ctx.clone(), cancel.clone())
                .await;

        let (directive_tx, directive_rx) =
            mpsc::channel::<Directive>(DIRECTIVE_CHANNEL_CAPACITY);
        let (result_tx, result_rx) =
            mpsc::channel::<TaskResult>(RESULT_CHANNEL_CAPACITY);

        let scheduler = Scheduler::new(ctx.clone());
        let scheduler_cancel = cancel.clone();
        let scheduler_handle = tokio::spawn(async move {
            scheduler
                .run(change_rx, result_rx, directive_tx, scheduler_cancel)
                .await;
        });

        let (peer_service, peer_handle) = PeerService::new();
        let peer_cancel = cancel.clone();
        let peer_service_handle = tokio::spawn(async move {
            peer_service.run(peer_cancel).await;
        });
        let supervisor = Supervisor::new(ctx.clone(), peer_handle, result_tx);
        let supervisor_cancel = cancel.clone();
        let supervisor_handle = tokio::spawn(async move {
            supervisor.run(directive_rx, supervisor_cancel).await;
        });

        // Give the pipeline a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Clean shutdown
        cancel.cancel();
        scheduler_handle.await.unwrap();
        supervisor_handle.await.unwrap();
        peer_service_handle.await.unwrap();
    }

    #[tokio::test]
    async fn ingestor_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5, no cursor → bootstrap needed
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(5)).unwrap();

        let (block_tx, _block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, block_tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Ingestor should be in the wait loop
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate bootstrap completing by setting cursor
        ctx.store
            .set_sync_cursor(SlotNumber(1000))
            .unwrap();

        // Wait for ingestor to notice (2s poll + margin)
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Clean shutdown
        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn fsm_shutdown() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        let (block_tx, block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
        let (change_tx, change_rx) =
            mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);

        let fsm_handle = spawn_test_fsm(ctx.clone(), block_rx, change_tx, cancel.clone()).await;

        // Drop the change receiver — sends will fail
        drop(change_rx);

        // Send a block that produces a StateChange
        let block = IngestedBlock {
            slot: SlotNumber(10),
            instructions: vec![ParsedInstruction::AdvanceEpoch {
                event: EpochAdvanced {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                    timestamp: [0; 8],
                    committee_size: [0; 8],
                    total_stake: [0; 8],
                    storage_price: [0; 8],
                    storage_capacity: StorageUnits(0),
                    nonce: Hash::default(),
                },
            }],
        };
        let _ = block_tx.send(block).await;

        // FSM should exit within 1s because change_tx.send fails
        tokio::time::timeout(Duration::from_secs(1), fsm_handle)
            .await
            .expect("FSM should exit when change channel closes")
            .unwrap();
    }
}
