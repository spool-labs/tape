//! Pipeline — wires the ingestor, FSM, reconciler, and supervisor together.
//!
//! The ingestor fetches and parses blocks, sending them over a bounded channel
//! to the FSM. The FSM applies each block and forwards state changes to the
//! reconciler. The reconciler diffs desired vs running tasks and sends directives
//! to the supervisor. Channel backpressure ensures no component outpaces another.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::core::NodeContext;
use crate::fsm::{Fsm, StateChange, UserEvent};
use crate::http::HttpServer;
use crate::ingestor::{BlockIngestor, IngestedBlock};
use crate::reconciler::{Directive, Reconciler};
use crate::supervisor::{Supervisor, TaskResult};

const INGESTOR_CHANNEL_CAPACITY: usize = 4;
const STATE_CHANGE_CHANNEL_CAPACITY: usize = 16;
const USER_EVENT_CHANNEL_CAPACITY: usize = 256;
const DIRECTIVE_CHANNEL_CAPACITY: usize = 256;
const RESULT_CHANNEL_CAPACITY: usize = 256;

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
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let node_id = context.node_id();
    let (block_tx, mut block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
    let (change_tx, change_rx) = mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);
    let (user_event_tx, mut user_event_rx) = mpsc::channel::<UserEvent>(USER_EVENT_CHANNEL_CAPACITY);

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
        async move {
            let fsm = Fsm::new(fsm_context.clone());
            loop {
                tokio::select! {
                    block = block_rx.recv() => {
                        match block {
                            Some(block) => {
                                match fsm.apply(&block) {
                                    Ok(changes) => {
                                        fsm_context.stats.inc_blocks();
                                        if !changes.is_empty() {
                                            if change_tx.send(changes).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => tracing::error!("FSM error: {e}"),
                                }
                            }
                            None => break,
                        }
                    }
                    event = user_event_rx.recv() => {
                        match event {
                            Some(event) => {
                                if let Err(e) = fsm.apply_user_event(&event) {
                                    tracing::error!("FSM user event error: {e}");
                                }
                            }
                            None => break,
                        }
                    }
                    _ = fsm_cancel.cancelled() => break,
                }
            }
        }
        .instrument(fsm_span),
    );

    (change_rx, user_event_tx, ingestor_handle, fsm_handle)
}

/// Handles for all runtime tasks.
pub struct RuntimeHandles {
    pub ingestor: JoinHandle<()>,
    pub fsm: JoinHandle<()>,
    pub reconciler: JoinHandle<()>,
    pub supervisor: JoinHandle<()>,
    pub http: JoinHandle<()>,
}

/// Spawn the full runtime: ingestor, FSM, reconciler, and supervisor.
pub async fn spawn_runtime<S: Store + 'static, R: Rpc + 'static>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> RuntimeHandles {
    let (change_rx, user_event_tx, ingestor_handle, fsm_handle) =
        spawn_pipeline(context.clone(), cancel.clone()).await;

    let (directive_tx, directive_rx) = mpsc::channel::<Directive>(DIRECTIVE_CHANNEL_CAPACITY);
    let (result_tx, result_rx) = mpsc::channel::<TaskResult>(RESULT_CHANNEL_CAPACITY);

    let reconciler = Reconciler::new(context.clone());
    let reconciler_cancel = cancel.clone();
    let node_id = context.node_id();
    let reconciler_span = tracing::info_span!("", node_id = node_id.0);
    let reconciler_handle = tokio::spawn(
        async move {
            reconciler
                .run(change_rx, result_rx, directive_tx, reconciler_cancel)
                .await;
        }
        .instrument(reconciler_span),
    );

    let supervisor = Supervisor::new(context.clone(), result_tx);
    let supervisor_cancel = cancel.clone();
    let supervisor_span = tracing::info_span!("", node_id = node_id.0);
    let supervisor_handle = tokio::spawn(
        async move {
            supervisor.run(directive_rx, supervisor_cancel).await;
        }
        .instrument(supervisor_span),
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
        reconciler: reconciler_handle,
        supervisor: supervisor_handle,
        http: http_handle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_api::event::EpochAdvanced;
    use tape_blocks::ParsedInstruction;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
    use tape_store::ops::{MetaOps, SpoolOps};
    use tape_store::types::{NodeStatus, SpoolStatus};

    use crate::test_util::test_context;

    #[tokio::test]
    async fn fsm_processes_blocks_from_channel() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        let (block_tx, mut block_rx) =
            mpsc::channel::<crate::ingestor::IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
        let (change_tx, mut change_rx) =
            mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);

        let fsm_ctx = ctx.clone();
        let fsm_cancel = cancel.clone();
        let fsm_handle = tokio::spawn(async move {
            let fsm = Fsm::new(fsm_ctx);
            loop {
                tokio::select! {
                    block = block_rx.recv() => {
                        match block {
                            Some(block) => {
                                match fsm.apply(&block) {
                                    Ok(changes) if !changes.is_empty() => {
                                        let _ = change_tx.send(changes).await;
                                    }
                                    Ok(_) => {}
                                    Err(e) => panic!("FSM error: {e}"),
                                }
                            }
                            None => break,
                        }
                    }
                    _ = fsm_cancel.cancelled() => break,
                }
            }
        });

        // Send blocks directly to the FSM channel
        let block1 = crate::ingestor::IngestedBlock {
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
            ctx.store.get_current_epoch().unwrap(),
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
    async fn runtime_end_to_end() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Pre-populate spool state so reconciler has work to do
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

        let reconciler = Reconciler::new(ctx.clone());
        let reconciler_cancel = cancel.clone();
        let reconciler_handle = tokio::spawn(async move {
            reconciler
                .run(change_rx, result_rx, directive_tx, reconciler_cancel)
                .await;
        });

        let supervisor = Supervisor::new(ctx.clone(), result_tx);
        let supervisor_cancel = cancel.clone();
        let supervisor_handle = tokio::spawn(async move {
            supervisor.run(directive_rx, supervisor_cancel).await;
        });

        // Give the pipeline a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Clean shutdown
        cancel.cancel();
        reconciler_handle.await.unwrap();
        supervisor_handle.await.unwrap();
    }

    #[tokio::test]
    async fn ingestor_waits_for_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5, no cursor → bootstrap needed
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(5)).unwrap();

        let (block_tx, _block_rx) =
            mpsc::channel::<crate::ingestor::IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, block_tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Ingestor should be in the wait loop
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Simulate bootstrap completing by setting cursor
        ctx.store
            .set_sync_cursor(SlotNumber(1000))
            .unwrap();

        // Wait for ingestor to notice (2s poll + margin)
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Clean shutdown
        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn fsm_exits_on_closed_change() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        let (block_tx, mut block_rx) =
            mpsc::channel::<crate::ingestor::IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
        let (change_tx, change_rx) =
            mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);

        let fsm_ctx = ctx.clone();
        let fsm_cancel = cancel.clone();
        let fsm_handle = tokio::spawn(async move {
            let fsm = Fsm::new(fsm_ctx);
            loop {
                tokio::select! {
                    block = block_rx.recv() => {
                        match block {
                            Some(block) => {
                                match fsm.apply(&block) {
                                    Ok(changes) if !changes.is_empty() => {
                                        if change_tx.send(changes).await.is_err() {
                                            break;
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(e) => panic!("FSM error: {e}"),
                                }
                            }
                            None => break,
                        }
                    }
                    _ = fsm_cancel.cancelled() => break,
                }
            }
        });

        // Drop the change receiver — sends will fail
        drop(change_rx);

        // Send a block that produces a StateChange
        let block = crate::ingestor::IngestedBlock {
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
                },
            }],
        };
        let _ = block_tx.send(block).await;

        // FSM should exit within 1s because change_tx.send fails
        tokio::time::timeout(std::time::Duration::from_secs(1), fsm_handle)
            .await
            .expect("FSM should exit when change channel closes")
            .unwrap();
    }
}
