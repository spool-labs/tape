//! Runtime — wires the ingestor, FSM, scheduler, and task_runner together.
//!
//! The ingestor fetches and parses blocks, sending them over a bounded channel
//! to the FSM. The FSM applies each block and forwards state changes to the
//! scheduler. The scheduler diffs desired vs running tasks and sends actions
//! to the task_runner. Channel backpressure ensures no component outpaces another.

use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use tape_retry::RetryConfig;

use crate::state::fetch_chain_state;
use crate::core::NodeContext;
use tape_core::types::NodeId;
use crate::core::{PeerHandle, PeerService};
use crate::fsm::{Fsm, StateChange, UserEvent};
use crate::http::HttpServer;
use crate::ingestor::{BlockIngestor, IngestedBlock};
use crate::scheduler::SpoolPlanner;
use crate::TaskResult;
use crate::task_scheduler::{Action, TaskScheduler};
use crate::task_runner::TaskRunner;

const INGESTOR_CHANNEL_CAPACITY: usize = 4;
const STATE_CHANGE_CHANNEL_CAPACITY: usize = 16;
const USER_EVENT_CHANNEL_CAPACITY: usize = 256;
const ACTION_CHANNEL_CAPACITY: usize = 256;
const RESULT_CHANNEL_CAPACITY: usize = 256;

/// Handles for all runtime tasks.
pub struct RuntimeHandles {
    pub ingestor: JoinHandle<()>,
    pub fsm: JoinHandle<()>,
    pub scheduler: JoinHandle<()>,
    pub task_runner: JoinHandle<()>,
    pub peer_service: JoinHandle<()>,
    pub http: JoinHandle<()>,
}

/// Spawn the runtime component channels.
///
/// Creates bounded channels between the ingestor and FSM, spawning both as
/// tokio tasks. Returns a receiver for state changes and the task handles.
pub async fn spawn_runtime_channels<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> (
    mpsc::Receiver<Vec<StateChange>>,
    mpsc::Sender<UserEvent>,
    JoinHandle<()>,
    JoinHandle<()>,
) {
    let channels = build_channels();
    let ingestor_handle = spawn_ingestor(
        context.clone(),
        cancel.clone(),
        channels.block_tx
    );

    let fsm_handle = spawn_fsm(
        context,
        cancel,
        channels.block_rx,
        channels.user_event_rx,
        channels.change_tx,
    );

    (
        channels.change_rx,
        channels.user_event_tx,
        ingestor_handle,
        fsm_handle,
    )
}

/// Spawn the full runtime: ingestor, FSM, scheduler, and task_runner.
///
/// Seeds the in-memory ChainState from RPC before spawning components.
/// If the seed fetch fails, components start with default (empty) state
/// and ChainState is populated on the first EpochAdvanced from the FSM.
pub async fn spawn_runtime<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
) -> RuntimeHandles {
    // One time fetch of current on-chain state
    boostrap_chain_state(&context).await;

    let (change_rx, user_event_tx, ingestor_handle, fsm_handle) =
        spawn_runtime_channels(context.clone(), cancel.clone()).await;

    let (action_tx, action_rx) = mpsc::channel::<Action>(ACTION_CHANNEL_CAPACITY);
    let (result_tx, result_rx) = mpsc::channel::<TaskResult>(RESULT_CHANNEL_CAPACITY);
    let (peer_service, peer_handle) = PeerService::new();

    let scheduler_handle = spawn_scheduler(
        context.clone(),
        cancel.clone(),
        change_rx,
        result_rx,
        action_tx,
    );

    let task_runner_handle = spawn_task_runner(
        context.clone(),
        cancel.clone(),
        action_rx,
        result_tx,
        peer_handle,
    );

    let peer_service_handle = spawn_peer_service(
        cancel.clone(),
        peer_service,
        context.node_id(),
    );

    let http_handle = spawn_http_server(
        context,
        cancel,
        user_event_tx
    );

    RuntimeHandles {
        ingestor: ingestor_handle,
        fsm: fsm_handle,
        scheduler: scheduler_handle,
        task_runner: task_runner_handle,
        peer_service: peer_service_handle,
        http: http_handle,
    }
}

struct RuntimeChannels {
    block_tx: mpsc::Sender<IngestedBlock>,
    block_rx: mpsc::Receiver<IngestedBlock>,
    change_tx: mpsc::Sender<Vec<StateChange>>,
    change_rx: mpsc::Receiver<Vec<StateChange>>,
    user_event_tx: mpsc::Sender<UserEvent>,
    user_event_rx: mpsc::Receiver<UserEvent>,
}

enum LoopControl {
    Continue,
    Break,
}

fn build_channels() -> RuntimeChannels {
    let (block_tx, block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
    let (change_tx, change_rx) = mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);
    let (user_event_tx, user_event_rx) = mpsc::channel::<UserEvent>(USER_EVENT_CHANNEL_CAPACITY);

    RuntimeChannels {
        block_tx,
        block_rx,
        change_tx,
        change_rx,
        user_event_tx,
        user_event_rx,
    }
}

fn spawn_ingestor<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    block_tx: mpsc::Sender<IngestedBlock>,
) -> JoinHandle<()> {
    let ingestor_span = tracing::info_span!("", node_id = context.node_id().0);

    tokio::spawn(
        async move {
            if let Err(e) = BlockIngestor::run(context, block_tx, cancel).await {
                tracing::error!("Ingestor error: {e}");
            }
        }
        .instrument(ingestor_span),
    )
}

fn spawn_fsm<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    block_rx: mpsc::Receiver<IngestedBlock>,
    user_event_rx: mpsc::Receiver<UserEvent>,
    change_tx: mpsc::Sender<Vec<StateChange>>,
) -> JoinHandle<()> {
    let fsm_span = tracing::info_span!("", node_id = context.node_id().0);

    tokio::spawn(
        run_fsm_loop(
            context,
            block_rx,
            user_event_rx,
            change_tx,
            cancel
        )
        .instrument(fsm_span)
    )
}

fn spawn_scheduler<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    change_rx: mpsc::Receiver<Vec<StateChange>>,
    result_rx: mpsc::Receiver<TaskResult>,
    action_tx: mpsc::Sender<Action>,
) -> JoinHandle<()> {
    let scheduler = TaskScheduler::new(context.clone());
    let scheduler_span = tracing::info_span!("", node_id = context.node_id().0);

    tokio::spawn(
        async move {
            scheduler
                .run(change_rx, result_rx, action_tx, cancel)
                .await;
        }
        .instrument(scheduler_span)
    )
}

fn spawn_task_runner<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    action_rx: mpsc::Receiver<Action>,
    result_tx: mpsc::Sender<TaskResult>,
    peer_handle: PeerHandle,
) -> JoinHandle<()> {
    let task_runner = TaskRunner::new(context.clone(), peer_handle, result_tx);
    let task_runner_span = tracing::info_span!("", node_id = context.node_id().0);

    tokio::spawn(
        async move {
            task_runner.run(action_rx, cancel).await;
        }
        .instrument(task_runner_span),
    )
}

fn spawn_peer_service(
    cancel: CancellationToken,
    peer_service: PeerService,
    node_id: NodeId,
) -> JoinHandle<()> {
    let peer_service_span = tracing::info_span!("", node_id = node_id.0);

    tokio::spawn(
        async move {
            peer_service.run(cancel).await;
        }
        .instrument(peer_service_span),
    )
}

fn spawn_http_server<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
    user_event_tx: mpsc::Sender<UserEvent>,
) -> JoinHandle<()> {
    let http_span = tracing::info_span!("", node_id = context.node_id().0);

    tokio::spawn(
        async move {
            let server = HttpServer::new(context, Some(user_event_tx));
            if let Err(e) = server.serve(cancel).await {
                tracing::error!("HTTP server error: {e}");
            }
        }
        .instrument(http_span),
    )
}

/// One-time RPC fetch to seed ChainState on startup.
///
/// Called before spawning FSM/scheduler. If it fails, components start with
/// default state and ChainState is populated on the first EpochAdvanced.
async fn boostrap_chain_state<Db: Store, Cluster: Api, Blockchain: Rpc>(context: &Arc<NodeContext<Db, Cluster, Blockchain>>) {

    let our_bls = match context.bls_keypair.public_key() {
        Ok(pk) => pk,
        Err(e) => {
            tracing::warn!("chain state seed: bls key error: {e:?}");
            return;
        }
    };

    match fetch_chain_state(&context.rpc, &our_bls).await {
        Ok(state) => {
            tracing::info!(
                epoch = state.epoch.0,
                phase = ?state.phase,
                committee_size = state.committee.len(),
                "chain state: seeded from RPC"
            );
            context.chain_state.store(state);
        }
        Err(e) => {
            tracing::warn!(error = %e, "chain state seed failed, starting with defaults");
        }
    }
}

async fn run_fsm_loop<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    mut block_rx: mpsc::Receiver<IngestedBlock>,
    mut user_event_rx: mpsc::Receiver<UserEvent>,
    change_tx: mpsc::Sender<Vec<StateChange>>,
    cancel: CancellationToken,
) {
    let mut fsm = Fsm::new(context.clone());

    loop {
        tokio::select! {
            maybe_block = block_rx.recv() => {
                let Some(block) = maybe_block else { break };
                if let LoopControl::Break = handle_block(
                    &mut fsm, &context, block, &change_tx, &cancel,
                ).await {
                    break;
                }
            }
            maybe_event = user_event_rx.recv() => {
                let Some(event) = maybe_event else { break };
                if let Err(e) = fsm.apply_event(&event) {
                    tracing::error!("FSM user event error: {e}");
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

async fn handle_block<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    fsm: &mut Fsm<Db, Cluster, Blockchain>,
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    block: IngestedBlock,
    change_tx: &mpsc::Sender<Vec<StateChange>>,
    cancel: &CancellationToken,
) -> LoopControl {
    match fsm.apply(&block) {
        Ok(mut changes) => {
            context.stats.inc_blocks();

            if changes.is_empty() {
                return LoopControl::Continue;
            }

            for change in &changes {
                if let StateChange::PhaseAdvanced { phase } = change {
                    context.chain_state.update_phase(*phase);
                    tracing::trace!(?phase, "chain state: phase updated");
                }
            }

            let has_epoch = changes.iter().any(|c| matches!(c, StateChange::EpochAdvanced { .. }));
            if has_epoch {
                if refresh_chain_state(context, cancel).await.is_err() {
                    return LoopControl::Break;
                }

                let cs = context.chain_state.load();
                SpoolPlanner::cleanup_locked(&*context.store, cs.epoch);
                if SpoolPlanner::reconcile_ownership(&*context.store, &cs.spools, cs.epoch) {
                    changes.push(StateChange::SpoolAssignmentChanged);
                }
            }

            if change_tx.send(changes).await.is_err() {
                return LoopControl::Break;
            }
        }
        Err(e) => tracing::error!("FSM error: {e}"),
    }
    LoopControl::Continue
}

/// Fetch chain state from RPC inline, blocking the FSM loop until complete.
///
/// Retries with exponential backoff (500ms → 30s cap) until success or
/// cancellation. Returns `Ok(())` on success or BLS key error (non-fatal),
/// `Err(())` on cancellation.
async fn refresh_chain_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: &CancellationToken,
) -> Result<(), ()> {
    let our_bls = match context.bls_keypair.public_key() {
        Ok(pk) => pk,
        Err(e) => {
            tracing::error!("chain state fetch: bls key error: {e:?}");
            return Ok(());
        }
    };

    match tape_retry::retry(chain_state_backoff(), Some(cancel), || {
        fetch_chain_state(&context.rpc, &our_bls)
    }).await {
        Ok(state) => {
            tracing::info!(
                epoch = state.epoch.0,
                phase = ?state.phase,
                committee_size = state.committee.len(),
                spools = state.spools.len(),
                "chain state: updated from RPC"
            );
            context.chain_state.store(state);
            Ok(())
        }
        Err(_) => {
            tracing::debug!("chain state fetch cancelled");
            Err(())
        }
    }
}

fn chain_state_backoff() -> RetryConfig {
    RetryConfig::infinite()
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use solana_sdk::pubkey::Pubkey;
    use tape_api::event::NodeRegistered;
    use tape_blocks::ParsedInstruction;
    use tape_core::types::{EpochNumber, NodeId, SlotNumber};
    use tape_store::ops::{MetaOps, SpoolOps};
    use tape_store::types::{NodeStatus, SpoolState, SpoolStatus};

    use crate::ingestor::IngestedBlock;
    use crate::state::ChainState;
    use crate::core::PeerService;
    use crate::core::test_utils::test_context;

    async fn spawn_test_fsm<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
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

        // Send a block that produces a StateChange (RegisterNode avoids
        // the refresh_chain_state retry loop that AdvanceEpoch would trigger)
        let node_pk = Pubkey::new_unique();
            let block1 = IngestedBlock {
                slot: SlotNumber(10),
                instructions: vec![ParsedInstruction::RegisterNode {
                    authority: Pubkey::new_unique(),
                    node: node_pk,
                    event: NodeRegistered {
                        node: node_pk,
                        id: NodeId(1),
                        authority: Pubkey::new_unique(),
                        epoch: EpochNumber(0),
                    },
                }],
            };

        block_tx.send(block1).await.unwrap();

        // Receive state changes
        let changes = change_rx.recv().await.unwrap();
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            StateChange::NodeRegistered { .. }
        ));

        // Verify store state
        assert_eq!(
            ctx.store.get_sync_cursor().unwrap(),
            Some(SlotNumber(10))
        );

        // Clean shutdown
        cancel.cancel();
        fsm_handle.await.unwrap();
    }

    #[tokio::test]
    async fn runtime_flow() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Pre-populate spool state so scheduler has work to do
        ctx.store
            .set_spool_state(5, SpoolState { status: SpoolStatus::ActiveSync, epoch: EpochNumber(0) })
            .unwrap();

        // We use spawn_runtime_channels + manual wiring since LiteSvmRpc
        // doesn't produce real blocks with Tapedrive instructions.
        let (change_rx, _user_event_tx, _ingestor_handle, _fsm_handle) =
            spawn_runtime_channels(ctx.clone(), cancel.clone())
                .await;

        let (action_tx, action_rx) =
        mpsc::channel::<Action>(ACTION_CHANNEL_CAPACITY);
        let (result_tx, result_rx) =
            mpsc::channel::<TaskResult>(RESULT_CHANNEL_CAPACITY);

        let scheduler = TaskScheduler::new(ctx.clone());
        let scheduler_cancel = cancel.clone();
        let scheduler_handle = tokio::spawn(async move {
            scheduler
                .run(change_rx, result_rx, action_tx, scheduler_cancel)
                .await;
        });

        let (peer_service, peer_handle) = PeerService::new();
        let peer_cancel = cancel.clone();
        let peer_service_handle = tokio::spawn(async move {
            peer_service.run(peer_cancel).await;
        });
        let task_runner = TaskRunner::new(ctx.clone(), peer_handle, result_tx);
        let task_runner_cancel = cancel.clone();
        let task_runner_handle = tokio::spawn(async move {
            task_runner.run(action_rx, task_runner_cancel).await;
        });

        // Give the runtime startup a moment
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Clean shutdown
        cancel.cancel();
        scheduler_handle.await.unwrap();
        task_runner_handle.await.unwrap();
        peer_service_handle.await.unwrap();
    }

    #[tokio::test]
    async fn ingestor_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5, no cursor → bootstrap needed
        ctx.chain_state.store(ChainState {
            node_status: NodeStatus::Active,
            epoch: EpochNumber(5),
            ..ChainState::default()
        });

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

        // Send a block that produces a StateChange (RegisterNode avoids
        // the refresh_chain_state retry loop that AdvanceEpoch would trigger)
        let node_pk = Pubkey::new_unique();
            let block = IngestedBlock {
                slot: SlotNumber(10),
                instructions: vec![ParsedInstruction::RegisterNode {
                    authority: Pubkey::new_unique(),
                    node: node_pk,
                    event: NodeRegistered {
                        node: node_pk,
                        id: NodeId(1),
                        authority: Pubkey::new_unique(),
                        epoch: EpochNumber(0),
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
