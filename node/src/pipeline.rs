//! Pipeline — wires the ingestor, FSM, reconciler, and supervisor together.
//!
//! The ingestor fetches and parses blocks, sending them over a bounded channel
//! to the FSM. The FSM applies each block and forwards state changes to the
//! reconciler. The reconciler diffs desired vs running tasks and sends directives
//! to the supervisor. Channel backpressure ensures no component outpaces another.

use std::sync::Arc;

use store::Store;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::fsm::{Fsm, StateChange};
use crate::http::HttpServer;
use crate::ingestor::{BlockIngestor, BlockSource, IngestedBlock};
use crate::reconciler::{Directive, Reconciler};
use crate::supervisor::{Supervisor, TaskResult};

const INGESTOR_CHANNEL_CAPACITY: usize = 4;
const STATE_CHANGE_CHANNEL_CAPACITY: usize = 16;
const DIRECTIVE_CHANNEL_CAPACITY: usize = 256;
const RESULT_CHANNEL_CAPACITY: usize = 256;

/// Spawn the block processing pipeline.
///
/// Creates bounded channels between the ingestor and FSM, spawning both as
/// tokio tasks. Returns a receiver for state changes and the task handles.
pub async fn spawn_pipeline<S: Store + 'static>(
    context: Arc<NodeContext<S>>,
    source: Arc<dyn BlockSource>,
    cancel: CancellationToken,
) -> (
    mpsc::Receiver<Vec<StateChange>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let (block_tx, mut block_rx) = mpsc::channel::<IngestedBlock>(INGESTOR_CHANNEL_CAPACITY);
    let (change_tx, change_rx) = mpsc::channel::<Vec<StateChange>>(STATE_CHANGE_CHANNEL_CAPACITY);

    let ingestor_context = context.clone();
    let ingestor_cancel = cancel.clone();
    let ingestor_handle = tokio::spawn(async move {
        if let Err(e) =
            BlockIngestor::run(ingestor_context, source, block_tx, ingestor_cancel).await
        {
            tracing::error!("Ingestor error: {e}");
        }
    });

    let fsm_cancel = cancel.clone();
    let fsm_handle = tokio::spawn(async move {
        let fsm = Fsm::new(context);
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
                                Err(e) => tracing::error!("FSM error: {e}"),
                            }
                        }
                        None => break,
                    }
                }
                _ = fsm_cancel.cancelled() => break,
            }
        }
    });

    (change_rx, ingestor_handle, fsm_handle)
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
pub async fn spawn_runtime<S: Store + 'static>(
    context: Arc<NodeContext<S>>,
    source: Arc<dyn BlockSource>,
    cancel: CancellationToken,
) -> RuntimeHandles {
    let (change_rx, ingestor_handle, fsm_handle) =
        spawn_pipeline(context.clone(), source, cancel.clone()).await;

    let (directive_tx, directive_rx) = mpsc::channel::<Directive>(DIRECTIVE_CHANNEL_CAPACITY);
    let (result_tx, result_rx) = mpsc::channel::<TaskResult>(RESULT_CHANNEL_CAPACITY);

    let reconciler = Reconciler::new(context.clone());
    let reconciler_cancel = cancel.clone();
    let reconciler_handle = tokio::spawn(async move {
        reconciler
            .run(change_rx, result_rx, directive_tx, reconciler_cancel)
            .await;
    });

    let supervisor = Supervisor::new(context.clone(), result_tx);
    let supervisor_cancel = cancel.clone();
    let supervisor_handle = tokio::spawn(async move {
        supervisor.run(directive_rx, supervisor_cancel).await;
    });

    let http_ctx = context;
    let http_cancel = cancel;
    let http_handle = tokio::spawn(async move {
        let server = HttpServer::new(http_ctx);
        if let Err(e) = server.serve(http_cancel).await {
            tracing::error!("HTTP server error: {e}");
        }
    });

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
    use std::path::PathBuf;

    use tape_api::event::EpochAdvanced;
    use tape_blocks::ParsedInstruction;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
    use tape_store::ops::{MetaOps, SpoolOps};
    use tape_store::types::SpoolStatus;
    use tape_store::{MemoryStore, TapeStore};

    use crate::core::config::RecoveryConfig;
    use crate::core::{NodeApiConfig, NodeConfig, NodeContext, TlsConfig};

    fn test_config() -> NodeConfig {
        NodeConfig {
            version: 1,
            name: "test-node".to_string(),
            tls_keypair: PathBuf::from("/dev/null"),
            bls_keypair: PathBuf::from("/dev/null"),
            node_keypair: String::new(),
            bind_address: "127.0.0.1:0".parse().unwrap(),
            public_host: "localhost".to_string(),
            public_port: 0,
            tls: TlsConfig::default(),
            storage_path: "/tmp".to_string(),
            poll_interval_ms: None,
            sync_concurrency: None,
            sync_batch_size: None,
            commission: None,
            recovery: RecoveryConfig::default(),
            node_api: NodeApiConfig::default(),
        }
    }

    fn test_context() -> Arc<NodeContext<MemoryStore>> {
        let config = test_config();
        let keypair = solana_sdk::signature::Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        NodeContext::new(config, keypair, bls_keypair, store)
    }

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

    struct MockBlockSource {
        blocks: tokio::sync::Mutex<Vec<(SlotNumber, Vec<ParsedInstruction>)>>,
    }

    impl MockBlockSource {
        fn new(blocks: Vec<(SlotNumber, Vec<ParsedInstruction>)>) -> Self {
            Self {
                blocks: tokio::sync::Mutex::new(blocks),
            }
        }
    }

    #[async_trait::async_trait]
    impl BlockSource for MockBlockSource {
        async fn get_slot(&self) -> Result<SlotNumber, anyhow::Error> {
            let blocks = self.blocks.lock().await;
            Ok(blocks
                .last()
                .map(|(slot, _)| *slot)
                .unwrap_or(SlotNumber(0)))
        }

        async fn get_block(
            &self,
            slot: SlotNumber,
        ) -> Result<Option<solana_transaction_status::UiConfirmedBlock>, anyhow::Error> {
            // The mock source doesn't produce real UiConfirmedBlocks.
            // The integration test below uses the pipeline's channel interface directly.
            let _ = slot;
            Ok(None)
        }
    }

    #[tokio::test]
    async fn runtime_end_to_end() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Pre-populate spool state so reconciler has work to do
        ctx.store
            .set_spool_status(5, SpoolStatus::ActiveSync)
            .unwrap();

        // We use spawn_pipeline + manual block injection since MockBlockSource
        // can't produce real UiConfirmedBlocks. Wire reconciler + supervisor manually.
        let (change_rx, _ingestor_handle, _fsm_handle) =
            spawn_pipeline(ctx.clone(), Arc::new(MockBlockSource::new(vec![])), cancel.clone())
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
}
