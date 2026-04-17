use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::snapshot::replay::ReplayableEvent;
use tape_core::types::{EpochNumber, NodeId, SlotNumber};
use tape_crypto::hash::hash;
use tape_protocol::Api;
use tape_store::ops::EventLogOps;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::core::channels::send_replay_batch;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::capture::capture_block;
use crate::features::replay::types::ReplayBatch;

pub struct ReplayManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    store_tx: mpsc::Sender<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> ReplayManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        store_tx: mpsc::Sender<ReplayBatch>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            rx,
            store_tx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            "replay manager started"
        );

        let mut current_epoch = self.context.state().epoch;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::ReplayManager })
                        };
                    };

                    current_epoch = self.persist_block(current_epoch, block).await?;
                }
            }
        }
    }

    async fn persist_block(
        &self,
        current_epoch: EpochNumber,
        block: Arc<ParsedBlock>,
    ) -> Result<EpochNumber, NodeError> {
        let captured = capture_block(current_epoch, &block)?;

        let node_id = self.context.node_id();
        for (pos, entry) in captured.events.iter().enumerate() {
            self.context
                .store
                .append_event(entry.epoch, block.slot, &entry.event)
                .map_err(store_error)?;

            debug_journal(node_id, entry.epoch, block.slot, pos, &entry.event);
        }

        let next_epoch = captured.next_epoch;
        let batch = captured.into_batch(block.slot);
        let event_count = batch.events.len();

        send_replay_batch(&self.store_tx, batch).await?;
        self.context.metrics.add_events(event_count as u64);

        debug!(
            node_id = self.context.node_id().0,
            slot = block.slot.0,
            journaled = event_count,
            next_epoch = next_epoch.0,
            "replay journal persisted"
        );

        Ok(next_epoch)
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

/// Write one TSV line per replayable event to `/tmp/tapedrive-snap/node-{id}.log`.
///
/// Debug-only journal used to diff per-node event streams when snapshot
/// `value_hash` mismatches appear. Line format:
///
///   {epoch}\t{slot}\t{pos}\t{variant}\t{blake3_hex}
///
/// Diff the per-node files to locate the first divergent event. Remove this
/// once the investigation is complete.
fn debug_journal(
    node_id: NodeId,
    epoch: EpochNumber,
    slot: SlotNumber,
    pos: usize,
    event: &ReplayableEvent,
) {
    let dir = "/tmp/tapedrive-snap";
    if let Err(error) = std::fs::create_dir_all(dir) {
        warn!(?error, dir, "replay debug journal: mkdir failed");
        return;
    }

    let path = format!("{dir}/node-{}.log", node_id.0);

    let serialized = match wincode::serialize(event) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(?error, "replay debug journal: serialize failed");
            return;
        }
    };
    let digest = hash(&serialized);
    let digest_hex = digest
        .to_bytes()
        .iter()
        .fold(String::with_capacity(64), |mut acc, byte| {
            use std::fmt::Write;
            let _ = write!(&mut acc, "{:02x}", byte);
            acc
        });
    let variant = variant_name(event);

    let line = format!(
        "{}\t{}\t{}\t{}\t{}\n",
        epoch.0, slot.0, pos, variant, digest_hex
    );

    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(line.as_bytes()));

    if let Err(error) = result {
        warn!(?error, path, "replay debug journal: write failed");
    }
}

fn variant_name(event: &ReplayableEvent) -> &'static str {
    match event {
        ReplayableEvent::Track(_) => "Track",
        ReplayableEvent::CertifyTrack { .. } => "CertifyTrack",
        ReplayableEvent::DeleteTrack { .. } => "DeleteTrack",
        ReplayableEvent::InvalidateTrack { .. } => "InvalidateTrack",
        ReplayableEvent::AdvanceEpoch { .. } => "AdvanceEpoch",
        ReplayableEvent::SyncEpoch { .. } => "SyncEpoch",
        ReplayableEvent::ReserveTape { .. } => "ReserveTape",
        ReplayableEvent::DestroyTape { .. } => "DestroyTape",
        ReplayableEvent::RegisterNode { .. } => "RegisterNode",
        ReplayableEvent::JoinNetwork { .. } => "JoinNetwork",
    }
}
