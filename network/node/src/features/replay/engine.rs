use store::Store;
use tape_blocks::capture_block;
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::types::EpochNumber;
use tape_store::ops::{EventLogOps, MetaOps};
use tape_store::TapeStore;
use tracing::debug;

use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;
use crate::features::store::apply::apply_event;
use crate::features::store::manager::persist_batch;

pub struct ReplayEngine<'a, Db: Store> {
    store: &'a TapeStore<Db>,
    current_epoch: EpochNumber,
}

impl<'a, Db: Store> ReplayEngine<'a, Db> {
    pub fn new(store: &'a TapeStore<Db>, current_epoch: EpochNumber) -> Self {
        Self {
            store,
            current_epoch,
        }
    }

    pub fn current_epoch(&self) -> EpochNumber {
        self.current_epoch
    }

    pub fn set_current_epoch(&mut self, epoch: EpochNumber) {
        self.current_epoch = epoch;
    }

    pub fn capture_and_journal(
        &mut self,
        block: &ParsedBlock,
    ) -> Result<(ReplayBatch, usize), NodeError> {

        let captured = capture_block(
            self.current_epoch,
            block.slot,
            &block.instructions,
            &block.instruction_tx_ids,
        )?;

        for entry in &captured.events {
            self.store
                .append_record(entry.epoch, block.slot, block.block_time, &entry.record)
                .map_err(store_error)?;
        }

        self.current_epoch = captured.next_epoch;

        let batch = ReplayBatch {
            slot: block.slot,
            block_time: block.block_time,
            records: captured.events.into_iter().map(|entry| entry.record).collect(),
            raw_tracks: captured.raw_tracks,
        };
        let event_count = batch.records.len();

        Ok((batch, event_count))
    }

    pub fn apply_block(&mut self, block: &ParsedBlock) -> Result<usize, NodeError> {
        let (batch, event_count) = self.capture_and_journal(block)?;
        persist_batch(self.store, &batch)?;
        Ok(event_count)
    }

    pub fn apply_snapshot_log(&mut self, log: &SnapshotLog) -> Result<(), NodeError> {
        let event_count: usize = log.entries.iter().map(|e| e.records.len()).sum();
        debug!(
            epoch = log.epoch.0,
            entries = log.entries.len(),
            events = event_count,
            start_slot = log.start_slot.0,
            end_slot = log.end_slot.0,
            "bootstrap: applying snapshot log"
        );

        for entry in &log.entries {
            for record in &entry.records {
                apply_event(self.store, entry.slot, entry.block_time, &record.event)?;
            }
        }

        self.current_epoch = log.epoch;
        self.store
            .set_sync_cursor(log.end_slot)
            .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))?;

        Ok(())
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
