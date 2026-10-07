use tape_store_api::Store;
use tape_blocks::capture_block;
use tape_core::encoding::ErasureCode;
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::types::EpochNumber;
use tape_store::ops::{EventLogOps, MetaOps};
use tape_store::TapeStore;
use tracing::debug;

use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;
use crate::features::store::apply::{apply_event, DeletePolicy};
use crate::features::store::manager::persist_batch;

pub type ReplayPersistFn<Db> =
    fn(&TapeStore<Db>, &ReplayBatch, ErasureCode) -> Result<(), NodeError>;

pub struct ReplayEngine<'a, Db: Store> {
    store: &'a TapeStore<Db>,
    current_epoch: EpochNumber,
    // The network's profile: how wide a commitment is and how big a slice gets.
    profile: ErasureCode,
}

impl<'a, Db: Store> ReplayEngine<'a, Db> {
    pub fn new(store: &'a TapeStore<Db>, current_epoch: EpochNumber, profile: ErasureCode) -> Self {
        Self {
            store,
            current_epoch,
            profile,
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
            self.profile,
        )?;

        for entry in &captured.events {
            self.store
                .append_record(entry.epoch, block.slot, block.block_time, &entry.record)
                .map_err(store_error)?;
        }

        // The epoch only moves on a block that holds the advance.
        let advanced_epoch =
            (captured.next_epoch != self.current_epoch).then_some(captured.next_epoch);
        self.current_epoch = captured.next_epoch;

        let batch = ReplayBatch {
            slot: block.slot,
            blockhash: block.blockhash,
            block_time: block.block_time,
            records: captured.events.into_iter().map(|entry| entry.record).collect(),
            raw_tracks: captured.raw_tracks,
            advanced_epoch,
        };
        let event_count = batch.records.len();

        Ok((batch, event_count))
    }

    pub fn apply_block(&mut self, block: &ParsedBlock) -> Result<usize, NodeError> {
        self.apply_block_with(block, persist_batch::<Db>)
    }

    pub fn apply_block_with(
        &mut self,
        block: &ParsedBlock,
        persist: ReplayPersistFn<Db>,
    ) -> Result<usize, NodeError> {
        let (batch, event_count) = self.capture_and_journal(block)?;
        persist(self.store, &batch, self.profile)?;
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

        // A snapshot log is a closed epoch, so no round still counts its deletes.
        for entry in &log.entries {
            for record in &entry.records {
                apply_event(
                    self.store,
                    entry.slot,
                    entry.block_time,
                    &record.event,
                    self.profile,
                    DeletePolicy::AtOnce,
                )?;
            }
        }

        self.current_epoch = log.epoch;
        self.store
            .set_sync_cursor(log.end_slot, None)
            .map_err(|error| NodeError::Store(format!("set_sync_cursor: {error}")))?;

        Ok(())
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::event::EpochAdvanced;
    use tape_blocks::ParsedInstruction;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::types::SlotNumber;
    use tape_crypto::tx::Txid;
    use tape_crypto::Hash;
    use tape_store_memory::MemoryStore;

    use super::*;

    fn block(slot: u64, instructions: Vec<ParsedInstruction>) -> ParsedBlock {
        ParsedBlock {
            slot: SlotNumber(slot),
            parent_slot: SlotNumber(slot - 1),
            blockhash: Hash::default(),
            previous_blockhash: Hash::default(),
            block_time: None,
            instruction_tx_ids: vec![Txid::default(); instructions.len()],
            instructions,
        }
    }

    // only the block that holds the advance carries the new epoch
    #[test]
    fn stamps_the_advance_block() {
        let store = TapeStore::new(MemoryStore::new());
        let mut engine = ReplayEngine::new(&store, EpochNumber(4), TAPENET_PROFILE);

        let (quiet, _) = engine.capture_and_journal(&block(10, Vec::new())).expect("capture");
        assert_eq!(quiet.advanced_epoch, None);

        let advance = ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch: EpochNumber(4),
                new_epoch: EpochNumber(5),
                ..Zeroable::zeroed()
            },
        };
        let (turned, _) = engine
            .capture_and_journal(&block(11, vec![advance]))
            .expect("capture");
        assert_eq!(turned.advanced_epoch, Some(EpochNumber(5)));

        let (after, _) = engine.capture_and_journal(&block(12, Vec::new())).expect("capture");
        assert_eq!(after.advanced_epoch, None);
    }
}
