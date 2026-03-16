use std::sync::Arc;

use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_store::ops::MetaOps;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_core::system::EpochPhase;

use crate::core::NodeContext;
use crate::ingestor::IngestedBlock;

use super::{RuntimeEvent, Fsm, FsmError, StateChange};

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Fsm<Db, Cluster, Blockchain> {
    pub fn new(context: Arc<NodeContext<Db, Cluster, Blockchain>>) -> Self {
        Self {
            context,
            state: super::FsmState {
                epoch: EpochNumber(0),
                phase: EpochPhase::Unknown,
            },
        }
    }

    /// Apply a single ingested block to local state.
    ///
    /// Returns the state changes produced, which the scheduler uses to
    /// determine what tasks to schedule or cancel.
    pub fn apply(&mut self, block: &IngestedBlock) -> Result<Vec<StateChange>, FsmError> {
        tracing::trace!(
            slot = %block.slot,
            instruction_count = block.instructions.len(),
            "fsm applying block"
        );

        let mut changes = Vec::new();
        let mut current_epoch = self.state.epoch;

        for instruction in &block.instructions {
            tracing::trace!(
                slot = %block.slot,
                epoch = current_epoch.0,
                instruction = ?instruction,
                "fsm applying instruction"
            );

            let before_len = changes.len();
            self.apply_instruction(
                instruction,
                block.slot,
                &mut changes,
                &mut current_epoch,
            )?;

            let added = changes.len() - before_len;
            if added > 0 {
                tracing::trace!(
                    slot = %block.slot,
                    epoch = current_epoch.0,
                    added,
                    "fsm emitted state change"
                );
            }
        }

        self.context.store.set_sync_cursor(block.slot)?;

        tracing::trace!(
            slot = %block.slot,
            change_count = changes.len(),
            "fsm finished block apply"
        );
        Ok(changes)
    }

    /// Apply a runtime event (e.g. slice accepted by HTTP handler).
    pub fn apply_event(&self, event: &RuntimeEvent) -> Result<(), FsmError> {
        match event {
            RuntimeEvent::SliceAccepted { .. } => Ok(()),
        }
    }

    /// Apply a parsed instruction (e.g. AdvanceEpoch)
    pub fn apply_instruction(
        &mut self,
        instruction: &ParsedInstruction,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: &mut EpochNumber,
    ) -> Result<(), FsmError> {
        match instruction {
            ParsedInstruction::AdvanceEpoch { event } => {
                self.handle_advance_epoch(event, slot, changes, current_epoch)
            }
            ParsedInstruction::SyncEpoch { event } => {
                self.handle_sync_epoch(event, slot, changes, *current_epoch)
            }
            ParsedInstruction::RegisterTrack { track, event, .. } => {
                self.handle_register_track(*track, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::CertifyTrack { track, event } => {
                self.handle_certify_track(*track, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::DeleteTrack { track, event, .. } => {
                self.handle_delete_track(*track, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::InvalidateTrack { track, event } => {
                self.handle_invalidate_track(*track, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::ReserveTape { tape, event, .. } => {
                self.handle_reserve_tape(*tape, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::DestroyTape { tape, event, .. } => {
                self.handle_destroy_tape(*tape, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::RegisterNode { authority, node, event, } => {
                self.handle_register_node(*authority, *node, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::JoinNetwork { node, event } => {
                self.handle_join_network(*node, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::AdvancePool { node, event } => {
                self.handle_advance_pool(*node, event, changes)
            }
        }
    }
}
