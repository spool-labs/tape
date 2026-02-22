//! Finite state machine — applies parsed instructions to local state.
//!
//! The FSM is the single writer to the local store. It receives `IngestedBlock`
//! batches from the ingestor, applies each instruction using the ops traits,
//! and emits `Vec<StateChange>` to the scheduler.
//!
//! Crash consistency: the sync cursor is updated LAST after all instructions in
//! a block are applied. If we crash mid-block, the cursor hasn't advanced, so
//! the entire block is re-processed on restart. All handlers are idempotent.

use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use store::Store;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, TapeDestroyed, TapeReserved,
    TrackCertified, TrackDeleted, TrackInvalidated, TrackRegistered,
};
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::{ReplayableEvent, SnapshotLog};
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_core::erasure::spool_in_group;
use tape_store::error::TapeStoreError;
use tape_store::ops::{
    CommitteeOps, EventLogOps, MetaOps, ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps,
};
use tape_store::types::{ObjectInfo, Pubkey as StorePubkey, TapeInfo, TrackInfo};

use crate::core::NodeContext;
use crate::core::committee::our_member_index;
use crate::ingestor::IngestedBlock;

#[derive(Debug, thiserror::Error)]
pub enum FsmError {
    #[error("store error: {0}")]
    Store(#[from] TapeStoreError),
}

/// A state change emitted by the FSM after applying instructions.
///
/// The scheduler consumes these to determine which tasks to schedule.
#[derive(Debug, Clone)]
pub enum StateChange {
    EpochAdvanced { epoch: EpochNumber },
    SpoolAssignmentChanged,
    TrackRegistered { track: Pubkey },
    TrackCertified { track: Pubkey },
    TrackDeleted { track: Pubkey },
    TrackInvalidated { track: Pubkey },
    TapeReserved { tape: Pubkey },
    TapeDestroyed { tape: Pubkey },
    NodeRegistered { node: Pubkey },
    NodeJoinedCommittee { node: Pubkey },
    NodeSynced { node: Pubkey },
}

/// An event from user-facing HTTP handlers, forwarded to the FSM.
#[derive(Debug)]
pub enum UserEvent {
    SliceAccepted { track: Pubkey, spool: u16 },
}

/// Single-writer state machine that processes blocks and updates local storage.
pub struct Fsm<S: Store, R: Rpc> {
    context: Arc<NodeContext<S, R>>,
}

impl<S: Store, R: Rpc> Fsm<S, R> {
    pub fn new(context: Arc<NodeContext<S, R>>) -> Self {
        Self { context }
    }

    /// Apply a user event (e.g. slice accepted by HTTP handler).
    pub fn apply_user_event(&self, event: &UserEvent) -> Result<(), FsmError> {
        match event {
            UserEvent::SliceAccepted { track, .. } => {
                let key: StorePubkey = (*track).into();
                let Some(obj) = self.context.store.get_object_info(key)? else {
                    return Ok(());
                };
                if let ObjectInfo::Valid {
                    is_stored: false,
                    track_address,
                    registered_epoch,
                    certified_epoch,
                    slot,
                } = obj
                {
                    self.context.store.put_object_info(
                        key,
                        ObjectInfo::Valid {
                            is_stored: true,
                            track_address,
                            registered_epoch,
                            certified_epoch,
                            slot,
                        },
                    )?;
                }
                Ok(())
            }
        }
    }

    /// Apply a single ingested block to local state.
    ///
    /// Returns the state changes produced, which the scheduler uses to
    /// determine what tasks to schedule or cancel.
    pub fn apply(&self, block: &IngestedBlock) -> Result<Vec<StateChange>, FsmError> {
        tracing::trace!(
            slot = %block.slot,
            instruction_count = block.instructions.len(),
            "fsm applying block"
        );
        let mut changes = Vec::new();
        let mut current_epoch = self
            .context
            .store
            .get_chain_epoch()?
            .unwrap_or(EpochNumber(0));
        for instruction in &block.instructions {
            tracing::trace!(
                slot = %block.slot,
                epoch = current_epoch.0,
                instruction = ?instruction,
                "fsm applying instruction"
            );
            let before_len = changes.len();
            self.apply_instruction(instruction, block.slot, &mut changes, &mut current_epoch)?;
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
        // Update sync cursor LAST — crash recovery re-processes from cursor
        self.context.store.set_sync_cursor(block.slot)?;
        tracing::trace!(
            slot = %block.slot,
            change_count = changes.len(),
            "fsm finished block apply"
        );
        Ok(changes)
    }

    fn apply_instruction(
        &self,
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
            ParsedInstruction::RegisterTrack { track, event, .. } => self.apply_opt(event, |ev| {
                self.handle_register_track(*track, ev, slot, changes, *current_epoch)
            }),
            ParsedInstruction::CertifyTrack { track, event } => {
                self.handle_certify_track(*track, event, slot, changes, *current_epoch)
            }
            ParsedInstruction::DeleteTrack { track, event, .. } => self.apply_opt(event, |ev| {
                self.handle_delete_track(*track, ev, slot, changes, *current_epoch)
            }),
            ParsedInstruction::InvalidateTrack { track, event } => self.apply_opt(event, |ev| {
                self.handle_invalidate_track(*track, ev, slot, changes, *current_epoch)
            }),
            ParsedInstruction::ReserveTape { tape, event, .. } => self.apply_opt(event, |ev| {
                self.handle_reserve_tape(*tape, ev, slot, changes, *current_epoch)
            }),
            ParsedInstruction::DestroyTape { tape, event, .. } => self.apply_opt(event, |ev| {
                self.handle_destroy_tape(*tape, ev, slot, changes, *current_epoch)
            }),
            ParsedInstruction::RegisterNode {
                authority,
                node,
                event,
            } => self.apply_opt(event, |ev| {
                self.handle_register_node(*authority, *node, ev, slot, changes, *current_epoch)
            }),
            ParsedInstruction::JoinNetwork { node, event } => {
                self.apply_opt(event, |ev| self.handle_join_network(*node, ev, slot, changes, *current_epoch))
            }
        }
    }

    fn apply_opt<T, F>(&self, event: &Option<T>, apply: F) -> Result<(), FsmError>
    where
        F: FnOnce(&T) -> Result<(), FsmError>,
    {
        if let Some(ev) = event {
            return apply(ev);
        }
        Ok(())
    }

    fn track_info(&self, event: &TrackRegistered) -> TrackInfo {
        let mut info = TrackInfo {
            tape_address: event.tape.into(),
            spool_group: u64::from_le_bytes(event.spool_group),
            original_size: event.size.0,
            stripe_size: u64::from_le_bytes(event.stripe_size),
            stripe_count: u64::from_le_bytes(event.stripe_count),
            encoding_type: 0,
            encoding_params: 0,
            commitment: event.leaves.to_vec(),
        };
        info.set_profile(event.profile);
        info
    }

    fn put_track_obj(
        &self,
        track: StorePubkey,
        event: &TrackRegistered,
        slot: SlotNumber,
    ) -> Result<(), FsmError> {
        self.context.store.put_track(track, self.track_info(event))?;
        self.context.store.put_object_info(
            track,
            ObjectInfo::Valid {
                is_stored: false,
                track_address: track,
                registered_epoch: event.epoch,
                certified_epoch: None,
                slot,
            },
        )?;
        Ok(())
    }

    fn set_certified(&self, track: StorePubkey, epoch: EpochNumber) -> Result<(), FsmError> {
        let Some(obj) = self.context.store.get_object_info(track)? else {
            return Ok(());
        };
        if let ObjectInfo::Valid {
            is_stored,
            track_address,
            registered_epoch,
            slot,
            ..
        } = obj
        {
            self.context.store.put_object_info(
                track,
                ObjectInfo::Valid {
                    is_stored,
                    track_address,
                    registered_epoch,
                    certified_epoch: Some(epoch),
                    slot,
                },
            )?;
        }
        Ok(())
    }

    fn handle_advance_epoch(
        &self,
        event: &EpochAdvanced,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: &mut EpochNumber,
    ) -> Result<(), FsmError> {
        let old_epoch = *current_epoch;
        self.context.store.set_chain_epoch(event.new_epoch)?;
        self.context.store.set_chain_epoch_phase(EpochPhase::Syncing)?;
        self.context.store.set_epoch_nonce(event.new_epoch, event.nonce)?;
        self.context
            .store
            .set_epoch_start_ts(event.new_epoch, i64::from_le_bytes(event.timestamp))?;
        *current_epoch = event.new_epoch;

        // GC expired tapes (end_epoch <= new_epoch)
        self.gc_expired_tapes(event.new_epoch)?;

        self.context.store.append_event(
            event.new_epoch,
            slot,
            &ReplayableEvent::AdvanceEpoch {
                old_epoch,
                new_epoch: event.new_epoch,
            },
        )?;

        self.context.stats.inc_epochs();
        self.log_member_index_for_epoch(event.new_epoch, "ingest");
        changes.push(StateChange::EpochAdvanced {
            epoch: event.new_epoch,
        });
        Ok(())
    }

    fn log_member_index_for_epoch(&self, epoch: EpochNumber, source: &str) {
        let committee = match self.context.store.get_committee(epoch).ok().flatten() {
            Some(committee) => committee,
            None => {
                tracing::warn!(
                    source = source,
                    epoch = epoch.0,
                    "cannot resolve committee when logging member index"
                );
                return;
            }
        };

        match our_member_index(&committee, self.context.keypair.pubkey()) {
            Ok(member_index) => {
                tracing::info!(
                    source = source,
                    epoch = epoch.0,
                    member_index,
                    committee_size = committee.len(),
                    "node member index for epoch"
                );
            }
            Err(error) => {
                tracing::warn!(
                    source = source,
                    epoch = epoch.0,
                    error = %error,
                    "node not found in committee for epoch"
                );
            }
        }
    }

    fn handle_sync_epoch(
        &self,
        event: &NodeSynced,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::SyncEpoch {
                node: event.node.to_bytes(),
                node_id: event.id,
                epoch: event.epoch,
                spools_hash: event.spools_hash,
            },
        )?;

        changes.push(StateChange::NodeSynced { node: event.node });
        Ok(())
    }

    fn handle_register_track(
        &self,
        track: Pubkey,
        event: &TrackRegistered,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        self.put_track_obj(track.into(), event, slot)?;

        let event_data = bytemuck::bytes_of(event).to_vec();
        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::RegisterTrack {
                track: track.to_bytes(),
                event_data,
            },
        )?;

        changes.push(StateChange::TrackRegistered { track });
        Ok(())
    }

    fn handle_certify_track(
        &self,
        track: Pubkey,
        event: &TrackCertified,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        self.set_certified(track.into(), event.epoch)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::CertifyTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            },
        )?;

        changes.push(StateChange::TrackCertified { track });
        Ok(())
    }

    fn handle_delete_track(
        &self,
        track: Pubkey,
        _event: &TrackDeleted,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let store_track: StorePubkey = track.into();

        // Read track info before deleting (need spool_group for slice cleanup)
        if let Ok(Some(info)) = self.context.store.get_track(store_track) {
            self.cleanup_slices_for_track(store_track, info.spool_group)?;
        }

        self.context.store.delete_track(store_track)?;
        self.context.store.delete_object_info(store_track)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::DeleteTrack {
                track: track.to_bytes(),
                epoch: current_epoch,
            },
        )?;

        changes.push(StateChange::TrackDeleted { track });
        Ok(())
    }

    fn cleanup_slices_for_track(
        &self,
        track: StorePubkey,
        spool_group: u64,
    ) -> Result<(), FsmError> {
        let owned_spools = self.context.store.iter_all_spools()?;
        for (spool_id, _status) in &owned_spools {
            if spool_in_group(*spool_id, spool_group) {
                let _ = self.context.store.delete_slice(*spool_id, track);
            }
        }
        Ok(())
    }

    fn cascade_delete_tape_tracks(
        &self,
        tape: StorePubkey,
    ) -> Result<(), FsmError> {
        let mut cursor = None;
        loop {
            let tracks = self.context.store.iter_tracks_from(cursor, 100)?;
            if tracks.is_empty() {
                break;
            }
            for (track_addr, track_info) in &tracks {
                if track_info.tape_address == tape {
                    self.cleanup_slices_for_track(*track_addr, track_info.spool_group)?;
                    self.context.store.delete_track(*track_addr)?;
                    self.context.store.delete_object_info(*track_addr)?;
                }
            }
            cursor = tracks.last().map(|(addr, _)| *addr);
        }
        Ok(())
    }

    fn gc_expired_tapes(&self, current_epoch: EpochNumber) -> Result<(), FsmError> {
        let tapes = self.context.store.iter_all_tapes()?;
        for (tape_addr, tape_info) in &tapes {
            if tape_info.end_epoch <= current_epoch {
                self.cascade_delete_tape_tracks(*tape_addr)?;
                self.context.store.delete_tape(*tape_addr)?;
            }
        }
        Ok(())
    }

    fn handle_invalidate_track(
        &self,
        track: Pubkey,
        event: &TrackInvalidated,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let store_track: StorePubkey = track.into();

        // Delete slices before marking invalid
        if let Ok(Some(info)) = self.context.store.get_track(store_track) {
            self.cleanup_slices_for_track(store_track, info.spool_group)?;
        }

        let invalid = ObjectInfo::Invalid {
            epoch: event.epoch,
            slot,
        };
        self.context.store.put_object_info(store_track, invalid)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::InvalidateTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            },
        )?;

        changes.push(StateChange::TrackInvalidated { track });
        Ok(())
    }

    fn handle_reserve_tape(
        &self,
        tape: Pubkey,
        event: &TapeReserved,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let tape_info = TapeInfo {
            end_epoch: event.expiry_epoch,
        };
        self.context.store.put_tape(tape.into(), tape_info)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::ReserveTape {
                tape: tape.to_bytes(),
                authority: event.authority.to_bytes(),
                active_epoch: event.active_epoch,
                expiry_epoch: event.expiry_epoch,
            },
        )?;

        changes.push(StateChange::TapeReserved { tape });
        Ok(())
    }

    fn handle_destroy_tape(
        &self,
        tape: Pubkey,
        _event: &TapeDestroyed,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let store_tape: StorePubkey = tape.into();

        // Cascade-delete all tracks belonging to this tape
        self.cascade_delete_tape_tracks(store_tape)?;

        self.context.store.delete_tape(store_tape)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::DestroyTape {
                tape: tape.to_bytes(),
                epoch: current_epoch,
            },
        )?;

        changes.push(StateChange::TapeDestroyed { tape });
        Ok(())
    }

    fn handle_register_node(
        &self,
        authority: Pubkey,
        node: Pubkey,
        _event: &NodeRegistered,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::RegisterNode {
                authority: authority.to_bytes(),
                node: node.to_bytes(),
            },
        )?;

        changes.push(StateChange::NodeRegistered { node });
        Ok(())
    }

    fn handle_join_network(
        &self,
        node: Pubkey,
        _event: &NodeJoinedCommittee,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        self.context.store.append_event(
            current_epoch,
            slot,
            &ReplayableEvent::JoinNetwork {
                node: node.to_bytes(),
            },
        )?;

        changes.push(StateChange::NodeJoinedCommittee { node });
        Ok(())
    }

    /// Replay a snapshot log into local state.
    ///
    /// This applies the same store operations as the live FSM handlers but
    /// skips event log writes and StateChange emission. Used by
    /// SnapshotBootstrap after downloading and decoding a snapshot.
    pub fn replay_snapshot(&self, log: &SnapshotLog) -> Result<(), FsmError> {
        for entry in &log.entries {
            for event in &entry.events {
                self.apply_replay_event(event, entry.slot)?;
            }
        }
        self.context.store.set_sync_cursor(log.end_slot)?;
        Ok(())
    }

    fn apply_replay_event(
        &self,
        event: &ReplayableEvent,
        slot: SlotNumber,
    ) -> Result<(), FsmError> {
        match event {
            ReplayableEvent::AdvanceEpoch { new_epoch, .. } => {
                self.context.store.set_chain_epoch(*new_epoch)?;
                self.context
                    .store
                    .set_chain_epoch_phase(EpochPhase::Unknown)?;
                self.log_member_index_for_epoch(*new_epoch, "bootstrap-replay");
            }
            ReplayableEvent::RegisterTrack { track, event_data } => {
                let track_key: StorePubkey = Pubkey::new_from_array(*track).into();
                let event: &TrackRegistered = bytemuck::from_bytes(event_data);
                self.put_track_obj(track_key, event, slot)?;
            }
            ReplayableEvent::CertifyTrack { track, epoch } => {
                let track_key: StorePubkey = Pubkey::new_from_array(*track).into();
                self.set_certified(track_key, *epoch)?;
            }
            ReplayableEvent::DeleteTrack { track, .. } => {
                let track_key: StorePubkey = Pubkey::new_from_array(*track).into();
                if let Ok(Some(info)) = self.context.store.get_track(track_key) {
                    self.cleanup_slices_for_track(track_key, info.spool_group)?;
                }
                self.context.store.delete_track(track_key)?;
                self.context.store.delete_object_info(track_key)?;
            }
            ReplayableEvent::InvalidateTrack { track, epoch } => {
                let track_key: StorePubkey = Pubkey::new_from_array(*track).into();
                if let Ok(Some(info)) = self.context.store.get_track(track_key) {
                    self.cleanup_slices_for_track(track_key, info.spool_group)?;
                }
                self.context.store.put_object_info(
                    track_key,
                    ObjectInfo::Invalid {
                        epoch: *epoch,
                        slot,
                    },
                )?;
            }
            ReplayableEvent::ReserveTape {
                tape, expiry_epoch, ..
            } => {
                let tape_key: StorePubkey = Pubkey::new_from_array(*tape).into();
                self.context.store.put_tape(
                    tape_key,
                    TapeInfo {
                        end_epoch: *expiry_epoch,
                    },
                )?;
            }
            ReplayableEvent::DestroyTape { tape, .. } => {
                let tape_key: StorePubkey = Pubkey::new_from_array(*tape).into();
                self.cascade_delete_tape_tracks(tape_key)?;
                self.context.store.delete_tape(tape_key)?;
            }
            // SyncEpoch, RegisterNode, JoinNetwork — no local store ops needed
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::encoding::EncodingProfile;
    use tape_core::types::StorageUnits;
    use tape_crypto::Hash;
    use tape_store::ops::{SliceOps, SpoolOps};
    use tape_store::types::SpoolStatus;

    use crate::core::test_utils::test_context;

    fn make_advance_epoch(old: u64, new: u64) -> ParsedInstruction {
        ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch: EpochNumber(old),
                new_epoch: EpochNumber(new),
                timestamp: [0; 8],
                committee_size: [0; 8],
                total_stake: [0; 8],
                storage_price: [0; 8],
                storage_capacity: StorageUnits(0),
                nonce: Hash::default(),
            },
        }
    }

    fn make_register_track(track: Pubkey, tape: Pubkey, epoch: u64) -> ParsedInstruction {
        ParsedInstruction::RegisterTrack {
            owner: Pubkey::new_unique(),
            track,
            key: Hash::default(),
            root: Hash::default(),
            commitment: Hash::default(),
            size: StorageUnits(1024),
            event: Some(TrackRegistered {
                track,
                tape,
                key: Hash::default(),
                size: StorageUnits(1024),
                commitment: Hash::default(),
                epoch: EpochNumber(epoch),
                profile: EncodingProfile::basic_default(),
                spool_group: 3u64.to_le_bytes(),
                stripe_size: (1024u64 * 1024).to_le_bytes(),
                stripe_count: 1u64.to_le_bytes(),
                leaves: [Hash::default(); 20],
            }),
        }
    }

    fn make_certify_track(track: Pubkey, epoch: u64) -> ParsedInstruction {
        ParsedInstruction::CertifyTrack {
            track,
            event: TrackCertified {
                track,
                epoch: EpochNumber(epoch),
                signer_count: [0; 8],
                signer_weight: [0; 8],
            },
        }
    }

    fn make_delete_track(track: Pubkey) -> ParsedInstruction {
        let tape = Pubkey::new_unique();
        ParsedInstruction::DeleteTrack {
            owner: Pubkey::new_unique(),
            track,
            event: Some(TrackDeleted {
                track,
                tape,
                key: Hash::default(),
                size: StorageUnits(1024),
            }),
        }
    }

    fn make_invalidate_track(track: Pubkey, epoch: u64) -> ParsedInstruction {
        ParsedInstruction::InvalidateTrack {
            track,
            event: Some(TrackInvalidated {
                track,
                epoch: EpochNumber(epoch),
            }),
        }
    }

    fn make_reserve_tape(tape: Pubkey, expiry_epoch: u64) -> ParsedInstruction {
        ParsedInstruction::ReserveTape {
            owner: Pubkey::new_unique(),
            tape,
            event: Some(TapeReserved {
                tape,
                authority: Pubkey::new_unique(),
                capacity: StorageUnits(5000),
                active_epoch: EpochNumber(1),
                expiry_epoch: EpochNumber(expiry_epoch),
                cost: [0; 8],
            }),
        }
    }

    fn make_destroy_tape(tape: Pubkey) -> ParsedInstruction {
        ParsedInstruction::DestroyTape {
            owner: Pubkey::new_unique(),
            tape,
            event: Some(TapeDestroyed {
                tape,
                authority: Pubkey::new_unique(),
            }),
        }
    }

    fn make_block(slot: u64, instructions: Vec<ParsedInstruction>) -> IngestedBlock {
        IngestedBlock {
            slot: SlotNumber(slot),
            instructions,
        }
    }

    #[test]
    fn slice_accepted() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block).unwrap();

        // Verify is_stored starts false
        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Valid { is_stored: false, .. }));

        // Apply SliceAccepted
        fsm.apply_user_event(&UserEvent::SliceAccepted { track, spool: 0 })
            .unwrap();

        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Valid { is_stored: true, .. }));
    }

    #[test]
    fn slice_accepted_idempotent() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block).unwrap();

        let event = UserEvent::SliceAccepted { track, spool: 0 };
        fsm.apply_user_event(&event).unwrap();
        fsm.apply_user_event(&event).unwrap();

        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Valid { is_stored: true, .. }));
    }

    #[test]
    fn slice_accepted_missing() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx);

        let track = Pubkey::new_unique();
        let event = UserEvent::SliceAccepted { track, spool: 0 };
        fsm.apply_user_event(&event).unwrap(); // no-op, no error
    }

    #[test]
    fn advance_epoch() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let block = make_block(100, vec![make_advance_epoch(0, 1)]);
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            StateChange::EpochAdvanced { epoch } if *epoch == EpochNumber(1)
        ));
        assert_eq!(
            ctx.store.get_chain_epoch().unwrap(),
            Some(EpochNumber(1))
        );
    }

    #[test]
    fn register_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], StateChange::TrackRegistered { .. }));

        let store_track: StorePubkey = track.into();
        let info = ctx.store.get_track(store_track).unwrap().unwrap();
        assert_eq!(info.spool_group, 3);
        assert_eq!(info.original_size, 1024);

        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(
            obj,
            ObjectInfo::Valid {
                is_stored: false,
                registered_epoch,
                certified_epoch: None,
                ..
            } if registered_epoch == EpochNumber(1)
        ));
    }

    #[test]
    fn certify_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(
            100,
            vec![
                make_register_track(track, tape, 1),
                make_certify_track(track, 2),
            ],
        );
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 2);
        assert!(matches!(&changes[1], StateChange::TrackCertified { .. }));

        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(
            obj,
            ObjectInfo::Valid {
                certified_epoch: Some(epoch),
                ..
            } if epoch == EpochNumber(2)
        ));
    }

    #[test]
    fn delete_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        // Register then delete
        let block1 = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block1).unwrap();

        let block2 = make_block(101, vec![make_delete_track(track)]);
        let changes = fsm.apply(&block2).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], StateChange::TrackDeleted { .. }));

        let store_track: StorePubkey = track.into();
        assert!(ctx.store.get_track(store_track).unwrap().is_none());
        assert!(ctx.store.get_object_info(store_track).unwrap().is_none());
    }

    #[test]
    fn invalidate_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let block = make_block(
            100,
            vec![
                make_register_track(track, tape, 1),
                make_invalidate_track(track, 2),
            ],
        );
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 2);
        assert!(matches!(
            &changes[1],
            StateChange::TrackInvalidated { .. }
        ));

        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Invalid { .. }));
    }

    #[test]
    fn reserve_tape() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_reserve_tape(tape, 50)]);
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], StateChange::TapeReserved { .. }));

        let store_tape: StorePubkey = tape.into();
        let info = ctx.store.get_tape(store_tape).unwrap().unwrap();
        assert_eq!(info.end_epoch, EpochNumber(50));
    }

    #[test]
    fn destroy_tape() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();

        let block1 = make_block(100, vec![make_reserve_tape(tape, 50)]);
        fsm.apply(&block1).unwrap();

        let block2 = make_block(101, vec![make_destroy_tape(tape)]);
        let changes = fsm.apply(&block2).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], StateChange::TapeDestroyed { .. }));

        let store_tape: StorePubkey = tape.into();
        assert!(ctx.store.get_tape(store_tape).unwrap().is_none());
    }

    #[test]
    fn sync_cursor_updated() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let block = make_block(42, vec![]);
        fsm.apply(&block).unwrap();

        assert_eq!(
            ctx.store.get_sync_cursor().unwrap(),
            Some(SlotNumber(42))
        );
    }

    #[test]
    fn idempotent_reprocess() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);

        // Apply twice — should not error
        let changes1 = fsm.apply(&block).unwrap();
        let changes2 = fsm.apply(&block).unwrap();

        assert_eq!(changes1.len(), 1);
        assert_eq!(changes2.len(), 1);

        // Store state should be consistent
        let store_track: StorePubkey = track.into();
        let info = ctx.store.get_track(store_track).unwrap().unwrap();
        assert_eq!(info.original_size, 1024);
    }

    #[test]
    fn event_log_populated() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        // Set epoch first so events are logged under the right epoch
        let block1 = make_block(1, vec![make_advance_epoch(0, 1)]);
        fsm.apply(&block1).unwrap();

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block2 = make_block(2, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block2).unwrap();

        let entries = ctx.store.get_epoch_events(EpochNumber(1)).unwrap();
        // Should have at least the AdvanceEpoch event (in slot 1)
        // and RegisterTrack event (in slot 2)
        assert!(entries.len() >= 2);
    }

    // --- replay_snapshot tests ---

    use tape_core::snapshot::{SnapshotEntry, SnapshotLog};

    fn make_log(entries: Vec<SnapshotEntry>, end_slot: u64) -> SnapshotLog {
        SnapshotLog {
            version: 1,
            epoch: EpochNumber(1),
            start_slot: SlotNumber(1),
            end_slot: SlotNumber(end_slot),
            entries,
        }
    }

    #[test]
    fn replay_advance_epoch() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(5),
                }],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        assert_eq!(
            ctx.store.get_chain_epoch().unwrap(),
            Some(EpochNumber(5))
        );
    }

    #[test]
    fn replay_register_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(2048),
            commitment: Hash::default(),
            epoch: EpochNumber(1),
            profile: EncodingProfile::basic_default(),
            spool_group: 7u64.to_le_bytes(),
            stripe_size: (512u64).to_le_bytes(),
            stripe_count: 4u64.to_le_bytes(),
            leaves: [Hash::default(); 20],
        };
        let event_data = bytemuck::bytes_of(&event).to_vec();

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![ReplayableEvent::RegisterTrack {
                    track: track.to_bytes(),
                    event_data,
                }],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        let store_track: StorePubkey = track.into();
        let info = ctx.store.get_track(store_track).unwrap().unwrap();
        assert_eq!(info.spool_group, 7);
        assert_eq!(info.original_size, 2048);

        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(
            obj,
            ObjectInfo::Valid {
                registered_epoch,
                certified_epoch: None,
                ..
            } if registered_epoch == EpochNumber(1)
        ));
    }

    #[test]
    fn replay_certify_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let reg_event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(1024),
            commitment: Hash::default(),
            epoch: EpochNumber(1),
            profile: EncodingProfile::basic_default(),
            spool_group: 0u64.to_le_bytes(),
            stripe_size: 512u64.to_le_bytes(),
            stripe_count: 1u64.to_le_bytes(),
            leaves: [Hash::default(); 20],
        };

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![
                    ReplayableEvent::RegisterTrack {
                        track: track.to_bytes(),
                        event_data: bytemuck::bytes_of(&reg_event).to_vec(),
                    },
                    ReplayableEvent::CertifyTrack {
                        track: track.to_bytes(),
                        epoch: EpochNumber(2),
                    },
                ],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(
            obj,
            ObjectInfo::Valid {
                certified_epoch: Some(epoch),
                ..
            } if epoch == EpochNumber(2)
        ));
    }

    #[test]
    fn replay_delete_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let reg_event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(1024),
            commitment: Hash::default(),
            epoch: EpochNumber(1),
            profile: EncodingProfile::basic_default(),
            spool_group: 0u64.to_le_bytes(),
            stripe_size: 512u64.to_le_bytes(),
            stripe_count: 1u64.to_le_bytes(),
            leaves: [Hash::default(); 20],
        };

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![
                    ReplayableEvent::RegisterTrack {
                        track: track.to_bytes(),
                        event_data: bytemuck::bytes_of(&reg_event).to_vec(),
                    },
                    ReplayableEvent::DeleteTrack {
                        track: track.to_bytes(),
                        epoch: EpochNumber(1),
                    },
                ],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        let store_track: StorePubkey = track.into();
        assert!(ctx.store.get_track(store_track).unwrap().is_none());
        assert!(ctx.store.get_object_info(store_track).unwrap().is_none());
    }

    #[test]
    fn replay_delete_slices() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let reg_event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(1024),
            commitment: Hash::default(),
            epoch: EpochNumber(1),
            profile: EncodingProfile::basic_default(),
            spool_group: 3u64.to_le_bytes(),
            stripe_size: 512u64.to_le_bytes(),
            stripe_count: 1u64.to_le_bytes(),
            leaves: [Hash::default(); 20],
        };
        let track_key: StorePubkey = track.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store
            .put_slice(60, track_key, vec![1, 2, 3])
            .unwrap();

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![
                    ReplayableEvent::RegisterTrack {
                        track: track.to_bytes(),
                        event_data: bytemuck::bytes_of(&reg_event).to_vec(),
                    },
                    ReplayableEvent::DeleteTrack {
                        track: track.to_bytes(),
                        epoch: EpochNumber(1),
                    },
                ],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        assert!(!ctx.store.has_slice(60, track_key).unwrap());
    }

    #[test]
    fn replay_cursor_update() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let log = make_log(vec![], 999);
        fsm.replay_snapshot(&log).unwrap();

        assert_eq!(
            ctx.store.get_sync_cursor().unwrap(),
            Some(SlotNumber(999))
        );
    }

    #[test]
    fn replay_no_event_log() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(0),
                    new_epoch: EpochNumber(1),
                }],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        // Replay must not write to event log
        assert!(!ctx.store.has_epoch_events(EpochNumber(1)).unwrap());
    }

    #[test]
    fn replay_invalidate_slices() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let reg_event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(1024),
            commitment: Hash::default(),
            epoch: EpochNumber(1),
            profile: EncodingProfile::basic_default(),
            spool_group: 3u64.to_le_bytes(),
            stripe_size: 512u64.to_le_bytes(),
            stripe_count: 1u64.to_le_bytes(),
            leaves: [Hash::default(); 20],
        };
        let track_key: StorePubkey = track.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store
            .put_slice(60, track_key, vec![1, 2, 3])
            .unwrap();

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![
                    ReplayableEvent::RegisterTrack {
                        track: track.to_bytes(),
                        event_data: bytemuck::bytes_of(&reg_event).to_vec(),
                    },
                    ReplayableEvent::InvalidateTrack {
                        track: track.to_bytes(),
                        epoch: EpochNumber(2),
                    },
                ],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        assert!(!ctx.store.has_slice(60, track_key).unwrap());
        assert!(matches!(
            ctx.store.get_object_info(track_key).unwrap(),
            Some(ObjectInfo::Invalid { .. })
        ));
    }

    #[test]
    fn replay_destroy_cascade() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();
        let reg_event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(1024),
            commitment: Hash::default(),
            epoch: EpochNumber(1),
            profile: EncodingProfile::basic_default(),
            spool_group: 3u64.to_le_bytes(),
            stripe_size: 512u64.to_le_bytes(),
            stripe_count: 1u64.to_le_bytes(),
            leaves: [Hash::default(); 20],
        };
        let track_key: StorePubkey = track.into();
        let tape_key: StorePubkey = tape.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store
            .put_slice(60, track_key, vec![1, 2, 3])
            .unwrap();

        let log = make_log(
            vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![
                    ReplayableEvent::ReserveTape {
                        tape: tape.to_bytes(),
                        authority: Pubkey::new_unique().to_bytes(),
                        active_epoch: EpochNumber(1),
                        expiry_epoch: EpochNumber(50),
                    },
                    ReplayableEvent::RegisterTrack {
                        track: track.to_bytes(),
                        event_data: bytemuck::bytes_of(&reg_event).to_vec(),
                    },
                    ReplayableEvent::DestroyTape {
                        tape: tape.to_bytes(),
                        epoch: EpochNumber(2),
                    },
                ],
            }],
            10,
        );
        fsm.replay_snapshot(&log).unwrap();

        assert!(ctx.store.get_tape(tape_key).unwrap().is_none());
        assert!(ctx.store.get_track(track_key).unwrap().is_none());
        assert!(ctx.store.get_object_info(track_key).unwrap().is_none());
        assert!(!ctx.store.has_slice(60, track_key).unwrap());
    }

    // --- data lifecycle tests ---

    #[test]
    fn delete_track_cleans_slices() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        // Register track in spool group 3 (spools 60-79)
        let block1 = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block1).unwrap();

        // Own spool 60 (in group 3) and store a slice
        let store_track: StorePubkey = track.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store
            .put_slice(60, store_track, vec![1, 2, 3])
            .unwrap();
        assert!(ctx.store.has_slice(60, store_track).unwrap());

        // Delete track — should clean up slice
        let block2 = make_block(101, vec![make_delete_track(track)]);
        fsm.apply(&block2).unwrap();

        assert!(!ctx.store.has_slice(60, store_track).unwrap());
        assert!(ctx.store.get_track(store_track).unwrap().is_none());
    }

    #[test]
    fn delete_track_no_track() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        // Delete a track that was never registered — no-op, no error
        let track = Pubkey::new_unique();
        let block = make_block(100, vec![make_delete_track(track)]);
        fsm.apply(&block).unwrap();
    }

    #[test]
    fn epoch_gc_expired() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();

        // Reserve tape expiring at epoch 5, register a track
        let block1 = make_block(
            100,
            vec![
                make_advance_epoch(0, 1),
                make_reserve_tape(tape, 5),
                make_register_track(track, tape, 1),
            ],
        );
        fsm.apply(&block1).unwrap();

        // Store a slice
        let store_track: StorePubkey = track.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store
            .put_slice(60, store_track, vec![1, 2, 3])
            .unwrap();

        // Advance to epoch 5 — tape expires, should be GC'd
        let block2 = make_block(200, vec![make_advance_epoch(1, 5)]);
        fsm.apply(&block2).unwrap();

        let store_tape: StorePubkey = tape.into();
        assert!(ctx.store.get_tape(store_tape).unwrap().is_none());
        assert!(ctx.store.get_track(store_track).unwrap().is_none());
        assert!(!ctx.store.has_slice(60, store_track).unwrap());
    }

    #[test]
    fn epoch_gc_keeps_active() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();

        // Reserve tape expiring at epoch 10
        let block1 = make_block(100, vec![make_reserve_tape(tape, 10)]);
        fsm.apply(&block1).unwrap();

        // Advance to epoch 5 — tape still active
        let block2 = make_block(200, vec![make_advance_epoch(0, 5)]);
        fsm.apply(&block2).unwrap();

        let store_tape: StorePubkey = tape.into();
        assert!(ctx.store.get_tape(store_tape).unwrap().is_some());
    }

    #[test]
    fn destroy_tape_cascades() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();
        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();

        // Reserve tape, register 2 tracks on it
        let block1 = make_block(
            100,
            vec![
                make_reserve_tape(tape, 50),
                make_register_track(track1, tape, 1),
                make_register_track(track2, tape, 1),
            ],
        );
        fsm.apply(&block1).unwrap();

        // Own spool 60 (group 3) and store slices for both tracks
        let st1: StorePubkey = track1.into();
        let st2: StorePubkey = track2.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store.put_slice(60, st1, vec![1, 2, 3]).unwrap();
        ctx.store.put_slice(60, st2, vec![4, 5, 6]).unwrap();

        // Destroy tape — should cascade-delete both tracks and slices
        let block2 = make_block(101, vec![make_destroy_tape(tape)]);
        fsm.apply(&block2).unwrap();

        let store_tape: StorePubkey = tape.into();
        assert!(ctx.store.get_tape(store_tape).unwrap().is_none());
        assert!(ctx.store.get_track(st1).unwrap().is_none());
        assert!(ctx.store.get_track(st2).unwrap().is_none());
        assert!(ctx.store.get_object_info(st1).unwrap().is_none());
        assert!(ctx.store.get_object_info(st2).unwrap().is_none());
        assert!(!ctx.store.has_slice(60, st1).unwrap());
        assert!(!ctx.store.has_slice(60, st2).unwrap());
    }

    #[test]
    fn destroy_tape_no_tracks() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let tape = Pubkey::new_unique();
        let block1 = make_block(100, vec![make_reserve_tape(tape, 50)]);
        fsm.apply(&block1).unwrap();

        // Destroy tape with no tracks — no error
        let block2 = make_block(101, vec![make_destroy_tape(tape)]);
        fsm.apply(&block2).unwrap();

        let store_tape: StorePubkey = tape.into();
        assert!(ctx.store.get_tape(store_tape).unwrap().is_none());
    }

    #[test]
    fn invalidate_track_cleans_slices() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        // Register track in spool group 3 (spools 60-79)
        let block1 = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block1).unwrap();

        // Own spool 60 and store a slice
        let store_track: StorePubkey = track.into();
        ctx.store
            .set_spool_status(60, SpoolStatus::Active)
            .unwrap();
        ctx.store
            .put_slice(60, store_track, vec![1, 2, 3])
            .unwrap();

        // Invalidate track — should clean up slice and mark invalid
        let block2 = make_block(101, vec![make_invalidate_track(track, 2)]);
        fsm.apply(&block2).unwrap();

        assert!(!ctx.store.has_slice(60, store_track).unwrap());
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Invalid { .. }));
    }
}
