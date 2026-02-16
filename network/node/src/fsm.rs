//! Finite state machine — applies parsed instructions to local state.
//!
//! The FSM is the single writer to the local store. It receives `IngestedBlock`
//! batches from the ingestor, applies each instruction using the ops traits,
//! and emits `Vec<StateChange>` to the reconciler.
//!
//! Crash consistency: the sync cursor is updated LAST after all instructions in
//! a block are applied. If we crash mid-block, the cursor hasn't advanced, so
//! the entire block is re-processed on restart. All handlers are idempotent.

use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, TapeDestroyed, TapeReserved,
    TrackCertified, TrackDeleted, TrackInvalidated, TrackRegistered,
};
use tape_blocks::ParsedInstruction;
use tape_core::snapshot::{ReplayableEvent, SnapshotLog};
use tape_core::types::{EpochNumber, SlotNumber};
use tape_store::error::TapeStoreError;
use tape_store::ops::{EventLogOps, MetaOps, ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, TapeInfo, TrackInfo};

use crate::core::NodeContext;
use crate::ingestor::IngestedBlock;

#[derive(Debug, thiserror::Error)]
pub enum FsmError {
    #[error("store error: {0}")]
    Store(#[from] TapeStoreError),
}

/// A state change emitted by the FSM after applying instructions.
///
/// The reconciler consumes these to determine which tasks to schedule.
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
pub struct Fsm<S: Store> {
    context: Arc<NodeContext<S>>,
}

impl<S: Store> Fsm<S> {
    pub fn new(context: Arc<NodeContext<S>>) -> Self {
        Self { context }
    }

    /// Apply a user event (e.g. slice accepted by HTTP handler).
    pub fn apply_user_event(&self, event: &UserEvent) -> Result<(), FsmError> {
        match event {
            UserEvent::SliceAccepted { track, .. } => {
                let key: tape_store::types::Pubkey = (*track).into();
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
    /// Returns the state changes produced, which the reconciler uses to
    /// determine what tasks to schedule or cancel.
    pub fn apply(&self, block: &IngestedBlock) -> Result<Vec<StateChange>, FsmError> {
        let mut changes = Vec::new();
        for instruction in &block.instructions {
            self.apply_instruction(instruction, block.slot, &mut changes)?;
        }
        // Update sync cursor LAST — crash recovery re-processes from cursor
        self.context.store.set_sync_cursor(block.slot)?;
        Ok(changes)
    }

    fn apply_instruction(
        &self,
        instruction: &ParsedInstruction,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
    ) -> Result<(), FsmError> {
        match instruction {
            ParsedInstruction::AdvanceEpoch { event } => {
                self.handle_advance_epoch(event, slot, changes)
            }
            ParsedInstruction::SyncEpoch { event } => {
                self.handle_sync_epoch(event, slot, changes)
            }
            ParsedInstruction::RegisterTrack { track, event, .. } => match event {
                Some(event) => self.handle_register_track(*track, event, slot, changes),
                None => Ok(()),
            },
            ParsedInstruction::CertifyTrack { track, event } => {
                self.handle_certify_track(*track, event, slot, changes)
            }
            ParsedInstruction::DeleteTrack { track, event, .. } => match event {
                Some(event) => self.handle_delete_track(*track, event, slot, changes),
                None => Ok(()),
            },
            ParsedInstruction::InvalidateTrack { track, event } => match event {
                Some(event) => self.handle_invalidate_track(*track, event, slot, changes),
                None => Ok(()),
            },
            ParsedInstruction::ReserveTape { tape, event, .. } => match event {
                Some(event) => self.handle_reserve_tape(*tape, event, slot, changes),
                None => Ok(()),
            },
            ParsedInstruction::DestroyTape { tape, event, .. } => match event {
                Some(event) => self.handle_destroy_tape(*tape, event, slot, changes),
                None => Ok(()),
            },
            ParsedInstruction::RegisterNode {
                authority,
                node,
                event,
            } => match event {
                Some(event) => {
                    self.handle_register_node(*authority, *node, event, slot, changes)
                }
                None => Ok(()),
            },
            ParsedInstruction::JoinNetwork { node, event } => match event {
                Some(event) => self.handle_join_network(*node, event, slot, changes),
                None => Ok(()),
            },
        }
    }

    fn handle_advance_epoch(
        &self,
        event: &EpochAdvanced,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
    ) -> Result<(), FsmError> {
        let old_epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.set_current_epoch(event.new_epoch)?;

        self.context.store.append_event(
            event.new_epoch,
            slot,
            &ReplayableEvent::AdvanceEpoch {
                old_epoch,
                new_epoch: event.new_epoch,
            },
        )?;

        self.context.stats.inc_epochs();
        changes.push(StateChange::EpochAdvanced {
            epoch: event.new_epoch,
        });
        Ok(())
    }

    fn handle_sync_epoch(
        &self,
        event: &NodeSynced,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
    ) -> Result<(), FsmError> {
        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));

        self.context.store.append_event(
            epoch,
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
    ) -> Result<(), FsmError> {
        let mut track_info = TrackInfo {
            tape_address: event.tape.into(),
            spool_group: u64::from_le_bytes(event.spool_group),
            original_size: event.size.0,
            stripe_size: u64::from_le_bytes(event.stripe_size),
            stripe_count: u64::from_le_bytes(event.stripe_count),
            encoding_type: 0,
            encoding_params: 0,
            commitment: event.leaves.to_vec(),
        };
        track_info.set_profile(event.profile);

        self.context.store.put_track(track.into(), track_info)?;

        let object_info = ObjectInfo::Valid {
            is_stored: false,
            track_address: track.into(),
            registered_epoch: event.epoch,
            certified_epoch: None,
            slot,
        };
        self.context
            .store
            .put_object_info(track.into(), object_info)?;

        let event_data = bytemuck::bytes_of(event).to_vec();
        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
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
    ) -> Result<(), FsmError> {
        // Read existing ObjectInfo — if missing, skip (idempotent)
        let Some(object_info) = self.context.store.get_object_info(track.into())? else {
            return Ok(());
        };

        if let ObjectInfo::Valid {
            is_stored,
            track_address,
            registered_epoch,
            slot: reg_slot,
            ..
        } = object_info
        {
            let updated = ObjectInfo::Valid {
                is_stored,
                track_address,
                registered_epoch,
                certified_epoch: Some(event.epoch),
                slot: reg_slot,
            };
            self.context
                .store
                .put_object_info(track.into(), updated)?;
        }

        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
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
    ) -> Result<(), FsmError> {
        self.context.store.delete_track(track.into())?;
        self.context.store.delete_object_info(track.into())?;

        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
            slot,
            &ReplayableEvent::DeleteTrack {
                track: track.to_bytes(),
                epoch,
            },
        )?;

        changes.push(StateChange::TrackDeleted { track });
        Ok(())
    }

    fn handle_invalidate_track(
        &self,
        track: Pubkey,
        event: &TrackInvalidated,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
    ) -> Result<(), FsmError> {
        let invalid = ObjectInfo::Invalid {
            epoch: event.epoch,
            slot,
        };
        self.context
            .store
            .put_object_info(track.into(), invalid)?;

        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
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
    ) -> Result<(), FsmError> {
        let tape_info = TapeInfo {
            end_epoch: event.expiry_epoch,
        };
        self.context.store.put_tape(tape.into(), tape_info)?;

        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
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
    ) -> Result<(), FsmError> {
        self.context.store.delete_tape(tape.into())?;

        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
            slot,
            &ReplayableEvent::DestroyTape {
                tape: tape.to_bytes(),
                epoch,
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
    ) -> Result<(), FsmError> {
        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
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
    ) -> Result<(), FsmError> {
        let epoch = self
            .context
            .store
            .get_current_epoch()?
            .unwrap_or(EpochNumber(0));
        self.context.store.append_event(
            epoch,
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
                self.context.store.set_current_epoch(*new_epoch)?;
            }
            ReplayableEvent::RegisterTrack { track, event_data } => {
                let track_key: tape_store::types::Pubkey =
                    Pubkey::new_from_array(*track).into();
                let event: &TrackRegistered = bytemuck::from_bytes(event_data);
                let mut track_info = TrackInfo {
                    tape_address: event.tape.into(),
                    spool_group: u64::from_le_bytes(event.spool_group),
                    original_size: event.size.0,
                    stripe_size: u64::from_le_bytes(event.stripe_size),
                    stripe_count: u64::from_le_bytes(event.stripe_count),
                    encoding_type: 0,
                    encoding_params: 0,
                    commitment: event.leaves.to_vec(),
                };
                track_info.set_profile(event.profile);
                self.context.store.put_track(track_key, track_info)?;
                let object_info = ObjectInfo::Valid {
                    is_stored: false,
                    track_address: track_key,
                    registered_epoch: event.epoch,
                    certified_epoch: None,
                    slot,
                };
                self.context.store.put_object_info(track_key, object_info)?;
            }
            ReplayableEvent::CertifyTrack { track, epoch } => {
                let track_key: tape_store::types::Pubkey =
                    Pubkey::new_from_array(*track).into();
                if let Some(obj) = self.context.store.get_object_info(track_key)? {
                    if let ObjectInfo::Valid {
                        is_stored,
                        track_address,
                        registered_epoch,
                        slot: reg_slot,
                        ..
                    } = obj
                    {
                        self.context.store.put_object_info(
                            track_key,
                            ObjectInfo::Valid {
                                is_stored,
                                track_address,
                                registered_epoch,
                                certified_epoch: Some(*epoch),
                                slot: reg_slot,
                            },
                        )?;
                    }
                }
            }
            ReplayableEvent::DeleteTrack { track, .. } => {
                let track_key: tape_store::types::Pubkey =
                    Pubkey::new_from_array(*track).into();
                self.context.store.delete_track(track_key)?;
                self.context.store.delete_object_info(track_key)?;
            }
            ReplayableEvent::InvalidateTrack { track, epoch } => {
                let track_key: tape_store::types::Pubkey =
                    Pubkey::new_from_array(*track).into();
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
                let tape_key: tape_store::types::Pubkey =
                    Pubkey::new_from_array(*tape).into();
                self.context.store.put_tape(
                    tape_key,
                    TapeInfo {
                        end_epoch: *expiry_epoch,
                    },
                )?;
            }
            ReplayableEvent::DestroyTape { tape, .. } => {
                let tape_key: tape_store::types::Pubkey =
                    Pubkey::new_from_array(*tape).into();
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
    use std::path::PathBuf;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::encoding::EncodingProfile;
    use tape_core::types::StorageUnits;
    use tape_crypto::Hash;
    use tape_store::{MemoryStore, TapeStore};

    use crate::core::config::RecoveryConfig;
    use crate::core::{NodeApiConfig, NodeConfig, TlsConfig};

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
        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_track: tape_store::types::Pubkey = track.into();
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
            ctx.store.get_current_epoch().unwrap(),
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

        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_tape: tape_store::types::Pubkey = tape.into();
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

        let store_tape: tape_store::types::Pubkey = tape.into();
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
        let store_track: tape_store::types::Pubkey = track.into();
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
            ctx.store.get_current_epoch().unwrap(),
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

        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_track: tape_store::types::Pubkey = track.into();
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

        let store_track: tape_store::types::Pubkey = track.into();
        assert!(ctx.store.get_track(store_track).unwrap().is_none());
        assert!(ctx.store.get_object_info(store_track).unwrap().is_none());
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
}
