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
use tape_protocol::Api;
use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_store::error::TapeStoreError;
#[cfg(test)]
use tape_store::types::Pubkey as StorePubkey;

use crate::core::NodeContext;

mod apply;
mod handlers;
mod replay;
mod helpers;

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
    PhaseAdvanced { phase: EpochPhase },
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
    PoolAdvanced { node: Pubkey },
}

/// An event from runtime-facing HTTP handlers, forwarded to the FSM.
#[derive(Debug)]
pub enum UserEvent {
    SliceAccepted { track: Pubkey, spool: u16 },
}

/// Backward-compatible alias for existing call sites.
pub type RuntimeEvent = UserEvent;

/// Internal FSM state, seeded at epoch 0 on startup.
///
/// The FSM replays blocks from the sync cursor forward. `AdvanceEpoch`
/// events in the block stream advance `state.epoch` to the correct value,
/// so seeding at 0 is safe — all handlers are idempotent.
struct FsmState {
    epoch: EpochNumber,
    phase: EpochPhase,
}

/// Single-writer state machine that processes blocks and updates local storage.
pub struct Fsm<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: FsmState,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Fsm<Db, Cluster, Blockchain> {}


#[cfg(test)]
mod tests {
    use super::*;

    use tape_api::event::{
        EpochAdvanced, TapeDestroyed, TapeReserved, TrackCertified, TrackDeleted, TrackInvalidated,
        TrackRegistered,
    };
    use tape_blocks::ParsedInstruction;
    use tape_core::encoding::EncodingProfile;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{SlotNumber, StorageUnits};
    use tape_crypto::Hash;
    use tape_store::ops::{
        EventLogOps, MetaOps, ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps,
    };
    use tape_store::types::{ObjectInfo, SpoolState, SpoolStatus};

    use crate::core::test_utils::test_context;
    use crate::ingestor::IngestedBlock;

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
                phase: 1, // Syncing
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
            size: StorageUnits::mb(1024),
            event: TrackRegistered {
                track,
                tape,
                key: Hash::default(),
                size: StorageUnits::mb(1024),
                commitment: Hash::default(),
                epoch: EpochNumber(epoch),
                profile: EncodingProfile::basic_default(),
                spool_group: 3u64.to_le_bytes(),
                stripe_size: (1024u64 * 1024).to_le_bytes(),
                stripe_count: 1u64.to_le_bytes(),
                leaves: [Hash::default(); 20],
            },
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
            event: TrackDeleted {
                track,
                tape,
                key: Hash::default(),
                size: StorageUnits::mb(1024),
            },
        }
    }

    fn make_invalidate_track(track: Pubkey, epoch: u64) -> ParsedInstruction {
        ParsedInstruction::InvalidateTrack {
            track,
            event: TrackInvalidated {
                track,
                epoch: EpochNumber(epoch),
            },
        }
    }

    fn make_reserve_tape(tape: Pubkey, expiry_epoch: u64) -> ParsedInstruction {
        ParsedInstruction::ReserveTape {
            owner: Pubkey::new_unique(),
            tape,
            event: TapeReserved {
                tape,
                authority: Pubkey::new_unique(),
                capacity: StorageUnits::mb(5000),
                active_epoch: EpochNumber(1),
                expiry_epoch: EpochNumber(expiry_epoch),
                cost: [0; 8],
            },
        }
    }

    fn make_destroy_tape(tape: Pubkey) -> ParsedInstruction {
        ParsedInstruction::DestroyTape {
            owner: Pubkey::new_unique(),
            tape,
            event: TapeDestroyed {
                tape,
                authority: Pubkey::new_unique(),
            },
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
        let mut fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block).unwrap();

        // Verify is_stored starts false
        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Valid { is_stored: false, .. }));

        // Apply SliceAccepted
        fsm.apply_event(&RuntimeEvent::SliceAccepted { track, spool: 0 })
            .unwrap();

        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Valid { is_stored: true, .. }));
    }

    #[test]
    fn slice_accepted_idempotent() {
        let ctx = test_context();
        let mut fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block).unwrap();

        let event = RuntimeEvent::SliceAccepted { track, spool: 0 };
        fsm.apply_event(&event).unwrap();
        fsm.apply_event(&event).unwrap();

        let store_track: StorePubkey = track.into();
        let obj = ctx.store.get_object_info(store_track).unwrap().unwrap();
        assert!(matches!(obj, ObjectInfo::Valid { is_stored: true, .. }));
    }

    #[test]
    fn slice_accepted_missing() {
        let ctx = test_context();
        let fsm = Fsm::new(ctx);

        let track = Pubkey::new_unique();
        let event = RuntimeEvent::SliceAccepted { track, spool: 0 };
        fsm.apply_event(&event).unwrap(); // no-op, no error
    }

    #[test]
    fn advance_epoch() {
        let ctx = test_context();
        let mut fsm = Fsm::new(ctx.clone());

        let block = make_block(100, vec![make_advance_epoch(0, 1)]);
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            StateChange::EpochAdvanced { epoch } if *epoch == EpochNumber(1)
        ));
    }

    #[test]
    fn register_track() {
        let ctx = test_context();
        let mut fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = make_block(100, vec![make_register_track(track, tape, 1)]);
        let changes = fsm.apply(&block).unwrap();

        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], StateChange::TrackRegistered { .. }));

        let store_track: StorePubkey = track.into();
        let info = ctx.store.get_track(store_track).unwrap().unwrap();
        assert_eq!(info.spool_group, SpoolGroup(3));
        assert_eq!(info.original_size, StorageUnits::mb(1024).0);

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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
        assert_eq!(info.original_size, StorageUnits::mb(1024).0);
    }

    #[test]
    fn event_log_populated() {
        let ctx = test_context();
        let mut fsm = Fsm::new(ctx.clone());

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
            size: StorageUnits::mb(2048),
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
        assert_eq!(info.spool_group, SpoolGroup(7));
        assert_eq!(info.original_size, StorageUnits::mb(2048).0);

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
            size: StorageUnits::mb(1024),
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
            size: StorageUnits::mb(1024),
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
            size: StorageUnits::mb(1024),
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
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
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
            size: StorageUnits::mb(1024),
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
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
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
            size: StorageUnits::mb(1024),
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
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
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
        let mut fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        // Register track in spool group 3 (spools 60-79)
        let block1 = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block1).unwrap();

        // Own spool 60 (in group 3) and store a slice
        let store_track: StorePubkey = track.into();
        ctx.store
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
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
        let mut fsm = Fsm::new(ctx.clone());

        // Delete a track that was never registered — no-op, no error
        let track = Pubkey::new_unique();
        let block = make_block(100, vec![make_delete_track(track)]);
        fsm.apply(&block).unwrap();
    }

    #[test]
    fn epoch_gc_expired() {
        let ctx = test_context();
        let mut fsm = Fsm::new(ctx.clone());

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
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
            .unwrap();
        ctx.store
            .put_slice(60, store_track, vec![1, 2, 3])
            .unwrap();
        ctx.store
            .add_pending_recovery(60, store_track)
            .unwrap();

        // Advance to epoch 5 — tape expires, should be GC'd
        let block2 = make_block(200, vec![make_advance_epoch(1, 5)]);
        fsm.apply(&block2).unwrap();

        let store_tape: StorePubkey = tape.into();
        assert!(ctx.store.get_tape(store_tape).unwrap().is_none());
        assert!(ctx.store.get_track(store_track).unwrap().is_none());
        assert!(!ctx.store.has_slice(60, store_track).unwrap());
        assert!(!ctx.store.has_pending_recovery(60, store_track).unwrap());
    }

    #[test]
    fn epoch_gc_keeps_active() {
        let ctx = test_context();
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

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
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
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
        let mut fsm = Fsm::new(ctx.clone());

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
        let mut fsm = Fsm::new(ctx.clone());

        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        // Register track in spool group 3 (spools 60-79)
        let block1 = make_block(100, vec![make_register_track(track, tape, 1)]);
        fsm.apply(&block1).unwrap();

        // Own spool 60 and store a slice
        let store_track: StorePubkey = track.into();
        ctx.store
            .set_spool_state(60, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
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
