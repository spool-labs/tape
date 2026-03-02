use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_api::event::TrackRegistered;
use tape_store::ops::MetaOps;
use tape_core::snapshot::{ReplayableEvent, SnapshotLog};
use tape_core::types::SlotNumber;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey as StorePubkey};
use tape_store::types::TapeInfo;

use super::{Fsm, FsmError};

impl<S: Store, R: Rpc> Fsm<S, R> {
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
                self.context
                    .store
                    .put_tape(tape_key, TapeInfo { end_epoch: *expiry_epoch })?;
            }
            ReplayableEvent::DestroyTape { tape, .. } => {
                let tape_key: StorePubkey = Pubkey::new_from_array(*tape).into();
                self.cascade_delete_tape_tracks(tape_key)?;
                self.context.store.delete_tape(tape_key)?;
            }
            // AdvanceEpoch, SyncEpoch, RegisterNode, JoinNetwork — no local store ops needed
            _ => {}
        }
        Ok(())
    }
}
