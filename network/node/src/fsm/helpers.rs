use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_api::event::TrackRegistered;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey as StorePubkey, TrackInfo};

use super::{Fsm, FsmError};

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Fsm<Db, Cluster, Blockchain> {

    pub fn put_track_obj(
        &self,
        track: StorePubkey,
        event: &TrackRegistered,
        slot: SlotNumber,
    ) -> Result<(), FsmError> {

        let mut info = TrackInfo {
            tape_address: event.tape.into(),
            spool_group: SpoolGroup(u64::from_le_bytes(event.spool_group)),
            original_size: event.size.0,
            stripe_size: u64::from_le_bytes(event.stripe_size),
            stripe_count: u64::from_le_bytes(event.stripe_count),
            encoding_type: 0,
            encoding_params: 0,
            commitment: event.leaves.to_vec(),
        };

        info.set_profile(event.profile);

        self.context.store.put_track(track, info)?;
        self.context.store.put_object_info(
            track,
            ObjectInfo::Valid {
                track_address: track,
                registered_epoch: event.epoch,
                certified_epoch: None,
                slot,
            },
        )?;

        Ok(())
    }

    pub fn set_certified(
        &self,
        track: StorePubkey,
        epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let Some(obj) = self.context.store.get_object_info(track)? else {
            return Ok(());
        };

        if let ObjectInfo::Valid {
            track_address,
            registered_epoch,
            slot,
            ..
        } = obj {
            self.context.store.put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch,
                    certified_epoch: Some(epoch),
                    slot,
                },
            )?;
        }

        Ok(())
    }

    pub fn cleanup_slices_for_track(
        &self,
        track: StorePubkey,
        spool_group: SpoolGroup,
    ) -> Result<(), FsmError> {

        let owned_spools = self.context.store.iter_all_spools()?;
        for (spool_id, _status) in &owned_spools {
            if spool_group.contains(*spool_id) {
                let _ = self.context.store.delete_slice(*spool_id, track);
                let _ = self.context.store.remove_pending_recovery(*spool_id, track);
            }
        }

        Ok(())
    }

    pub fn cascade_delete_tape_tracks(
        &self,
        tape: StorePubkey,
    ) -> Result<(), FsmError> {
        let mut cursor = None;
        loop {
            let tracks = self.context.store
                .iter_tracks_from(cursor, 100)?;

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

    pub fn gc_expired_tapes(&self, current_epoch: EpochNumber) -> Result<(), FsmError> {
        let tapes = self.context.store.iter_all_tapes()?;
        for (tape_addr, tape_info) in &tapes {
            if tape_info.end_epoch <= current_epoch {
                self.cascade_delete_tape_tracks(*tape_addr)?;
                self.context.store.delete_tape(*tape_addr)?;
            }
        }

        Ok(())
    }

}
