use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use store::Store;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, PoolAdvanced, TapeDestroyed,
    TapeReserved, TrackCertified, TrackDeleted, TrackInvalidated, TrackRegistered,
};
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_store::ops::{EventLogOps, ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey as StorePubkey, TapeInfo, TrackInfo};
use crate::core::committee::our_member_index;

use super::{RuntimeEvent, Fsm, FsmError, StateChange};

impl<S: Store, R: Rpc> Fsm<S, R> {
    /// Apply a runtime event (e.g. slice accepted by HTTP handler).
    pub fn apply_event(&self, event: &RuntimeEvent) -> Result<(), FsmError> {
        match event {
            RuntimeEvent::SliceAccepted { track, spool } => self.handle_slice_accepted(*track, *spool),
        }
    }

    pub fn put_track_obj(
        &self,
        track: StorePubkey,
        event: &TrackRegistered,
        slot: SlotNumber,
    ) -> Result<(), FsmError> {
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

        self.context.store.put_track(track, info)?;
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

    pub fn set_certified(
        &self,
        track: StorePubkey,
        epoch: EpochNumber,
    ) -> Result<(), FsmError> {
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

    fn handle_slice_accepted(&self, track: Pubkey, _spool: u16) -> Result<(), FsmError> {
        let key: StorePubkey = track.into();
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

    pub fn handle_advance_epoch(
        &mut self,
        event: &EpochAdvanced,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: &mut EpochNumber,
    ) -> Result<(), FsmError> {
        let old_epoch = *current_epoch;
        let new_phase = EpochPhase::try_from(event.phase).unwrap_or(EpochPhase::Syncing);

        self.state.epoch = event.new_epoch;
        self.state.phase = new_phase;
        *current_epoch = event.new_epoch;

        self.gc_expired_tapes(event.new_epoch)?;

        self.context.store.append_event(
            event.new_epoch,
            slot,
            &tape_core::snapshot::ReplayableEvent::AdvanceEpoch {
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

    pub fn log_member_index_for_epoch(&self, epoch: EpochNumber, source: &str) {
        let cs = self.context.chain_state.load();
        let Some(committee) = cs.committee_for(epoch) else {
            tracing::warn!(
                source = source,
                epoch = epoch.0,
                "cannot resolve committee when logging member index"
            );
            return;
        };
        if committee.is_empty() {
            tracing::warn!(
                source = source,
                epoch = epoch.0,
                "cannot resolve committee when logging member index"
            );
            return;
        }

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

    pub fn handle_sync_epoch(
        &mut self,
        event: &NodeSynced,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        self.context.store.append_event(
            current_epoch,
            slot,
            &tape_core::snapshot::ReplayableEvent::SyncEpoch {
                node: event.node.to_bytes(),
                node_id: event.id,
                epoch: event.epoch,
                spools_hash: event.spools_hash,
            },
        )?;

        self.emit_phase(event.phase, changes);

        changes.push(StateChange::NodeSynced { node: event.node });
        Ok(())
    }

    pub fn handle_advance_pool(
        &mut self,
        node: Pubkey,
        event: &PoolAdvanced,
        changes: &mut Vec<StateChange>,
    ) -> Result<(), FsmError> {
        self.emit_phase(event.phase, changes);
        changes.push(StateChange::PoolAdvanced { node });
        Ok(())
    }

    /// If the event's phase differs from our tracked phase, update and emit.
    fn emit_phase(&mut self, event_phase: u64, changes: &mut Vec<StateChange>) {
        if let Ok(new_phase) = EpochPhase::try_from(event_phase) {
            if new_phase != self.state.phase {
                self.state.phase = new_phase;
                changes.push(StateChange::PhaseAdvanced { phase: new_phase });
            }
        }
    }

    pub fn handle_register_track(
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
            &tape_core::snapshot::ReplayableEvent::RegisterTrack {
                track: track.to_bytes(),
                event_data,
            },
        )?;

        changes.push(StateChange::TrackRegistered { track });
        Ok(())
    }

    pub fn handle_certify_track(
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
            &tape_core::snapshot::ReplayableEvent::CertifyTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            },
        )?;

        changes.push(StateChange::TrackCertified { track });
        Ok(())
    }

    pub fn handle_delete_track(
        &self,
        track: Pubkey,
        _event: &TrackDeleted,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let store_track: StorePubkey = track.into();

        if let Ok(Some(info)) = self.context.store.get_track(store_track) {
            self.cleanup_slices_for_track(store_track, info.spool_group)?;
        }

        self.context.store.delete_track(store_track)?;
        self.context.store.delete_object_info(store_track)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &tape_core::snapshot::ReplayableEvent::DeleteTrack {
                track: track.to_bytes(),
                epoch: current_epoch,
            },
        )?;

        changes.push(StateChange::TrackDeleted { track });
        Ok(())
    }

    pub fn cleanup_slices_for_track(
        &self,
        track: StorePubkey,
        spool_group: u64,
    ) -> Result<(), FsmError> {
        let owned_spools = self.context.store.iter_all_spools()?;
        for (spool_id, _status) in &owned_spools {
            if tape_core::erasure::spool_in_group(*spool_id, spool_group) {
                let _ = self.context.store.delete_slice(*spool_id, track);
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

    pub fn handle_invalidate_track(
        &self,
        track: Pubkey,
        event: &TrackInvalidated,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let store_track: StorePubkey = track.into();

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
            &tape_core::snapshot::ReplayableEvent::InvalidateTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            },
        )?;

        changes.push(StateChange::TrackInvalidated { track });
        Ok(())
    }

    pub fn handle_reserve_tape(
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
            &tape_core::snapshot::ReplayableEvent::ReserveTape {
                tape: tape.to_bytes(),
                authority: event.authority.to_bytes(),
                active_epoch: event.active_epoch,
                expiry_epoch: event.expiry_epoch,
            },
        )?;

        changes.push(StateChange::TapeReserved { tape });
        Ok(())
    }

    pub fn handle_destroy_tape(
        &self,
        tape: Pubkey,
        _event: &TapeDestroyed,
        slot: SlotNumber,
        changes: &mut Vec<StateChange>,
        current_epoch: EpochNumber,
    ) -> Result<(), FsmError> {
        let store_tape: StorePubkey = tape.into();

        self.cascade_delete_tape_tracks(store_tape)?;

        self.context.store.delete_tape(store_tape)?;

        self.context.store.append_event(
            current_epoch,
            slot,
            &tape_core::snapshot::ReplayableEvent::DestroyTape {
                tape: tape.to_bytes(),
                epoch: current_epoch,
            },
        )?;

        changes.push(StateChange::TapeDestroyed { tape });
        Ok(())
    }

    pub fn handle_register_node(
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
            &tape_core::snapshot::ReplayableEvent::RegisterNode {
                authority: authority.to_bytes(),
                node: node.to_bytes(),
            },
        )?;

        changes.push(StateChange::NodeRegistered { node });
        Ok(())
    }

    pub fn handle_join_network(
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
            &tape_core::snapshot::ReplayableEvent::JoinNetwork {
                node: node.to_bytes(),
            },
        )?;

        changes.push(StateChange::NodeJoinedCommittee { node });
        Ok(())
    }
}
