use rpc::Rpc;
use tape_protocol::Api;
use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, PoolAdvanced, TapeDestroyed,
    TapeReserved, TrackCertified, TrackDeleted, TrackInvalidated, TrackRegistered,
};
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_store::ops::{EventLogOps, ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey as StorePubkey, TapeInfo};

use super::{Fsm, FsmError, StateChange};

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Fsm<Db, Cluster, Blockchain> {
    pub fn handle_slice_accepted(&self, track: Pubkey, _spool: u16) -> Result<(), FsmError> {
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
        changes.push(StateChange::EpochAdvanced {
            epoch: event.new_epoch,
        });
        Ok(())
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

        if let Ok(new_phase) = EpochPhase::try_from(event.phase) {
            if new_phase != self.state.phase {
                self.state.phase = new_phase;
                changes.push(StateChange::PhaseAdvanced { phase: new_phase });
            }
        }

        changes.push(StateChange::NodeSynced { node: event.node });
        Ok(())
    }

    pub fn handle_advance_pool(
        &mut self,
        node: Pubkey,
        event: &PoolAdvanced,
        changes: &mut Vec<StateChange>,
    ) -> Result<(), FsmError> {
        if let Ok(new_phase) = EpochPhase::try_from(event.phase) {
            if new_phase != self.state.phase {
                self.state.phase = new_phase;
                changes.push(StateChange::PhaseAdvanced { phase: new_phase });
            }
        }

        changes.push(StateChange::PoolAdvanced { node });
        Ok(())
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
