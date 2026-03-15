use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::event::{TapeReserved, TrackRegistered};
use tape_blocks::ParsedInstruction;
use tape_core::spooler::SpoolGroup;
use tape_core::types::SlotNumber;
use tape_crypto::Pubkey;
use tape_protocol::Api;
use tape_store::ops::{SliceOps, SpoolOps, TapeOps, TrackOps};
use tape_store::types::{Pubkey as StorePubkey, TapeInfo, TrackInfo};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::config::ReplayConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;

pub struct ReplayManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: ReplayConfig,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> ReplayManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: ReplayConfig,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            config = ?self.config,
            "replay manager started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::ReplayManager })
                        };
                    };

                    self.persist_block(block)?;
                }
            }
        }
    }

    fn persist_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        for instruction in &block.instructions {
            match instruction {
                ParsedInstruction::RegisterTrack { track, event, .. } => {
                    self.persist_track(*track, event, block.slot)?;
                }
                ParsedInstruction::ReserveTape { tape, event, .. } => {
                    self.persist_tape(*tape, event)?;
                }
                ParsedInstruction::DeleteTrack { track, .. } => {
                    self.delete_track(*track)?;
                }
                ParsedInstruction::DestroyTape { tape, .. } => {
                    self.delete_tape(*tape)?;
                }
                _ => {}
            }
        }

        debug!(
            node_id = self.context.node_id().0,
            slot = block.slot.0,
            entries = block.instructions.len(),
            "replay state persisted"
        );

        Ok(())
    }

    fn persist_track(
        &self,
        track: Pubkey,
        event: &TrackRegistered,
        _slot: SlotNumber,
    ) -> Result<(), NodeError> {
        let spool_group = SpoolGroup::unpack(event.spool_group);
        let stripe_size = u64::from_le_bytes(event.stripe_size);
        let stripe_count = u64::from_le_bytes(event.stripe_count);

        let mut info = TrackInfo {
            tape_address: event.tape.into(),
            spool_group,
            original_size: event.size.0,
            stripe_size,
            stripe_count,
            encoding_type: 0,
            encoding_params: 0,
            commitment: event.leaves.to_vec(),
        };

        info.set_profile(event.profile);

        self.context
            .store
            .put_track(track.into(), info)
            .map_err(store_error)
    }

    fn persist_tape(&self, tape: Pubkey, event: &TapeReserved) -> Result<(), NodeError> {
        self.context
            .store
            .put_tape(
                tape.into(),
                TapeInfo {
                    end_epoch: event.expiry_epoch,
                },
            )
            .map_err(store_error)
    }

    fn delete_track(&self, track: Pubkey) -> Result<(), NodeError> {
        let track_key: StorePubkey = track.into();

        if let Some(info) = self.context.store.get_track(track_key).map_err(store_error)? {
            self.cleanup_slices(track_key, info.spool_group)?;
        }

        self.context
            .store
            .delete_track(track_key)
            .map_err(store_error)
    }

    fn delete_tape(&self, tape: Pubkey) -> Result<(), NodeError> {
        let tape_key: StorePubkey = tape.into();
        let mut cursor = None;

        loop {
            let tracks = self
                .context
                .store
                .iter_tracks_from(cursor, 100)
                .map_err(store_error)?;

            if tracks.is_empty() {
                break;
            }

            for (track, info) in &tracks {
                if info.tape_address == tape_key {
                    self.cleanup_slices(*track, info.spool_group)?;
                    self.context.store.delete_track(*track).map_err(store_error)?;
                }
            }

            cursor = tracks.last().map(|(track, _)| *track);
        }

        self.context
            .store
            .delete_tape(tape_key)
            .map_err(store_error)
    }

    fn cleanup_slices(&self, track: StorePubkey, spool_group: SpoolGroup) -> Result<(), NodeError> {
        let spools = self.context.store.iter_all_spools().map_err(store_error)?;
        for (spool_id, _) in spools {
            if spool_group.contains(spool_id) {
                let _ = self.context.store.delete_slice(spool_id, track);
            }
        }

        Ok(())
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
