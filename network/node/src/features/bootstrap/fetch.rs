//! Fetch and decode snapshot logs from the network during bootstrap, and
//! persist the verified chunk-track metadata so a late node takes durable
//! custody. The `enumerate -> verify -> fetch -> decode` pipeline lives in
//! `tape_protocol::snapshot`; this wires it to the node's rpc/api/state.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
use tape_core::tape::{snapshot_tape_number, TapeFlags};
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_protocol::{read_snapshot_epoch, Api, DecodedSnapshot};
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Fetch every chunk for an epoch's snapshot tape from peers, verify against the
/// on-chain committed root, decode, and return the reconstructed `SnapshotLog`.
pub async fn fetch_and_decode_epoch<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<DecodedSnapshot, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let tape = Address::from(snapshot_tape_pda(epoch).0);

    // The committed merkle root the snapshot was voted on. The
    // reader verifies every chunk list against this before it
    // trusts metadata or decodes slices.
    let committed = context
        .rpc
        .get_snapshot_tape(epoch)
        .await
        .map_err(NodeError::Rpc)?;

    let state = context.state();

    read_snapshot_epoch(
        &context.api, 
        state.as_ref(), 
        &committed.tracks, 
        tape, 
        epoch, 
        cancel
    )
        .await
        .map_err(|error| NodeError::Store(error.to_string()))
}

/// Materialize the snapshot tape and its chunk-track metadata after a bootstrap
/// replay, so the node takes the same custody entry point a builder would have.
///
/// Mirrors `persist_snapshot_candidate` minus the blob/slice data: the chunk
/// tracks are written as `ObjectInfo::System { Snapshot }` (always certified),
/// and the generic spool sync/repair then fetches the slices for owned spools.
/// `decoded.tracks` was verified against the committed merkle root before decode.
pub fn persist_snapshot_metadata<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    epoch: EpochNumber,
    decoded: &DecodedSnapshot,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let snapshot_tape = snapshot_tape_pda(epoch).0;

    context
        .store
        .put_tape(
            snapshot_tape,
            TapeInfo {
                id: snapshot_tape_number(epoch),
                flags: TapeFlags::SYSTEM,
                end_epoch: EpochNumber(u64::MAX),
                next_track_number: TrackNumber(decoded.tracks.len() as u64),
            },
        )
        .map_err(store_err)?;

    for snapshot_track in &decoded.tracks {
        let track = snapshot_track.state;
        let track_address = track_pda(track.tape, track.track_number).0;

        context
            .store
            .put_track(track_address, track)
            .map_err(store_err)?;

        context
            .store
            .put_object_info(
                track_address,
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot { epoch },
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: decoded.log.end_slot,
                },
            )
            .map_err(store_err)?;
    }

    Ok(())
}

fn store_err(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
