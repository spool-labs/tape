use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::{EventLogOps, SliceOps, SnapshotOps};

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub fn clear_snapshot_data<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    keep_epoch: EpochNumber,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    ctx.store
        .delete_snapshot_epochs_except(keep_epoch)
        .map_err(|e| NodeError::Store(format!("delete_snapshot_epochs_except({keep_epoch}): {e}")))
}

pub fn move_snapshot_data<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    track: Address,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    todo!();

    ctx.store
        .put_slice(my_spool, track, artifact.local_slice)
        .map_err(|e| NodeError::Store(format!("put_slice({my_spool},{track}): {e}")))?;

    Ok(())
}

pub fn advance_snapshot_epoch<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    ctx.store
        .delete_snapshot_epoch(epoch)
        .map_err(|e| NodeError::Store(format!("delete_snapshot_epoch({epoch}): {e}")))?;

    ctx.store
        .delete_epoch_events(epoch)
        .map_err(|e| NodeError::Store(format!("delete_epoch_events({epoch}): {e}")))?;

    Ok(())
}

