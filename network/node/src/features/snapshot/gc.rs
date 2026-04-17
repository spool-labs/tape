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

pub fn clear_stale_snapshot_epochs<Db, Cluster, Blockchain>(
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

pub fn flush_snapshot_write<Db, Cluster, Blockchain>(
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
    let Some(artifact) = ctx
        .store
        .mark_snapshot_artifact_written(epoch, group, chunk, track)
        .map_err(|e| NodeError::Store(format!(
            "mark_snapshot_artifact_written({epoch},{group},{chunk}): {e}"
        )))?
    else {
        return Ok(());
    };

    let Some(my_spool) = local_spool_in_group(ctx, group) else {
        return Ok(());
    };

    ctx.store
        .put_slice(my_spool, track, artifact.local_slice)
        .map_err(|e| NodeError::Store(format!("put_slice({my_spool},{track}): {e}")))?;

    Ok(())
}

pub fn finalize_snapshot_epoch<Db, Cluster, Blockchain>(
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

fn local_spool_in_group<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    group: SpoolGroup,
) -> Option<SpoolIndex> {
    ctx.my_spools()
        .into_iter()
        .find(|spool| group.contains(*spool))
}
