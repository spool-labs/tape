use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::types::SnapshotManagerInput;

pub async fn collect_snapshot_signatures<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    input: SnapshotManagerInput,
) -> Result<(), NodeError> {
    let _ = (context, input);
    todo!("snapshot signature collection")
}
