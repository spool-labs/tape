use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub async fn build_snapshot_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
) -> Result<(), NodeError> {
    let _ = (context, snapshot_epoch);
    todo!("snapshot build algorithm")
}
