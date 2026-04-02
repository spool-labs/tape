use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub async fn download_snapshot_group<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<(), NodeError> {
    let _ = context;
    todo!("snapshot download orchestration")
}
