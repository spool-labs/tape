use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;

use crate::context::NodeContext;
use crate::core::error::NodeError;

pub async fn submit_snapshot_group_certification<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<(), NodeError> {
    let _ = context;
    todo!("snapshot group certification submission")
}
