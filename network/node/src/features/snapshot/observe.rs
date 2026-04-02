use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_protocol::ProtocolState;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;

pub async fn observe_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: Arc<ProtocolState>,
) -> Result<(), NodeError> {
    let _ = (context, state);
    todo!("snapshot state observation")
}

pub async fn observe_block<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    block: Arc<ParsedBlock>,
) -> Result<(), NodeError> {
    let _ = (context, block);
    todo!("snapshot block observation")
}
