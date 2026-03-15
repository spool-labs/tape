use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;

use crate::core::context::NodeContext;
use crate::features::http::error::RouteError;

pub struct AppState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub context: Arc<NodeContext<Db, Cluster, Blockchain>>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Clone for AppState<Db, Cluster, Blockchain> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}

pub fn current_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
) -> Result<EpochNumber, RouteError> {
    let epoch = state.context.state().epoch;
    if epoch.is_zero() {
        return Err(RouteError::BadRequest("chain epoch missing".into()));
    }

    Ok(epoch)
}
