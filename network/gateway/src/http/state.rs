use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_node::context::NodeContext;
use tape_protocol::Api;

use crate::cache::GatewaySliceCache;
use crate::meter::GatewayMeter;

pub(crate) struct AppState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub(crate) context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    pub(crate) slice_cache: Arc<GatewaySliceCache<Db>>,
    pub(crate) meter: Arc<GatewayMeter>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Clone for AppState<Db, Cluster, Blockchain> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            slice_cache: self.slice_cache.clone(),
            meter: self.meter.clone(),
        }
    }
}
