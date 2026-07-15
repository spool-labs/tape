use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_node::context::NodeContext;
use tape_protocol::Api;

use crate::admission::Admission;
use crate::cache::GatewaySliceCache;
use crate::http::handlers::s3::accounting::Accounting;
use crate::http::handlers::s3::write::S3WriteContext;
use crate::meter::GatewayMeter;

pub(crate) struct AppState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub(crate) context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    pub(crate) slice_cache: Arc<GatewaySliceCache<Db>>,
    pub(crate) meter: Arc<GatewayMeter>,
    /// Delegate signing context for the S3 write path. `None` on the native read
    /// listener and whenever `gateway.s3.delegate_key` is unset (writes
    /// unavailable). Shared (Arc) so it is cheap to clone with the state.
    pub write_ctx: Option<Arc<S3WriteContext>>,
    /// Write-authorization accounting state: the ledger RMW lock and the on-chain
    /// precondition cache (see [`Accounting`]). Shared (Arc) across listeners.
    pub accounting: Arc<Accounting>,
    /// Admission gate consulted at the write chokepoint; injected by an
    /// embedder, everything else admits all writes
    pub admission: Arc<dyn Admission>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> Clone for AppState<Db, Cluster, Blockchain> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            slice_cache: self.slice_cache.clone(),
            meter: self.meter.clone(),
            write_ctx: self.write_ctx.clone(),
            accounting: self.accounting.clone(),
            admission: self.admission.clone(),
        }
    }
}
