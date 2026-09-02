use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_node::context::NodeContext;
use tape_protocol::Api;

use crate::admission::Admission;
use crate::staging::StagingStore;
use crate::cache::GatewaySliceCache;
use crate::http::handlers::s3::accounting::Accounting;
use crate::http::handlers::s3::write::S3WriteContext;
use crate::http::handlers::site::hosts::SiteHostBindings;
use crate::meter::GatewayMeter;

pub struct AppState<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    pub slice_cache: Arc<GatewaySliceCache<Db>>,
    pub meter: Arc<GatewayMeter>,
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
    /// TXT-proven host bindings for self-serve site domains. `None` when
    /// txt domains are disabled or no system resolver is available.
    pub site_hosts: Option<Arc<SiteHostBindings>>,
    /// Writes acknowledged to the client but not yet applied on chain.
    ///
    /// Reads and listings serve from here until the drain lands the write and
    /// the ingestor indexes it, which is what gives an S3 client
    /// read-after-write without waiting a block per object.
    pub staging: Arc<StagingStore<Db>>,
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
            site_hosts: self.site_hosts.clone(),
            staging: self.staging.clone(),
        }
    }
}

