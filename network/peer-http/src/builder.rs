use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use peer_manager::PeerManager;
use tape_crypto::ed25519::Keypair;

use crate::HttpApi;
use crate::metrics::ApiMetrics;

pub struct HttpApiBuilder {
    connect_timeout: Duration,
    request_timeout: Duration,
    put_slice_timeout: Duration,
    get_slice_timeout: Duration,
    metrics: Option<Arc<ApiMetrics>>,
    local_identity: Option<Arc<Keypair>>,
}

impl Default for HttpApiBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpApiBuilder {
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            put_slice_timeout: Duration::from_secs(120),
            get_slice_timeout: Duration::from_secs(120),
            metrics: None,
            local_identity: None,
        }
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn put_slice_timeout(mut self, timeout: Duration) -> Self {
        self.put_slice_timeout = timeout;
        self
    }

    pub fn get_slice_timeout(mut self, timeout: Duration) -> Self {
        self.get_slice_timeout = timeout;
        self
    }

    pub fn metrics(mut self, metrics: Arc<ApiMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn local_identity(mut self, identity: Arc<Keypair>) -> Self {
        self.local_identity = Some(identity);
        self
    }

    pub fn build(self, peer_manager: Arc<PeerManager>) -> Result<HttpApi, peer_tls::TlsError> {
        peer_tls::install_default_provider();
        Ok(HttpApi {
            peer_manager,
            clients: Arc::new(DashMap::new()),
            metrics: self.metrics,
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            put_slice_timeout: self.put_slice_timeout,
            get_slice_timeout: self.get_slice_timeout,
            local_identity: self.local_identity,
        })
    }
}
