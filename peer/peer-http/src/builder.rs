use std::sync::Arc;
use std::time::Duration;

use peer_tls::TlsConfig;
use tape_peer::TrustedPeers;

use crate::metrics::PeerClientMetrics;
use crate::HttpPeerClient;

pub struct HttpPeerClientBuilder {
    connect_timeout: Duration,
    request_timeout: Duration,
    tls: Option<TlsConfig>,
    metrics: Option<Arc<PeerClientMetrics>>,
}

impl Default for HttpPeerClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpPeerClientBuilder {
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            tls: None,
            metrics: None,
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

    pub fn tls(mut self, config: TlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    pub fn metrics(mut self, metrics: Arc<PeerClientMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn build(self) -> Result<HttpPeerClient, peer_tls::TlsError> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout);

        let has_tls_keys = self
            .tls
            .as_ref()
            .is_some_and(|c| !c.server_tls_keys.is_empty());

        if let Some(ref config) = self.tls {
            builder = peer_tls::configure_tls(builder, config)?;
        }

        let client = builder.build().map_err(peer_tls::TlsError::Build)?;

        let scheme = if has_tls_keys { "https" } else { "http" };

        Ok(HttpPeerClient {
            peers: TrustedPeers::new(),
            client,
            metrics: self.metrics,
            scheme,
        })
    }
}
