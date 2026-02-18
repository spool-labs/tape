//! Builder for `NodeClient` instances.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use url::Url;

use crate::client::NodeClient;
use crate::error::NodeError;
use crate::metrics::NodeClientMetrics;
use crate::tls::PinnedServerCertVerifier;
use crate::Pubkey;

/// Builder for creating `NodeClient` instances.
pub struct NodeClientBuilder {
    connect_timeout: Duration,
    request_timeout: Duration,
    server_tls_keys: Vec<Pubkey>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    metrics: Option<Arc<NodeClientMetrics>>,
}

impl Default for NodeClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeClientBuilder {
    /// Create a new builder with default timeouts.
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            server_tls_keys: Vec::new(),
            client_cert_path: None,
            client_key_path: None,
            metrics: None,
        }
    }

    /// Set the TCP connect timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the per-request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Attach Prometheus metrics.
    pub fn with_metrics(mut self, metrics: Arc<NodeClientMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Pin a single server TLS public key.
    pub fn server_tls_key(mut self, key: Pubkey) -> Self {
        self.server_tls_keys.push(key);
        self
    }

    /// Pin multiple server TLS public keys.
    pub fn server_tls_keys<I: IntoIterator<Item = Pubkey>>(mut self, keys: I) -> Self {
        self.server_tls_keys.extend(keys);
        self
    }

    /// Set client certificate and key paths for mTLS.
    pub fn with_client_paths(
        mut self,
        cert_path: Option<PathBuf>,
        key_path: Option<PathBuf>,
    ) -> Self {
        self.client_cert_path = cert_path;
        self.client_key_path = key_path;
        self
    }

    /// Build a `NodeClient` from a host:port address string.
    pub fn build(self, address: &str) -> Result<NodeClient, NodeError> {
        let value = if address.contains("://") {
            address.to_string()
        } else {
            format!("http://{address}")
        };
        let url: Url = value
            .parse()
            .map_err(NodeError::Url)?;
        self.build_with_url(url)
    }

    /// Build a `NodeClient` from a pre-constructed URL.
    pub fn build_with_url(self, url: Url) -> Result<NodeClient, NodeError> {
        let inner = self.build_client()?;
        Ok(NodeClient {
            inner,
            base_url: url,
            metrics: self.metrics,
        })
    }

    fn build_client(&self) -> Result<Client, NodeError> {
        let mut builder = Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout);

        // TLS pinning
        if !self.server_tls_keys.is_empty() {
            let verifier = PinnedServerCertVerifier::new(self.server_tls_keys.clone());
            let tls_config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();

            builder = builder
                .use_preconfigured_tls(tls_config)
                .tls_built_in_root_certs(false);
        }

        // Client certs (mTLS)
        if let (Some(cert_path), Some(key_path)) =
            (&self.client_cert_path, &self.client_key_path)
        {
            let (certs, key) =
                load_client_keys(cert_path, key_path)?;
            let mut pem_buf = pem_encode_certs(&certs);
            pem_buf.extend_from_slice(&pem_encode_key(&key));
            let identity = reqwest::Identity::from_pem(&pem_buf)
                .map_err(|e| NodeError::Tls(format!("identity: {e}")))?;
            builder = builder.identity(identity);
        }

        builder.build().map_err(NodeError::Request)
    }
}

fn load_client_keys(
    cert_path: &PathBuf,
    key_path: &PathBuf,
) -> Result<
    (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    NodeError,
> {
    let cert_data =
        std::fs::read(cert_path).map_err(|e| NodeError::Tls(format!("read cert: {e}")))?;
    let key_data =
        std::fs::read(key_path).map_err(|e| NodeError::Tls(format!("read key: {e}")))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_data[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| NodeError::Tls(format!("parse certs: {e}")))?;

    let key = rustls_pemfile::private_key(&mut &key_data[..])
        .map_err(|e| NodeError::Tls(format!("parse key: {e}")))?
        .ok_or_else(|| NodeError::Tls("no private key found".into()))?;

    Ok((certs, key))
}

fn pem_encode_certs(certs: &[rustls::pki_types::CertificateDer<'_>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for cert in certs {
        buf.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
        buf.extend_from_slice(base64_encode(cert.as_ref()).as_bytes());
        buf.extend_from_slice(b"\n-----END CERTIFICATE-----\n");
    }
    buf
}

fn pem_encode_key(key: &rustls::pki_types::PrivateKeyDer<'_>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"-----BEGIN PRIVATE KEY-----\n");
    buf.extend_from_slice(base64_encode(key.secret_der()).as_bytes());
    buf.extend_from_slice(b"\n-----END PRIVATE KEY-----\n");
    buf
}

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(chars[((triple >> 18) & 0x3F) as usize] as char);
        out.push(chars[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(chars[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(chars[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    // Insert newlines every 76 chars for PEM
    let mut formatted = String::new();
    for (i, c) in out.chars().enumerate() {
        if i > 0 && i % 76 == 0 {
            let _ = write!(formatted, "\n");
        }
        formatted.push(c);
    }
    formatted
}
