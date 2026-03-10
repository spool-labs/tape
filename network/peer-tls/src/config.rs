use std::path::PathBuf;
use std::sync::Arc;

use tape_crypto::Pubkey;

use crate::identity::load_identity;
use crate::pinning::PinnedServerCertVerifier;

/// TLS configuration for peer connections.
pub struct TlsConfig {
    pub server_tls_keys: Vec<Pubkey>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
}

/// Apply TLS pinning and mTLS identity to a `reqwest::ClientBuilder`.
///
/// The caller controls timeouts and other settings; this function only touches TLS.
pub fn configure_tls(
    mut builder: reqwest::ClientBuilder,
    config: &TlsConfig,
) -> Result<reqwest::ClientBuilder, TlsError> {
    if !config.server_tls_keys.is_empty() {
        let verifier = PinnedServerCertVerifier::new(config.server_tls_keys.clone());
        let tls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();

        builder = builder
            .use_preconfigured_tls(tls_config)
            .tls_built_in_root_certs(false);
    }

    if let (Some(cert_path), Some(key_path)) =
        (&config.client_cert_path, &config.client_key_path)
    {
        let identity = load_identity(cert_path, key_path)?;
        builder = builder.identity(identity);
    }

    Ok(builder)
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("read cert: {0}")]
    ReadCert(#[source] std::io::Error),
    #[error("parse certs: {0}")]
    ParseCert(String),
    #[error("read key: {0}")]
    ReadKey(#[source] std::io::Error),
    #[error("no private key found")]
    NoPrivateKey,
    #[error("identity: {0}")]
    Identity(String),
    #[error("build client: {0}")]
    Build(#[source] reqwest::Error),
}
