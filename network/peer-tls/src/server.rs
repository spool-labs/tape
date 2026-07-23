//! Server-side TLS configuration. Builds a `rustls::ServerConfig` from a node
//! Ed25519 TLS keypair; consumers hand it to axum-server.

use std::net::IpAddr;
use std::sync::Arc;

use rustls::ServerConfig;
use tape_crypto::ed25519::Keypair as EdKeypair;

use crate::cert::self_signed_cert;
use crate::error::TlsError;
use crate::provider::ring_provider;
use crate::verifier::PeerClientVerifier;

/// Build a `rustls::ServerConfig` that presents a self-signed Ed25519 cert
/// derived from `keypair`, with SANs for each listen IP.
///
/// No client authentication: CLI clients connect anonymously.
pub fn build_server_config(
    keypair: &EdKeypair,
    san_ips: &[IpAddr],
) -> Result<Arc<ServerConfig>, TlsError> {
    let signed = self_signed_cert(keypair, san_ips)?;

    let config = ServerConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::BuildServer(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(vec![signed.cert], signed.key)
        .map_err(|e| TlsError::BuildServer(e.to_string()))?;

    Ok(Arc::new(config))
}

/// Build a `rustls::ServerConfig` with optional peer mTLS: connections may
/// present an Ed25519 client cert for later authorization checks, but are not
/// required to (CLI clients still succeed without one).
pub fn build_server_config_with_peer_auth(
    keypair: &EdKeypair,
    san_ips: &[IpAddr],
) -> Result<Arc<ServerConfig>, TlsError> {
    let signed = self_signed_cert(keypair, san_ips)?;
    let verifier = Arc::new(PeerClientVerifier::new());

    let config = ServerConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::BuildServer(e.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![signed.cert], signed.key)
        .map_err(|e| TlsError::BuildServer(e.to_string()))?;

    Ok(Arc::new(config))
}
