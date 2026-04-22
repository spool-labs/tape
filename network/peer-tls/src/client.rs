//! Client-side TLS builders for reqwest.

use std::sync::Arc;

use rustls::ClientConfig;
use tape_crypto::address::Address;

use crate::error::TlsError;
use crate::provider::ring_provider;
use crate::verifier::TlsVerifier;

/// Build a reqwest client that pins the peer's server cert to exactly one
/// Ed25519 public key. Use for peer-to-peer calls; the `expected` key comes
/// from the peer's on-chain `network_tls` field.
pub fn pinned_client(expected: Address) -> Result<reqwest::Client, TlsError> {
    let verifier = Arc::new(TlsVerifier::pinned(expected));
    let tls = ClientConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::BuildServer(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();

    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .tls_built_in_root_certs(false)
        .build()
        .map_err(TlsError::BuildClient)
}

/// Apply a pinned-public-key TLS verifier to an existing reqwest builder.
/// Caller owns timeouts, headers, and other knobs.
pub fn apply_pinned_tls(
    builder: reqwest::ClientBuilder,
    expected: Address,
) -> Result<reqwest::ClientBuilder, TlsError> {
    let verifier = Arc::new(TlsVerifier::pinned(expected));
    let tls = ClientConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::BuildServer(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();

    Ok(builder
        .use_preconfigured_tls(tls)
        .tls_built_in_root_certs(false))
}

/// Apply standard WebPKI server verification (Mozilla root store) to an
/// existing reqwest builder. Use for SDK / non-peer HTTPS.
pub fn apply_webpki_tls(
    builder: reqwest::ClientBuilder,
) -> Result<reqwest::ClientBuilder, TlsError> {
    let verifier = Arc::new(
        TlsVerifier::webpki_with_mozilla_roots()
            .map_err(|e| TlsError::BuildServer(e.to_string()))?,
    );
    let tls = ClientConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::BuildServer(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();

    Ok(builder.use_preconfigured_tls(tls))
}
