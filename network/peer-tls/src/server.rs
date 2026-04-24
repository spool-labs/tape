//! Server-side TLS configuration. Builds a `rustls::ServerConfig` from a node
//! P-256 TLS keypair (self-signed mode) or from an operator-supplied PEM cert
//! chain paired with the same keypair (CA-issued mode).

use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tape_crypto::p256::Keypair as P256Keypair;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::cert::self_signed_cert;
use crate::error::TlsError;
use crate::provider::ring_provider;
use crate::spki::encode_p256_spki;
use crate::verifier::PeerClientVerifier;

/// Build a `rustls::ServerConfig` that presents a self-signed P-256 cert
/// derived from `keypair`, with SANs for each listen IP.
///
/// No client authentication: CLI clients connect anonymously.
pub fn build_server_config(
    keypair: &P256Keypair,
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
/// present a P-256 client cert for later authorization checks, but are not
/// required to (CLI clients still succeed without one).
pub fn build_server_config_with_peer_auth(
    keypair: &P256Keypair,
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

/// Build a `rustls::ServerConfig` using an operator-supplied PEM certificate
/// chain paired with the node's local P-256 keypair.
///
/// The leaf certificate's SubjectPublicKeyInfo MUST match `keypair`'s SPKI;
/// this is enforced at startup. A mismatch means the operator pointed at a
/// cert issued for a different key, which would serve browsers correctly but
/// silently break every SDK pin — refuse to start rather than ship that.
///
/// `with_peer_auth` controls whether incoming peers may present mTLS client
/// certs (mirrors [`build_server_config_with_peer_auth`]).
pub fn build_server_config_from_pem(
    pem_cert_path: &Path,
    keypair: &P256Keypair,
    with_peer_auth: bool,
) -> Result<Arc<ServerConfig>, TlsError> {
    let chain = load_pem_cert_chain(pem_cert_path)?;
    let leaf = chain
        .first()
        .ok_or_else(|| TlsError::PemCert(format!("{} contained no certificates", pem_cert_path.display())))?;
    verify_leaf_matches_keypair(leaf, keypair)?;

    let pkcs8 = keypair
        .to_pkcs8_der()
        .map_err(|e| TlsError::InvalidKeypair(e.to_string()))?;
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8));

    let builder = ServerConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::BuildServer(e.to_string()))?;

    let config = if with_peer_auth {
        builder
            .with_client_cert_verifier(Arc::new(PeerClientVerifier::new()))
            .with_single_cert(chain, key)
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(chain, key)
    }
    .map_err(|e| TlsError::BuildServer(e.to_string()))?;

    Ok(Arc::new(config))
}

/// Read a PEM file and return every `CERTIFICATE` block it contains, in order.
fn load_pem_cert_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let bytes = fs::read(path)
        .map_err(|e| TlsError::PemCert(format!("read {}: {e}", path.display())))?;
    let mut reader = bytes.as_slice();
    let mut chain = Vec::new();
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert = cert
            .map_err(|e| TlsError::PemCert(format!("parse {}: {e}", path.display())))?;
        chain.push(cert);
    }
    Ok(chain)
}

/// Confirm the PEM leaf cert's SPKI equals the local keypair's SPKI.
fn verify_leaf_matches_keypair(
    leaf: &CertificateDer<'_>,
    keypair: &P256Keypair,
) -> Result<(), TlsError> {
    let (_, parsed) = X509Certificate::from_der(leaf.as_ref())
        .map_err(|e| TlsError::PemCert(format!("parse leaf cert: {e}")))?;
    let leaf_spki = parsed.public_key().raw;
    let expected_pubkey = tape_core::types::tls::NetworkTlsPubkey::new(keypair.public_key_bytes());
    let expected_spki = encode_p256_spki(&expected_pubkey);
    if leaf_spki != expected_spki.as_slice() {
        return Err(TlsError::PemCert(
            "leaf certificate SPKI does not match local TLS keypair; \
             the PEM cert must be issued to the same P-256 key whose pubkey is on-chain"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::Ipv4Addr;

    use rand::thread_rng;
    use tempfile::NamedTempFile;

    use super::*;

    fn write_pem_cert(pem: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(pem.as_bytes()).expect("write");
        f
    }

    fn self_signed_pem(keypair: &P256Keypair) -> String {
        use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
        let pkcs8 = keypair.to_pkcs8_der().expect("pkcs8");
        let pkcs8_der = rustls::pki_types::PrivatePkcs8KeyDer::from(pkcs8);
        let key =
            KeyPair::from_pkcs8_der_and_sign_algo(&pkcs8_der, &PKCS_ECDSA_P256_SHA256).expect("key");
        let params = CertificateParams::new(vec!["tape-node.example.com".into()]).expect("params");
        let cert = params.self_signed(&key).expect("sign");
        cert.pem()
    }

    #[test]
    fn from_pem_accepts_matching_keypair() {
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let pem = self_signed_pem(&kp);
        let file = write_pem_cert(&pem);
        build_server_config_from_pem(file.path(), &kp, false).expect("accept matching pem");
    }

    #[test]
    fn from_pem_rejects_mismatched_keypair() {
        let mut rng = thread_rng();
        let cert_kp = P256Keypair::generate(&mut rng);
        let local_kp = P256Keypair::generate(&mut rng);
        let pem = self_signed_pem(&cert_kp);
        let file = write_pem_cert(&pem);
        let err = build_server_config_from_pem(file.path(), &local_kp, false).unwrap_err();
        match err {
            TlsError::PemCert(msg) => assert!(msg.contains("SPKI")),
            other => panic!("expected PemCert error, got: {other}"),
        }
    }

    #[test]
    fn from_pem_rejects_empty_file() {
        let file = write_pem_cert("");
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let err = build_server_config_from_pem(file.path(), &kp, false).unwrap_err();
        assert!(matches!(err, TlsError::PemCert(_)));
    }

    #[test]
    fn self_signed_mode_builds() {
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let _ = build_server_config(&kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("build");
    }
}
