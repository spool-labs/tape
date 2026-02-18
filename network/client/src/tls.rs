//! TLS certificate pinning for node-to-node communication.

use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use crate::Pubkey;

/// Server certificate verifier that pins the leaf cert's public key bytes.
pub struct PinnedServerCertVerifier {
    provider: Arc<CryptoProvider>,
    allowed_pubkeys: HashSet<Pubkey>,
}

impl PinnedServerCertVerifier {
    /// Create a verifier that accepts only certificates whose Ed25519 public key
    /// matches one of the given keys.
    pub fn new<I: IntoIterator<Item = Pubkey>>(keys: I) -> Self {
        Self {
            provider: Arc::new(rustls::crypto::ring::default_provider()),
            allowed_pubkeys: keys.into_iter().collect(),
        }
    }

    /// Extract the SubjectPublicKeyInfo bytes from a DER certificate.
    fn extract_pubkey(cert_der: &CertificateDer<'_>) -> Result<Pubkey, RustlsError> {
        let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
            .map_err(|_| RustlsError::InvalidCertificate(rustls::CertificateError::BadEncoding))?;

        let spki = cert.public_key().subject_public_key.as_ref();

        // Ed25519 public keys are 32 bytes
        if spki.len() == 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(spki);
            return Ok(Pubkey::new(key));
        }

        // Some encodings wrap the key in ASN.1; try the last 32 bytes
        if spki.len() > 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&spki[spki.len() - 32..]);
            return Ok(Pubkey::new(key));
        }

        Err(RustlsError::InvalidCertificate(
            rustls::CertificateError::BadEncoding,
        ))
    }
}

impl Debug for PinnedServerCertVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedServerCertVerifier")
            .field("pinned_keys", &self.allowed_pubkeys.len())
            .finish()
    }
}

impl ServerCertVerifier for PinnedServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        let pubkey = Self::extract_pubkey(end_entity)?;
        if self.allowed_pubkeys.contains(&pubkey) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(RustlsError::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
