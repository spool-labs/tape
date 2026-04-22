//! TLS server-cert verification in two modes:
//! - [`TlsVerifier::Webpki`] — standard WebPKI, delegates to rustls for chain
//!   and name validation.
//! - [`TlsVerifier::PinnedPublicKey`] — authenticates the peer solely by
//!   comparing the leaf certificate's SubjectPublicKeyInfo against an expected
//!   Ed25519 key published on-chain. WebPKI chain/name/expiry are not
//!   enforced; the TLS handshake-signature check (via the crypto provider)
//!   still proves possession of the private key.

use std::fmt;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme};
use tape_crypto::address::Address;
use x509_parser::prelude::FromDer;
use x509_parser::prelude::X509Certificate;

use crate::provider::ring_provider;
use crate::spki::{ED25519_SPKI_LEN, encode_ed25519_spki};

/// Server certificate verifier with two mutually-exclusive modes.
pub enum TlsVerifier {
    /// Standard WebPKI verification against the provided root store. Use for
    /// non-peer HTTPS traffic (public RPC, external services).
    Webpki(Arc<WebPkiServerVerifier>),

    /// Pin the leaf cert to exactly one Ed25519 public key. Use for peer-to-
    /// peer calls where the pin comes from on-chain `network_tls`.
    PinnedPublicKey(PinnedVerifier),
}

impl TlsVerifier {
    /// Build a WebPKI-mode verifier using the Mozilla root store.
    pub fn webpki_with_mozilla_roots() -> Result<Self, RustlsError> {
        let mut roots = RootCertStore::empty();
        roots.extend(
            webpki_roots::TLS_SERVER_ROOTS
                .iter()
                .cloned(),
        );
        Self::webpki(roots)
    }

    /// Build a WebPKI-mode verifier using a caller-supplied root store.
    pub fn webpki(roots: RootCertStore) -> Result<Self, RustlsError> {
        let provider = ring_provider();
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|e| RustlsError::Other(rustls::OtherError(Arc::new(WebPkiBuildError(e)))))?;
        Ok(Self::Webpki(inner))
    }

    /// Build a pinned verifier for exactly one Ed25519 public key.
    pub fn pinned(expected: Address) -> Self {
        Self::PinnedPublicKey(PinnedVerifier::new(expected))
    }
}

impl fmt::Debug for TlsVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Webpki(_) => f.debug_tuple("TlsVerifier::Webpki").finish(),
            Self::PinnedPublicKey(p) => f.debug_tuple("TlsVerifier::PinnedPublicKey").field(p).finish(),
        }
    }
}

impl ServerCertVerifier for TlsVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        match self {
            Self::Webpki(inner) => {
                inner.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
            }
            Self::PinnedPublicKey(p) => p.verify_server_cert(end_entity),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        match self {
            Self::Webpki(inner) => inner.verify_tls12_signature(message, cert, dss),
            Self::PinnedPublicKey(p) => p.verify_tls12_signature(message, cert, dss),
        }
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        match self {
            Self::Webpki(inner) => inner.verify_tls13_signature(message, cert, dss),
            Self::PinnedPublicKey(p) => p.verify_tls13_signature(message, cert, dss),
        }
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        match self {
            Self::Webpki(inner) => inner.supported_verify_schemes(),
            Self::PinnedPublicKey(p) => p.supported_schemes(),
        }
    }
}

/// Pinned-public-key verifier. Constructed via [`TlsVerifier::pinned`].
pub struct PinnedVerifier {
    expected_spki: [u8; ED25519_SPKI_LEN],
    expected_key: Address,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for PinnedVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedVerifier")
            .field("expected_key", &self.expected_key)
            .finish()
    }
}

impl PinnedVerifier {
    fn new(expected: Address) -> Self {
        Self {
            expected_spki: encode_ed25519_spki(&expected),
            expected_key: expected,
            provider: ring_provider(),
        }
    }

    /// The pinned public key.
    pub fn expected_key(&self) -> Address {
        self.expected_key
    }

    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
    ) -> Result<ServerCertVerified, RustlsError> {
        let (_, parsed) = X509Certificate::from_der(end_entity.as_ref()).map_err(|_| {
            RustlsError::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;

        let leaf_spki = parsed.public_key().raw;
        if leaf_spki != self.expected_spki.as_slice() {
            return Err(RustlsError::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }

        Ok(ServerCertVerified::assertion())
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

    fn supported_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

#[derive(Debug)]
struct WebPkiBuildError(rustls::client::VerifierBuilderError);

impl fmt::Display for WebPkiBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "build webpki verifier: {:?}", self.0)
    }
}

impl std::error::Error for WebPkiBuildError {}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use rand::thread_rng;
    use rustls::pki_types::CertificateDer;
    use tape_crypto::address::Address;
    use tape_crypto::ed25519::Keypair as EdKeypair;

    use super::*;
    use crate::cert::self_signed_cert;
    use crate::provider::install_default;

    fn setup() {
        install_default();
    }

    fn make_cert(kp: &EdKeypair) -> CertificateDer<'static> {
        self_signed_cert(kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .expect("cert")
            .cert
    }

    #[test]
    fn pinned_accepts_matching_key() {
        setup();
        let mut rng = thread_rng();
        let kp = EdKeypair::new(&mut rng);
        let cert = make_cert(&kp);

        let verifier = PinnedVerifier::new(kp.address());
        verifier.verify_server_cert(&cert).expect("accept matching pin");
    }

    #[test]
    fn pinned_rejects_wrong_key() {
        setup();
        let mut rng = thread_rng();
        let kp = EdKeypair::new(&mut rng);
        let other = EdKeypair::new(&mut rng);
        let cert = make_cert(&kp);

        let verifier = PinnedVerifier::new(other.address());
        let err = verifier.verify_server_cert(&cert).unwrap_err();
        assert!(matches!(err, RustlsError::InvalidCertificate(_)));
    }

    #[test]
    fn pinned_rejects_random_junk_cert() {
        setup();
        let junk = CertificateDer::from(vec![0u8; 16]);
        let verifier = PinnedVerifier::new(Address::new_unique());
        assert!(verifier.verify_server_cert(&junk).is_err());
    }

    #[test]
    fn webpki_mode_constructs() {
        setup();
        let v = TlsVerifier::webpki_with_mozilla_roots().expect("mozilla roots build");
        match v {
            TlsVerifier::Webpki(_) => {}
            _ => panic!("expected webpki variant"),
        }
    }
}
