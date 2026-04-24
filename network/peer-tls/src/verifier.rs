//! TLS server/client cert verification in two modes:
//! - [`TlsVerifier::Webpki`] — standard WebPKI, delegates to rustls for chain
//!   and name validation.
//! - [`TlsVerifier::PinnedPublicKey`] — authenticates the peer solely by
//!   comparing the leaf certificate's SubjectPublicKeyInfo against an expected
//!   P-256 key published on-chain. WebPKI chain/name/expiry are not enforced;
//!   the TLS handshake-signature check (via the crypto provider) still proves
//!   possession of the private key.

use std::fmt;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme};
use tape_core::types::tls::NetworkTlsPubkey;
use x509_parser::prelude::FromDer;
use x509_parser::prelude::X509Certificate;

use crate::provider::ring_provider;
use crate::spki::{P256_SPKI_LEN, encode_p256_spki};

/// Server certificate verifier with two mutually-exclusive modes.
pub enum TlsVerifier {
    /// Standard WebPKI verification against the provided root store. Use for
    /// non-peer HTTPS traffic (public RPC, external services).
    Webpki(Arc<WebPkiServerVerifier>),

    /// Pin the leaf cert to exactly one P-256 public key. Use for peer-to-
    /// peer calls where the pin comes from on-chain `network_tls`.
    PinnedPublicKey(PinnedVerifier),
}

impl TlsVerifier {
    /// Build a WebPKI-mode verifier using the Mozilla root store.
    pub fn webpki_with_mozilla_roots() -> Result<Self, RustlsError> {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
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

    /// Build a pinned verifier for exactly one P-256 public key.
    pub fn pinned(expected: NetworkTlsPubkey) -> Self {
        Self::PinnedPublicKey(PinnedVerifier::new(expected))
    }
}

impl fmt::Debug for TlsVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Webpki(_) => f.debug_tuple("TlsVerifier::Webpki").finish(),
            Self::PinnedPublicKey(p) => f
                .debug_tuple("TlsVerifier::PinnedPublicKey")
                .field(p)
                .finish(),
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
    expected_spki: [u8; P256_SPKI_LEN],
    expected_key: NetworkTlsPubkey,
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
    fn new(expected: NetworkTlsPubkey) -> Self {
        Self {
            expected_spki: encode_p256_spki(&expected),
            expected_key: expected,
            provider: ring_provider(),
        }
    }

    /// The pinned public key.
    pub fn expected_key(&self) -> NetworkTlsPubkey {
        self.expected_key
    }

    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
    ) -> Result<ServerCertVerified, RustlsError> {
        let (_, parsed) = X509Certificate::from_der(end_entity.as_ref())
            .map_err(|_| RustlsError::InvalidCertificate(rustls::CertificateError::BadEncoding))?;

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
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
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

/// Server-side verifier for optional peer mTLS.
///
/// Accepts any well-formed P-256 client certificate. The TLS layer proves
/// key possession via `verify_tls1x_signature`; the caller's identity (the
/// cert's SPKI) is then available from `ServerConnection::peer_certificates()`
/// and can be mapped to a committee member at the application layer.
///
/// `client_auth_mandatory = false` so CLI clients that don't present a cert
/// can still connect for public endpoints.
pub struct PeerClientVerifier {
    provider: Arc<CryptoProvider>,
    root_hints: Vec<rustls::DistinguishedName>,
}

impl fmt::Debug for PeerClientVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerClientVerifier").finish()
    }
}

impl PeerClientVerifier {
    pub fn new() -> Self {
        Self {
            provider: ring_provider(),
            root_hints: Vec::new(),
        }
    }
}

impl Default for PeerClientVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl rustls::server::danger::ClientCertVerifier for PeerClientVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &self.root_hints
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, RustlsError> {
        let (_, parsed) = X509Certificate::from_der(end_entity.as_ref())
            .map_err(|_| RustlsError::InvalidCertificate(rustls::CertificateError::BadEncoding))?;

        // Reject anything that isn't P-256 / prime256v1 / uncompressed so we
        // don't open ourselves up to curve algorithms we haven't audited. The
        // upstream SPKI compare relies on exact P-256 encoding.
        if crate::spki::decode_p256_spki(parsed.public_key().raw).is_none() {
            return Err(RustlsError::InvalidCertificate(
                rustls::CertificateError::BadSignature,
            ));
        }

        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use rand::thread_rng;
    use rustls::pki_types::CertificateDer;
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::p256::Keypair as P256Keypair;

    use super::*;
    use crate::cert::self_signed_cert;
    use crate::provider::install_default;

    fn setup() {
        install_default();
    }

    fn make_cert(kp: &P256Keypair) -> CertificateDer<'static> {
        self_signed_cert(kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .expect("cert")
            .cert
    }

    fn pubkey_of(kp: &P256Keypair) -> NetworkTlsPubkey {
        NetworkTlsPubkey::new(kp.public_key_bytes())
    }

    #[test]
    fn pinned_accepts_matching_key() {
        setup();
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let cert = make_cert(&kp);

        let verifier = PinnedVerifier::new(pubkey_of(&kp));
        verifier
            .verify_server_cert(&cert)
            .expect("accept matching pin");
    }

    #[test]
    fn pinned_rejects_wrong_key() {
        setup();
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let other = P256Keypair::generate(&mut rng);
        let cert = make_cert(&kp);

        let verifier = PinnedVerifier::new(pubkey_of(&other));
        let err = verifier.verify_server_cert(&cert).unwrap_err();
        assert!(matches!(err, RustlsError::InvalidCertificate(_)));
    }

    #[test]
    fn pinned_rejects_random_junk_cert() {
        setup();
        let junk = CertificateDer::from(vec![0u8; 16]);
        let verifier = PinnedVerifier::new(NetworkTlsPubkey::new_unique());
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
