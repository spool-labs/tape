//! Self-signed P-256 certificate generation.
//!
//! Certs are derived from a node's persistent P-256 TLS keypair each time the
//! node boots. Cert lifetime is effectively unbounded (Unix epoch → year 9999)
//! because pinning, not expiry, is the trust signal for peer clients.
//!
//! Operators who own a domain can instead drop in a CA-issued cert (see
//! [`crate::server::build_server_config_from_pem`]). The CA cert must have
//! been issued to the same P-256 keypair; the server refuses to start on
//! mismatch, and the SDK's SPKI pin works identically for both modes.

use std::net::IpAddr;

use rcgen::{
    CertificateParams, DistinguishedName, DnType, Ia5String, KeyPair, PKCS_ECDSA_P256_SHA256,
    SanType, date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tape_crypto::p256::Keypair as P256Keypair;

use crate::error::TlsError;

/// A self-signed cert + its private key in rustls-ready DER form.
pub struct SelfSignedCert {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

/// Build a self-signed P-256 cert from `keypair` with SANs covering every
/// listen IP in `san_ips`. Validity is Unix epoch → year 9999; pinning
/// verifiers ignore the expiry window when a pin matches.
pub fn self_signed_cert(
    keypair: &P256Keypair,
    san_ips: &[IpAddr],
) -> Result<SelfSignedCert, TlsError> {
    let pkcs8 = keypair
        .to_pkcs8_der()
        .map_err(|e| TlsError::InvalidKeypair(e.to_string()))?;
    let pkcs8_der = PrivatePkcs8KeyDer::from(pkcs8.clone());

    let rcgen_key = KeyPair::from_pkcs8_der_and_sign_algo(&pkcs8_der, &PKCS_ECDSA_P256_SHA256)
        .map_err(|e| TlsError::InvalidKeypair(e.to_string()))?;

    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "tape-node");
    params.not_before = date_time_ymd(1970, 1, 1);
    params.not_after = date_time_ymd(9999, 12, 31);
    params.subject_alt_names = san_ips.iter().copied().map(SanType::IpAddress).collect();

    // rustls rejects an empty SAN list, and reqwest SNIs the URL hostname —
    // add a stable hash-derived DNS name so pinned clients that dial by name
    // still handshake. Pinning ignores the name.
    let dns_name = dns_name_for_key(keypair);
    if let Ok(ia5) = Ia5String::try_from(dns_name) {
        params.subject_alt_names.push(SanType::DnsName(ia5));
    }

    let cert = params
        .self_signed(&rcgen_key)
        .map_err(|e| TlsError::CertGeneration(e.to_string()))?;

    Ok(SelfSignedCert {
        cert: cert.der().clone(),
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8)),
    })
}

/// Derive a stable DNS name from the P-256 pubkey for the cert SAN. Cosmetic.
fn dns_name_for_key(keypair: &P256Keypair) -> String {
    let bytes = keypair.public_key_bytes();
    let prefix: [u8; 8] = bytes[..8]
        .try_into()
        .expect("pubkey has at least 8 bytes");
    format!("{}.tape.peer", hex_lower(&prefix))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0F) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use rand::thread_rng;
    use tape_core::types::tls::NetworkTlsPubkey;
    use x509_parser::prelude::FromDer;
    use x509_parser::prelude::X509Certificate;

    use super::*;
    use crate::spki::{P256_SPKI_LEN, encode_p256_spki};

    #[test]
    fn cert_contains_expected_spki() {
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let expected = encode_p256_spki(&NetworkTlsPubkey::new(kp.public_key_bytes()));

        let signed = self_signed_cert(&kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("cert");
        let (_, parsed) = X509Certificate::from_der(signed.cert.as_ref()).expect("parse");

        assert_eq!(parsed.public_key().raw.len(), P256_SPKI_LEN);
        assert_eq!(parsed.public_key().raw, expected.as_slice());
    }

    #[test]
    fn cert_has_ip_san() {
        let mut rng = thread_rng();
        let kp = P256Keypair::generate(&mut rng);
        let signed =
            self_signed_cert(&kp, &[IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]).expect("cert");
        let (_, parsed) = X509Certificate::from_der(signed.cert.as_ref()).expect("parse");
        assert!(parsed.subject_alternative_name().expect("san").is_some());
    }
}
