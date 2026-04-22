//! Self-signed Ed25519 certificate generation.
//!
//! Certs are derived deterministically-ish from a node's persistent TLS
//! keypair each time the node boots. Cert lifetime is effectively unbounded
//! (Unix epoch → year 9999) because pinning, not expiry, is the trust signal.

use std::net::IpAddr;

use rcgen::{
    CertificateParams, DistinguishedName, DnType, Ia5String, KeyPair, PKCS_ED25519, SanType,
    date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tape_crypto::ed25519::Keypair as EdKeypair;

use crate::error::TlsError;

/// A self-signed cert + its private key in rustls-ready DER form.
pub struct SelfSignedCert {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

/// Build a self-signed Ed25519 cert from `keypair` with SANs covering every
/// listen IP in `san_ips`. Validity is Unix epoch → year 9999; pinning
/// verifiers ignore the expiry window when a pin matches.
pub fn self_signed_cert(keypair: &EdKeypair, san_ips: &[IpAddr]) -> Result<SelfSignedCert, TlsError> {
    let pkcs8 = encode_ed25519_pkcs8(keypair);
    let pkcs8_der = PrivatePkcs8KeyDer::from(pkcs8.clone());

    let rcgen_key = KeyPair::from_pkcs8_der_and_sign_algo(&pkcs8_der, &PKCS_ED25519)
        .map_err(|e| TlsError::InvalidKeypair(e.to_string()))?;

    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "tape-node");
    params.not_before = date_time_ymd(1970, 1, 1);
    params.not_after = date_time_ymd(9999, 12, 31);
    params.subject_alt_names = san_ips.iter().copied().map(SanType::IpAddress).collect();

    // Always include a stable DNS SAN derived from the public key so clients
    // that use that name for SNI can still handshake; pinning ignores the name
    // either way, but rustls rejects empty SAN sets.
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

/// PKCS#8 DER encoding of an Ed25519 private key per RFC 5958 / RFC 8410.
fn encode_ed25519_pkcs8(keypair: &EdKeypair) -> Vec<u8> {
    let seed = keypair.secret_key().as_bytes();
    let mut out = Vec::with_capacity(48);
    // SEQUENCE { INTEGER 0, SEQUENCE { OID 1.3.101.112 }, OCTET STRING { OCTET STRING <seed> } }
    out.extend_from_slice(&[
        0x30, 0x2E, // SEQUENCE, length 46
        0x02, 0x01, 0x00, // INTEGER 0 (version)
        0x30, 0x05, // SEQUENCE, length 5 (AlgorithmIdentifier)
        0x06, 0x03, 0x2B, 0x65, 0x70, // OID 1.3.101.112 (Ed25519)
        0x04, 0x22, // OCTET STRING, length 34
        0x04, 0x20, // inner OCTET STRING, length 32
    ]);
    out.extend_from_slice(seed);
    debug_assert_eq!(out.len(), 48);
    out
}

/// Derive a stable DNS name from the Ed25519 pubkey for the cert SAN.
///
/// The name is cosmetic, pinning authenticates the peer, not the name, but
/// rustls requires at least one SAN entry and reqwest will SNI the URL host.
fn dns_name_for_key(keypair: &EdKeypair) -> String {
    let bytes = keypair.public_key().as_bytes();
    let prefix: [u8; 8] = bytes[..8].try_into().expect("32-byte pubkey");
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
    use x509_parser::prelude::FromDer;
    use x509_parser::prelude::X509Certificate;

    use super::*;
    use crate::spki::{ED25519_SPKI_LEN, encode_ed25519_spki};

    #[test]
    fn cert_contains_expected_spki() {
        let mut rng = thread_rng();
        let kp = EdKeypair::new(&mut rng);
        let expected = encode_ed25519_spki(&kp.address());

        let signed = self_signed_cert(&kp, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("cert");
        let (_, parsed) = X509Certificate::from_der(signed.cert.as_ref()).expect("parse");

        assert_eq!(parsed.public_key().raw.len(), ED25519_SPKI_LEN);
        assert_eq!(parsed.public_key().raw, expected.as_slice());
    }

    #[test]
    fn pkcs8_round_trip_matches_seed() {
        let mut rng = thread_rng();
        let kp = EdKeypair::new(&mut rng);
        let pkcs8 = encode_ed25519_pkcs8(&kp);
        assert_eq!(pkcs8.len(), 48);
        assert_eq!(&pkcs8[16..], kp.secret_key().as_bytes());
    }

    #[test]
    fn cert_parses_under_webpki() {
        let mut rng = thread_rng();
        let kp = EdKeypair::new(&mut rng);
        let signed = self_signed_cert(&kp, &[IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]).expect("cert");
        let (_, parsed) = X509Certificate::from_der(signed.cert.as_ref()).expect("parse");
        assert!(
            parsed
                .subject_alternative_name()
                .expect("san")
                .is_some()
        );
    }
}
