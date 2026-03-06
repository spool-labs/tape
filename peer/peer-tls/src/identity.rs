use std::path::Path;

use crate::TlsError;

/// Load client certificate and key from PEM files and return a `reqwest::Identity`.
pub fn load_identity(cert_path: &Path, key_path: &Path) -> Result<reqwest::Identity, TlsError> {
    let (certs, key) = load_client_keys(cert_path, key_path)?;
    let mut pem_buf = pem_encode_certs(&certs);
    pem_buf.extend_from_slice(&pem_encode_key(&key));
    reqwest::Identity::from_pem(&pem_buf).map_err(|e| TlsError::Identity(e.to_string()))
}

fn load_client_keys(
    cert_path: &Path,
    key_path: &Path,
) -> Result<
    (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    TlsError,
> {
    let cert_data = std::fs::read(cert_path).map_err(TlsError::ReadCert)?;
    let key_data = std::fs::read(key_path).map_err(TlsError::ReadKey)?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_data[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::ParseCert(e.to_string()))?;

    let key = rustls_pemfile::private_key(&mut &key_data[..])
        .map_err(|e| TlsError::ParseCert(e.to_string()))?
        .ok_or(TlsError::NoPrivateKey)?;

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
    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
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
    let mut formatted = String::new();
    for (i, c) in out.chars().enumerate() {
        if i > 0 && i % 76 == 0 {
            let _ = write!(formatted, "\n");
        }
        formatted.push(c);
    }
    formatted
}
