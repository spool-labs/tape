use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rcgen::{CertificateParams, DnType, KeyPair, PKCS_ED25519};
use rustls_pki_types::PrivatePkcs8KeyDer;
use solana_sdk::signature::Keypair;

pub fn init_tls() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

pub fn temp_dir(prefix: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("clock drift")?
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{stamp}"));
    std::fs::create_dir_all(&dir).with_context(|| format!("create temp dir {}", dir.display()))?;
    Ok(dir)
}

pub fn pick_bind(off: u64) -> Result<std::net::SocketAddr> {
    if let Ok(lst) = std::net::TcpListener::bind("127.0.0.1:0") {
        let addr = lst.local_addr().context("read local addr")?;
        drop(lst);
        return Ok(addr);
    }

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("clock drift")?
        .as_nanos();
    let port = 20_000 + ((stamp + off as u128 * 9973) % 20_000) as u16;
    Ok(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
}

fn pkcs8_der(seed: [u8; 32]) -> Vec<u8> {
    let mut der = Vec::with_capacity(48);
    der.extend_from_slice(&[
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
        0x04, 0x20,
    ]);
    der.extend_from_slice(&seed);
    der
}

pub fn write_cert(kp: &Keypair, dir: &Path, cn: &str) -> Result<(PathBuf, PathBuf)> {
    let crt = dir.join("node.crt.pem");
    let key = dir.join("node.key.pem");

    let mut seed = [0u8; 32];
    seed.copy_from_slice(&kp.to_bytes()[..32]);
    let key_der = pkcs8_der(seed);
    let key_der = PrivatePkcs8KeyDer::from(key_der.as_slice());
    let key_pair = KeyPair::from_pkcs8_der_and_sign_algo(&key_der, &PKCS_ED25519)
        .map_err(|e| anyhow::anyhow!("build rcgen keypair: {e}"))?;

    let mut prm = CertificateParams::new(Vec::new())
        .map_err(|e| anyhow::anyhow!("certificate params: {e}"))?;
    prm.distinguished_name.push(DnType::CommonName, cn);
    let cert = prm
        .self_signed(&key_pair)
        .map_err(|e| anyhow::anyhow!("self-sign certificate: {e}"))?;

    std::fs::write(&crt, cert.pem()).with_context(|| format!("write cert {}", crt.display()))?;
    std::fs::write(&key, key_pair.serialize_pem())
        .with_context(|| format!("write key {}", key.display()))?;

    Ok((crt, key))
}
