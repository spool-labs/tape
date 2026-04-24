use thiserror::Error;

/// Errors from P-256 key handling.
#[derive(Debug, Error)]
pub enum P256Error {
    /// PKCS#8 encode/decode failure (malformed PEM/DER, wrong algorithm, etc.).
    #[error("pkcs8: {0}")]
    Pkcs8(String),

    /// Bytes do not form a valid P-256 public key (off-curve or identity).
    #[error("invalid P-256 public key")]
    InvalidPublicKey,
}
