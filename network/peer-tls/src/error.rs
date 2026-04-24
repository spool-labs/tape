use thiserror::Error;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("cert generation: {0}")]
    CertGeneration(String),

    #[error("invalid keypair: {0}")]
    InvalidKeypair(String),

    #[error("build client: {0}")]
    BuildClient(#[source] reqwest::Error),

    #[error("build server config: {0}")]
    BuildServer(String),

    #[error("pem cert: {0}")]
    PemCert(String),

    #[error("crypto provider already installed with different instance")]
    ProviderInstalled,
}
