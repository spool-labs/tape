use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SignatureError {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("invalid public key")]
    InvalidPublicKey,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("invalid account owner")]
    InvalidAccountOwner,
    #[error("verification failed")]
    VerificationFailed,
}

#[cfg(not(target_os = "solana"))]
#[derive(Debug, Error)]
pub enum KeypairFileError {
    #[error("failed to read keypair file {path}: {message}")]
    FileRead { path: String, message: String },

    #[error("failed to parse keypair JSON from {path}: {message}")]
    JsonParse { path: String, message: String },

    #[error("keypair JSON must contain {expected} bytes (got {actual})")]
    InvalidLength { expected: usize, actual: usize },

    #[error("invalid keypair data: {0}")]
    InvalidKeypair(String),
}
