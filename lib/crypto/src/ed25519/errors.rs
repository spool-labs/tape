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
