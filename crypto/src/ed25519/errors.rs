#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureError {
    InvalidArgument,
    InvalidPublicKey,
    InvalidSignature,
    InvalidAccountOwner,
}
