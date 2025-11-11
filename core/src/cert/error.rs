
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateError {
    /// Provided epoch does not match this certificate's epoch (committee rotates per-epoch).
    EpochMismatch,

    /// The provided committee index is out of range for the bitmap.
    BadIndex,

    /// This committee index is already marked as signed.
    AlreadySigned,

    /// Signature verification failed.
    SignatureInvalid,
}
