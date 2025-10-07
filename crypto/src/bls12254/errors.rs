#[derive(Debug, PartialEq, Eq)]
pub enum BLSError {
    SecretKeyError,
    AltBN128AddError,
    AltBN128MulError,
    AltBN128PairingError,
    HashToCurveError,
    BLSSigningError,
    BLSVerificationError,
    SerializationError,
    G1PointCompressionError,
    G1PointDecompressionError,
    G2PointCompressionError,
    G2PointDecompressionError,
}
