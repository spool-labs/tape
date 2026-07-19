//! Erasure coder trait.
//!
//! Defines the low-level interface for erasure code implementations.

use crate::errors::{DecodeError, EncodeError};

/// Low-level trait for erasure code encoding/decoding.
///
/// Implementations include:
/// - `ClayCoder`: Raw Clay MSR codes (k data, m parity)
/// - `ReedSolomonCoder`: Raw Reed-Solomon codes
///
/// For producing network-ready slices with metadata, use `Slicer<C: ErasureCoder>`.
pub trait ErasureCoder {
    /// Data chunks (k) needed for reconstruction.
    fn k(&self) -> usize;

    /// Parity chunks (m).
    fn m(&self) -> usize;

    /// Total chunks (n = k + m).
    fn n(&self) -> usize {
        self.k() + self.m()
    }

    /// Encode data into n chunks.
    ///
    /// # Arguments
    /// * `data` - Raw bytes to encode
    ///
    /// # Returns
    /// Vector of n chunks (each chunk is a Vec<u8>).
    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError>;

    /// Decode from available chunks.
    ///
    /// # Arguments
    /// * `chunks` - Sparse array of (chunk_index, chunk_data) pairs.
    ///              Must have at least k chunks for reconstruction.
    ///
    /// # Returns
    /// Reconstructed original data.
    fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError>;
}
