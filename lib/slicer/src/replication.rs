//! Replication coder: n full copies of every stripe.

use tape_core::encoding::RepParams;

use crate::{DecodeError, EncodeError, ErasureCoder};

/// Replication coder holding n identical copies of every stripe.
pub struct ReplicationCoder {
    n: usize,
}

impl ReplicationCoder {
    /// Create a coder producing n copies.
    pub fn new(n: usize) -> Self {
        Self { n }
    }

    /// Create from RepParams.
    pub fn from_params(params: RepParams) -> Self {
        Self::new(params.n() as usize)
    }
}

impl ErasureCoder for ReplicationCoder {
    #[inline]
    fn k(&self) -> usize {
        1
    }

    #[inline]
    fn m(&self) -> usize {
        self.n.saturating_sub(1)
    }

    // A copy needs no padding, so a stripe keeps the length it has.
    #[inline]
    fn stripe_alignment(&self) -> usize {
        1
    }

    fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        if data.is_empty() {
            return Err(EncodeError::EmptyInput);
        }
        Ok(vec![data.to_vec(); self.n])
    }

    fn decode(&self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        // Any single copy is the whole stripe, so the first one present answers.
        let (_, data) = chunks.first().ok_or(DecodeError::NotEnoughSlices)?;
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    // every copy is the whole stripe
    #[test]
    fn copies() {
        let coder = ReplicationCoder::new(3);
        let data = payload(4096);

        let copies = coder.encode(&data).expect("encode");

        assert_eq!(copies.len(), 3);
        assert!(copies.iter().all(|copy| copy.as_slice() == data.as_slice()));
    }

    // any one copy reconstructs the stripe
    #[test]
    fn one_copy() {
        let coder = ReplicationCoder::new(2);
        let data = payload(1024);
        let copies = coder.encode(&data).expect("encode");

        let recovered = coder.decode(&[(1, copies[1].as_slice())]).expect("decode");

        assert_eq!(recovered, data);
    }

    // a single drive is a code with no parity
    #[test]
    fn single_drive() {
        let coder = ReplicationCoder::new(1);
        let data = payload(512);

        let copies = coder.encode(&data).expect("encode");

        assert_eq!((coder.n(), coder.k(), coder.m()), (1, 1, 0));
        assert_eq!(copies.len(), 1);
        assert_eq!(coder.decode(&[(0, copies[0].as_slice())]).expect("decode"), data);
    }

    // with no copy present there is nothing to decode from
    #[test]
    fn no_copies() {
        let coder = ReplicationCoder::new(2);

        assert!(matches!(coder.decode(&[]), Err(DecodeError::NotEnoughSlices)));
    }
}
