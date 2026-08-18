//! Bytes a read produced, without saying who owns them
//!
//! A backend that reads into a buffer of its own can hand the buffer over; one
//! that reads into a block several values share can hand out a share of it. Both
//! reach a caller as this, and neither has to copy on the way.

use std::ops::Deref;
use std::sync::Arc;

/// Bytes a read produced
#[derive(Clone, Debug)]
pub struct Value {
    held: Held,
}

/// Which arrangement a value's bytes are under
#[derive(Clone, Debug)]
enum Held {
    /// A buffer this value has to itself
    Owned(Vec<u8>),

    /// Bytes shared with whatever else is holding the same read
    Shared(Arc<[u8]>),
}

impl Value {
    /// Bytes whose buffer belongs to whoever holds them
    pub fn new(bytes: Vec<u8>) -> Value {
        Value {
            held: Held::Owned(bytes),
        }
    }

    /// Bytes a backend is keeping alive for more than this reader
    pub fn shared(bytes: Arc<[u8]>) -> Value {
        Value {
            held: Held::Shared(bytes),
        }
    }

    /// The bytes as a slice, which never copies
    pub fn as_slice(&self) -> &[u8] {
        match &self.held {
            Held::Owned(bytes) => bytes,
            Held::Shared(bytes) => bytes,
        }
    }

    /// The bytes as a vector, moving the buffer where this value owns it
    ///
    /// A shared value copies, since the bytes outlive it. A caller that only
    /// reads should take the slice instead.
    pub fn into_vec(self) -> Vec<u8> {
        match self.held {
            Held::Owned(bytes) => bytes,
            Held::Shared(bytes) => bytes.to_vec(),
        }
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Value {
        Value::new(bytes)
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Vec<u8> {
        value.into_vec()
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Value {}

impl PartialEq<[u8]> for Value {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<Vec<u8>> for Value {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // an owned value hands its buffer over rather than copying it
    #[test]
    fn owned_moves() {
        let bytes = vec![7u8; 64];
        let address = bytes.as_ptr();
        let value = Value::new(bytes);

        assert_eq!(value.len(), 64);
        assert_eq!(value.into_vec().as_ptr(), address, "the buffer was copied");
    }

    // a shared value reads without copying and copies only when asked to own
    #[test]
    fn shared_reads() {
        let value = Value::shared(Arc::from(&[3u8; 32][..]));

        assert_eq!(value.as_slice(), &[3u8; 32]);
        assert_eq!(value.into_vec(), vec![3u8; 32]);
    }
}
