//! A record naming a tape, with the small metadata its rack keeps for it

use tape_crypto::address::Address;

use crate::error::RackError;

/// Longest metadata key
pub const MAX_KEY_LEN: usize = 32;

/// One line of a rack track: a named tape and what the rack records about it
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub tape: Address,
    metadata: Vec<(String, Vec<u8>)>,
}

impl Record {
    pub fn new(tape: Address) -> Self {
        Self {
            tape,
            metadata: Vec::new(),
        }
    }

    /// The record with one more metadata field
    pub fn try_with(mut self, key: &str, value: &[u8]) -> Result<Self, RackError> {
        if !is_valid_key(key) {
            return Err(RackError::InvalidKey(key.to_string()));
        }
        if self.get(key).is_some() {
            return Err(RackError::DuplicateKey(key.to_string()));
        }
        self.metadata.push((key.to_string(), value.to_vec()));
        Ok(self)
    }

    /// A metadata value by key
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.metadata
            .iter()
            .find(|(field, _)| field == key)
            .map(|(_, value)| value.as_slice())
    }

    /// Metadata fields in the order they were recorded
    pub fn metadata(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.metadata
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_slice()))
    }
}

/// Whether a metadata key is 1 to 32 characters of `a-z 0-9 _ . -`
pub fn is_valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_KEY_LEN
        && key
            .bytes()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    // fields keep their order and are found by key
    #[test]
    fn metadata_fields() {
        let record = Record::new(Address::new([1u8; 32]))
            .try_with("name", b"photos/2024")
            .and_then(|record| record.try_with("part", b"3"))
            .expect("metadata");

        let fields: Vec<(&str, &[u8])> = record.metadata().collect();

        assert_eq!(fields, vec![("name", &b"photos/2024"[..]), ("part", &b"3"[..])]);
        assert_eq!(record.get("part"), Some(&b"3"[..]));
        assert_eq!(record.get("length"), None);
    }

    // keys outside the alphabet or used twice are refused
    #[test]
    fn invalid_keys() {
        let record = Record::new(Address::new([1u8; 32]));

        assert_eq!(record.clone().try_with("Name", b"x"), Err(RackError::InvalidKey("Name".to_string())));
        assert_eq!(record.clone().try_with("", b"x"), Err(RackError::InvalidKey(String::new())));
        assert!(record.clone().try_with(&"k".repeat(MAX_KEY_LEN + 1), b"x").is_err());
        assert_eq!(
            record.try_with("part", b"1").and_then(|record| record.try_with("part", b"2")),
            Err(RackError::DuplicateKey("part".to_string()))
        );
    }
}
