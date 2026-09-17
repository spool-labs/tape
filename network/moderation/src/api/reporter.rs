//! Reporter identity as a salted hash, so records never hold caller addresses

use std::net::IpAddr;

use sha2::{Digest, Sha256};

/// Separates the salt from the address in the hashed input
const SALT_SEPARATOR: u8 = 0;

/// Hashes caller addresses with a secret salt
pub struct ReporterHasher {
    salt: String,
}

impl ReporterHasher {
    pub fn new(salt: String) -> Self {
        Self { salt }
    }

    /// Hex sha-256 of the salt and the caller address
    pub fn hash(&self, caller: IpAddr) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.salt.as_bytes());
        hasher.update([SALT_SEPARATOR]);
        hasher.update(caller.to_string().as_bytes());
        let digest = hasher.finalize();

        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            hex.push_str(&format!("{byte:02x}"));
        }
        hex
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // the same caller hashes the same under one salt, differently under another, never in the clear
    #[test]
    fn salted() {
        let caller: IpAddr = "198.51.100.9".parse().expect("address");
        let hasher = ReporterHasher::new("salt-one".to_string());

        let hash = hasher.hash(caller);

        assert_eq!(hash, hasher.hash(caller));
        assert_ne!(hash, ReporterHasher::new("salt-two".to_string()).hash(caller));
        assert_eq!(hash.len(), 64);
        assert!(!hash.contains("198.51"));
    }
}
