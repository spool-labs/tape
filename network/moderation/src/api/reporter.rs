//! Reporter identity as a salted hash, so records never hold caller addresses

use std::net::IpAddr;

use tape_crypto::hash::hashv;

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
        let address = caller.to_string();
        let digest = hashv(&[self.salt.as_bytes(), &[SALT_SEPARATOR], address.as_bytes()]).to_bytes();

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

    // the digest is sha-256 of the salt, a zero byte and the address, so stored hashes still match
    #[test]
    fn pinned_digest() {
        let caller: IpAddr = "198.51.100.9".parse().expect("address");

        let hash = ReporterHasher::new("salt-one".to_string()).hash(caller);

        assert_eq!(hash, "af1006ac3715ae22666e515da8ddb15768c52226bec4b1e8b6a7448dde8bef17");
    }
}
