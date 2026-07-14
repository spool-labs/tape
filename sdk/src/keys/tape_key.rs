//! Tape key type for controlling tape ownership.

use std::path::Path;

use tape_api::program::tapedrive::tape_pda;
use tape_crypto::address::Address;
use tape_crypto::ed25519::{Keypair, Pubkey};

use crate::keys::helpers::{load_ed25519_keypair, HelperError};

/// A key that controls a tape on the Tapedrive network.
///
/// Each tape has exactly one key, and each key controls exactly one tape.
/// The tape's on-chain address is derived from this key, you don't need
/// to store the address separately.
///
/// **Keep this key safe.** Anyone with it can write to, delete from, or
/// destroy the tape. You can share the *address* freely for reads.
///
/// # Example
/// ```rust,ignore
/// // Create and save a new tape key
/// let key = TapeKey::generate();
/// key.save("my-tape.json")?;
///
/// // Later, load it back
/// let key = TapeKey::load("my-tape.json")?;
/// println!("tape address: {}", key.address());
/// ```
pub struct TapeKey {
    keypair: Keypair,
}

impl TapeKey {
    /// Generate a new random tape key.
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            keypair: Keypair::new(&mut rng),
        }
    }

    /// Load from a Solana-compatible JSON keypair file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, HelperError> {
        let keypair = load_ed25519_keypair(path.as_ref())?;
        Ok(Self { keypair })
    }

    /// Save to a JSON keypair file. Creates parent directories if needed.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), std::io::Error> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = std::fs::File::create(path)?;
        serde_json::to_writer(file, &self.keypair.to_keypair_bytes().to_vec())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// The on-chain address of the tape this key controls.
    /// This is a PDA derived from the key, safe to share publicly.
    pub fn address(&self) -> Address {
        tape_pda(self.keypair.address()).0
    }

    /// The underlying public key (the authority). Rarely needed directly.
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Access the underlying Tapedrive keypair.
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate() {
        let key = TapeKey::generate();
        // address should be deterministically derived from the key
        assert_eq!(key.address(), tape_pda(key.pubkey().into()).0);
    }

    #[test]
    fn save_and_load() {
        let dir = std::env::temp_dir().join("tape_key_test");
        let path = dir.join("test.json");

        let original = TapeKey::generate();
        original.save(&path).unwrap();

        let loaded = TapeKey::load(&path).unwrap();
        assert_eq!(original.pubkey(), loaded.pubkey());
        assert_eq!(original.address(), loaded.address());

        std::fs::remove_dir_all(dir).ok();
    }
}
