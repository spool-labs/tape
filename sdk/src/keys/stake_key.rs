//! Stake key type for controlling delegated stake accounts.

use std::path::Path;

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::program::tapedrive::stake_pda;

use crate::keys::helpers::{load_solana_keypair, HelperError};

/// A key that controls a delegated stake account on the Tapedrive network.
///
/// Each stake key maps to exactly one on-chain stake account via a PDA.
/// The key's holder can delegate, request unlock, and unstake from pools.
pub struct StakeKey {
    keypair: Keypair,
}

impl StakeKey {
    /// Generate a new random stake key.
    pub fn generate() -> Self {
        Self {
            keypair: Keypair::new(),
        }
    }

    /// Load from a Solana-compatible JSON keypair file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, HelperError> {
        let keypair = load_solana_keypair(path.as_ref())?;
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
        serde_json::to_writer(file, &self.keypair.to_bytes().to_vec())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// The on-chain address of the stake account this key controls.
    pub fn address(&self) -> Pubkey {
        stake_pda(self.keypair.pubkey()).0
    }

    /// The underlying public key (the authority).
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Access the underlying keypair for signing transactions.
    pub fn as_keypair(&self) -> &Keypair {
        &self.keypair
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate() {
        let key = StakeKey::generate();
        assert_eq!(key.address(), stake_pda(key.pubkey()).0);
    }

    #[test]
    fn save_and_load() {
        let dir = std::env::temp_dir().join("stake_key_test");
        let path = dir.join("test.json");

        let original = StakeKey::generate();
        original.save(&path).unwrap();

        let loaded = StakeKey::load(&path).unwrap();
        assert_eq!(original.pubkey(), loaded.pubkey());
        assert_eq!(original.address(), loaded.address());

        std::fs::remove_dir_all(dir).ok();
    }
}
