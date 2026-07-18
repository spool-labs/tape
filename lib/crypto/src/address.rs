#![allow(unexpected_cfgs)]

use core::fmt;
use core::str::FromStr;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(target_os = "solana")]
use solana_program::pubkey::{
    ParsePubkeyError,
    Pubkey as SolanaPubkey,
    PubkeyError as SolanaPubkeyError,
};
#[cfg(not(target_os = "solana"))]
use solana_pubkey::{
    ParsePubkeyError,
    Pubkey as SolanaPubkey,
    PubkeyError as SolanaPubkeyError,
};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::hash::Hash;

const SUBDOMAIN_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const SUBDOMAIN_LABEL_LEN: usize = 52;

#[repr(transparent)]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Address([u8; 32]);

impl Address {
    pub const LEN: usize = 32;

    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        SolanaPubkey::new_unique().into()
    }

    pub fn find_program_address<P>(seeds: &[&[u8]], program_id: P) -> (Self, u8)
    where
        P: Into<SolanaPubkey>,
    {
        let program_id = program_id.into();
        let (address, bump) = SolanaPubkey::find_program_address(seeds, &program_id);
        (address.into(), bump)
    }

    pub fn create_program_address<P>(
        seeds: &[&[u8]],
        program_id: P,
    ) -> Result<Self, SolanaPubkeyError>
    where
        P: Into<SolanaPubkey>,
    {
        let program_id = program_id.into();
        SolanaPubkey::create_program_address(seeds, &program_id).map(Into::into)
    }

    /// Encode as a lowercase base32 DNS label, for one-subdomain-per-site
    /// hosting where hostnames are case-insensitive
    pub fn to_subdomain_label(self) -> String {
        let mut label = String::with_capacity(SUBDOMAIN_LABEL_LEN);
        let mut accumulator: u32 = 0;
        let mut bits: u32 = 0;

        for byte in self.0 {
            accumulator = (accumulator << 8) | u32::from(byte);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                let index = (accumulator >> bits) & 0x1f;
                label.push(SUBDOMAIN_ALPHABET[index as usize] as char);
            }
        }
        if bits > 0 {
            let index = (accumulator << (5 - bits)) & 0x1f;
            label.push(SUBDOMAIN_ALPHABET[index as usize] as char);
        }

        label
    }

    /// Decode a DNS label produced by the subdomain encoding, in any case
    pub fn try_from_subdomain_label(label: &str) -> Option<Self> {
        if label.len() != SUBDOMAIN_LABEL_LEN {
            return None;
        }

        let mut bytes = [0u8; Self::LEN];
        let mut filled = 0usize;
        let mut accumulator: u32 = 0;
        let mut bits: u32 = 0;

        for character in label.bytes() {
            let lower = character.to_ascii_lowercase();
            let value = SUBDOMAIN_ALPHABET
                .iter()
                .position(|letter| *letter == lower)? as u32;
            accumulator = (accumulator << 5) | value;
            bits += 5;
            if bits >= 8 {
                bits -= 8;
                bytes[filled] = ((accumulator >> bits) & 0xff) as u8;
                filled += 1;
            }
        }

        // The label's trailing pad bits must be zero for a canonical encoding.
        if filled != Self::LEN || accumulator & ((1 << bits) - 1) != 0 {
            return None;
        }
        Some(Self(bytes))
    }
}

impl AsRef<[u8]> for Address {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pubkey: SolanaPubkey = (*self).into();
        pubkey.fmt(f)
    }
}

impl FromStr for Address {
    type Err = ParsePubkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SolanaPubkey::from_str(s).map(Into::into)
    }
}

impl From<[u8; 32]> for Address {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<Address> for [u8; 32] {
    fn from(value: Address) -> Self {
        value.0
    }
}

impl From<SolanaPubkey> for Address {
    fn from(value: SolanaPubkey) -> Self {
        Self(value.to_bytes())
    }
}

impl From<Address> for SolanaPubkey {
    fn from(value: Address) -> Self {
        SolanaPubkey::new_from_array(value.0)
    }
}

impl From<&Address> for SolanaPubkey {
    fn from(value: &Address) -> Self {
        (*value).into()
    }
}

impl From<Address> for Hash {
    fn from(value: Address) -> Self {
        value.to_bytes().into()
    }
}

impl From<&Address> for Hash {
    fn from(value: &Address) -> Self {
        value.to_bytes().into()
    }
}

impl From<Hash> for Address {
    fn from(value: Hash) -> Self {
        value.to_bytes().into()
    }
}

impl From<&Hash> for Address {
    fn from(value: &Hash) -> Self {
        value.to_bytes().into()
    }
}

#[cfg(all(test, not(target_os = "solana")))]
mod tests {
    use solana_pubkey::Pubkey as SolanaPubkey;

    use crate::ed25519::{Keypair, Pubkey};

    use super::*;

    #[test]
    fn address_roundtrip_with_bytes() {
        let bytes = [7u8; Address::LEN];
        let address = Address::from(bytes);

        assert_eq!(address.to_bytes(), bytes);
        assert_eq!(<[u8; Address::LEN]>::from(address), bytes);
        assert_eq!(address.as_bytes(), &bytes);
    }

    #[test]
    fn address_roundtrip_with_solana_pubkey() {
        let pubkey = SolanaPubkey::new_unique();
        let address = Address::from(pubkey);
        let recovered: SolanaPubkey = address.into();

        assert_eq!(recovered, pubkey);
    }

    #[test]
    fn address_roundtrip_with_hash() {
        let address = Address::new_unique();
        let hash = Hash::from(address);
        let recovered = Address::from(hash);

        assert_eq!(recovered, address);
    }

    #[test]
    fn pubkey_converts_to_address() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let pubkey = keypair.pubkey();
        let address: Address = pubkey.into();

        assert_eq!(address.to_bytes(), pubkey.to_bytes());
    }

    #[test]
    fn address_converts_to_pubkey_for_valid_ed25519_bytes() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let address = keypair.address();
        let recovered = Pubkey::try_from(address).expect("valid ed25519 bytes should convert");

        assert_eq!(recovered, keypair.pubkey());
    }

    #[test]
    fn address_conversion_to_pubkey_fails_for_invalid_ed25519_bytes() {
        let program_id = Address::from([9u8; Address::LEN]);
        let (address, _) = Address::find_program_address(&[b"invalid-ed25519"], &program_id);

        assert!(Pubkey::try_from(address).is_err());
    }

    // subdomain labels round-trip and decode case-insensitively
    #[test]
    fn subdomain_label_roundtrip() {
        let address = Address::new_unique();
        let label = address.to_subdomain_label();

        assert_eq!(label.len(), 52);
        assert!(label.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        assert_eq!(Address::try_from_subdomain_label(&label), Some(address));
        assert_eq!(
            Address::try_from_subdomain_label(&label.to_ascii_uppercase()),
            Some(address)
        );
    }

    // malformed labels are rejected
    #[test]
    fn subdomain_label_rejects_junk() {
        assert_eq!(Address::try_from_subdomain_label("short"), None);
        assert_eq!(Address::try_from_subdomain_label(&"1".repeat(52)), None);
        assert_eq!(Address::try_from_subdomain_label(&"a".repeat(53)), None);
    }
}
