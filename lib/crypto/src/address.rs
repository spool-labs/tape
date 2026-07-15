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
}
