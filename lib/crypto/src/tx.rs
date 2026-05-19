#![allow(unexpected_cfgs)]

use core::fmt;

use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(not(target_os = "solana"))]
use solana_sdk::signature::Signature as SolanaSignature;
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

#[repr(transparent)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Txid([u8; 64]);

impl Default for Txid {
    fn default() -> Self {
        Self([0u8; Self::LEN])
    }
}

impl Txid {
    pub const LEN: usize = 64;

    pub const fn new(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 64] {
        self.0
    }

    pub const fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl AsRef<[u8]> for Txid {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 64]> for Txid {
    fn from(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
}

impl From<Txid> for [u8; 64] {
    fn from(value: Txid) -> Self {
        value.0
    }
}

#[cfg(not(target_os = "solana"))]
impl fmt::Display for Txid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&SolanaSignature::from(self.0), formatter)
    }
}

impl Serialize for Txid {
    fn serialize<SerializerType>(
        &self,
        serializer: SerializerType,
    ) -> Result<SerializerType::Ok, SerializerType::Error>
    where
        SerializerType: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(Self::LEN)?;
        for byte in self.0 {
            tuple.serialize_element(&byte)?;
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for Txid {
    fn deserialize<DeserializerType>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error>
    where
        DeserializerType: Deserializer<'de>,
    {
        struct TxidVisitor;

        impl<'de> Visitor<'de> for TxidVisitor {
            type Value = Txid;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 64-byte transaction identifier")
            }

            fn visit_bytes<ErrorType>(self, value: &[u8]) -> Result<Self::Value, ErrorType>
            where
                ErrorType: de::Error,
            {
                let bytes: [u8; Txid::LEN] = value
                    .try_into()
                    .map_err(|_| ErrorType::invalid_length(value.len(), &self))?;
                Ok(Txid::from(bytes))
            }

            fn visit_seq<Access>(self, mut seq: Access) -> Result<Self::Value, Access::Error>
            where
                Access: SeqAccess<'de>,
            {
                let mut bytes = [0u8; Txid::LEN];

                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(index, &self))?;
                }

                Ok(Txid::from(bytes))
            }
        }

        deserializer.deserialize_tuple(Txid::LEN, TxidVisitor)
    }
}

#[cfg(not(target_os = "solana"))]
impl From<SolanaSignature> for Txid {
    fn from(value: SolanaSignature) -> Self {
        Self(value.into())
    }
}

#[cfg(not(target_os = "solana"))]
impl From<Txid> for SolanaSignature {
    fn from(value: Txid) -> Self {
        value.0.into()
    }
}

#[cfg(all(test, not(target_os = "solana")))]
mod tests {
    use solana_sdk::signature::Keypair as SolanaKeypair;
    use solana_sdk::signer::Signer as SolanaSigner;

    use super::Txid;

    #[test]
    fn txid_roundtrips_with_solana_signature() {
        let keypair = SolanaKeypair::new();
        let signature = keypair.sign_message(b"hello txid");
        let txid = Txid::from(signature);
        let recovered_signature: solana_sdk::signature::Signature = txid.into();
        let recovered_txid = Txid::from(recovered_signature);

        assert_eq!(recovered_signature, signature);
        assert_eq!(recovered_txid, txid);
        assert_eq!(txid.to_bytes(), <[u8; Txid::LEN]>::from(txid));
        assert_eq!(txid.as_bytes(), &<[u8; Txid::LEN]>::from(txid));
    }
}
