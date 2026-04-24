use bytemuck::{Pod, Zeroable};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

/// The length of a node's TLS public key
pub const NETWORK_TLS_PUBKEY_LEN: usize = 32;

/// A node's TLS public key, stored on-chain as raw Ed25519 public key bytes.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct NetworkTlsPubkey {
    data: [u8; NETWORK_TLS_PUBKEY_LEN],
}

unsafe impl Pod for NetworkTlsPubkey {}
unsafe impl Zeroable for NetworkTlsPubkey {}

impl Default for NetworkTlsPubkey {
    #[inline]
    fn default() -> Self {
        Self { data: [0; NETWORK_TLS_PUBKEY_LEN] }
    }
}

impl NetworkTlsPubkey {
    #[inline]
    pub fn new(bytes: [u8; NETWORK_TLS_PUBKEY_LEN]) -> Self {
        Self { data: bytes }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; NETWORK_TLS_PUBKEY_LEN] {
        &self.data
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; NETWORK_TLS_PUBKEY_LEN] {
        self.data
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        self.data == [0; NETWORK_TLS_PUBKEY_LEN]
    }

    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        Self {
            data: tape_crypto::address::Address::new_unique().into(),
        }
    }
}

impl From<[u8; NETWORK_TLS_PUBKEY_LEN]> for NetworkTlsPubkey {
    #[inline]
    fn from(bytes: [u8; NETWORK_TLS_PUBKEY_LEN]) -> Self {
        Self::new(bytes)
    }
}

impl AsRef<[u8]> for NetworkTlsPubkey {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(not(target_os = "solana"))]
impl core::fmt::Display for NetworkTlsPubkey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for b in &self.data {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}
