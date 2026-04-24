use bytemuck::{Pod, Zeroable};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

/// Length of an uncompressed SEC1 P-256 public key (x || y, no 0x04 prefix).
pub const NETWORK_TLS_PUBKEY_LEN: usize = 64;

/// A P-256 (secp256r1) TLS identity key published on-chain in
/// `Node.metadata.network_tls`.
///
/// Stored as the raw uncompressed SEC1 public key: the 32-byte x coordinate
/// followed by the 32-byte y coordinate, with no leading `0x04` tag byte.
/// Peer and SDK clients pin the TLS handshake's leaf-cert SubjectPublicKeyInfo
/// against this value; operators can serve either a self-signed cert derived
/// from the same keypair or a CA-issued cert issued to the same keypair, and
/// both satisfy the pin.
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

    /// A fresh, monotonically-unique value for tests. Not a valid P-256 point;
    /// just 64 bytes guaranteed distinct from every other `new_unique()`.
    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        let a = tape_crypto::address::Address::new_unique();
        let b = tape_crypto::address::Address::new_unique();
        let mut data = [0u8; NETWORK_TLS_PUBKEY_LEN];
        data[..32].copy_from_slice(a.as_bytes());
        data[32..].copy_from_slice(b.as_bytes());
        Self { data }
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
