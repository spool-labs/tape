#![cfg(not(target_os = "solana"))]

//! ECDSA P-256 (secp256r1) keypairs for TLS identity.
//!
//! This module is host-only. On Solana the TLS public key is handled as raw
//! uncompressed SEC1 bytes (x || y, no 0x04 tag) via
//! [`tape_core::types::NetworkTlsPubkey`], so on-chain code does no P-256
//! arithmetic.
//!
//! PKCS#8 PEM is the on-disk and operator-facing format because it is also
//! what `certbot` / ACME tooling consume and emit. A single keypair file can
//! therefore back both the self-signed TLS cert the node generates at boot and
//! a CA-issued cert the operator plugs in later.
//!
//! # Usage
//!
//! ```no_run
//! use tape_crypto::p256::Keypair;
//!
//! let mut rng = rand::thread_rng();
//! let kp = Keypair::generate(&mut rng);
//!
//! // Persist (e.g. to ~/.tape/tls.key):
//! let pem = kp.to_pkcs8_pem().expect("encode");
//!
//! // Reload:
//! let kp2 = Keypair::from_pkcs8_pem(&pem).expect("decode");
//!
//! // 64-byte uncompressed pubkey to publish on-chain:
//! let tls_pubkey_bytes: [u8; 64] = kp2.public_key_bytes();
//! ```

pub mod error;
pub mod keypair;
pub mod validate;

pub use error::P256Error;
pub use keypair::Keypair;
pub use validate::validate_uncompressed_pubkey;

/// Length of an uncompressed SEC1 P-256 public key payload (x || y, no tag).
pub const P256_PUBKEY_LEN: usize = 64;
