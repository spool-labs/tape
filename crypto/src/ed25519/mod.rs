pub mod consts;
pub mod errors;
pub mod sig;
pub mod types;
pub mod utils;

pub use errors::SignatureError;
pub use sig::sig_verify;

// All wrapper types are only available off-chain (they use ed25519-consensus
// which brings in curve25519-dalek-ng with stack size issues on SBF).
// For on-chain signature verification, use `sig_verify` with raw bytes.
#[cfg(not(target_os = "solana"))]
pub use types::{Keypair, PublicKey, SecretKey, Signature};

