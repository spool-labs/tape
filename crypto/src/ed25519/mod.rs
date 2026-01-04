pub mod consts;
pub mod errors;
pub mod sig;
pub mod types;
pub mod utils;

pub use errors::SignatureError;
pub use sig::sig_verify;
pub use types::{PublicKey, Signature};

// SecretKey and Keypair are only available off-chain (they require rand)
#[cfg(not(target_os = "solana"))]
pub use types::{Keypair, SecretKey};

