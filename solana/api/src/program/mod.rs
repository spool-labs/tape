pub mod exchange;
pub mod staking;
pub mod token;
pub mod tapedrive;

// Note: Each program exports `id()`, `ID`, and `PROGRAM_ID` via declare_id!
// Use specific paths like `tapedrive::id()` or `staking::id()` to disambiguate.
#[allow(ambiguous_glob_reexports)]
pub use exchange::*;
#[allow(ambiguous_glob_reexports)]
pub use staking::*;
#[allow(ambiguous_glob_reexports)]
pub use token::*;
#[allow(ambiguous_glob_reexports)]
pub use tapedrive::*;

#[allow(ambiguous_glob_reexports)]
pub mod prelude {
    pub use tape_core::bft::*;
    pub use tape_core::bls::*;
    pub use tape_core::cert::*;
    pub use tape_core::erasure::*;
    pub use tape_core::prelude::*;
    pub use tape_core::spooler::*;
    pub use tape_core::staking::*;
    pub use tape_core::system::*;
    pub use tape_core::tape::*;
    pub use tape_core::types::*;
    #[allow(unused_imports)]
    pub use tape_core::types::coin::*;
    pub use tape_solana::*;

    #[cfg(not(target_os = "solana"))]
    pub use tape_crypto::prelude::{Keypair, SecretKey, Signature};
    pub use tape_crypto::prelude::{Address, BLSError, Hash, SignatureError};

    pub use crate::compute::*;
    pub use crate::consts::*;
    pub use crate::errors::{ExchangeError, TapeError};
    #[cfg(not(target_os = "solana"))]
    pub use crate::errors::RequiredAction;
    pub use crate::event::*;
    pub use crate::instruction::*;
    pub use crate::dynamic::DynamicState;
    pub use crate::loaders::{AccountInfoHelper, AccountInfoLoader, FromAccountSlice};
    pub use crate::program::exchange::*;
    pub use crate::program::staking::*;
    pub use crate::program::tapedrive::*;
    pub use crate::program::token::*;
    pub use crate::program::{exchange, staking, tapedrive, token};
    pub use crate::state::*;
    pub use crate::utils::*;
}
