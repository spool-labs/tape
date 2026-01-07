//! # tape-solana
//!
//! Solana smart contract framework for Tapedrive.
//!
//! This crate provides account validation, serialization, CPI helpers,
//! and SPL token integration for Solana programs.

mod account;
mod log;
pub mod macros;
#[cfg(feature = "spl")]
mod spl;
mod utils;

pub use account::*;
pub use log::*;
#[cfg(feature = "spl")]
pub use spl::*;
pub use utils::*;

// Re-export common dependencies for convenience
pub use bytemuck::{Pod, Zeroable};
pub use num_enum::{IntoPrimitive, TryFromPrimitive};
#[allow(deprecated)]
pub use solana_program::system_program;
pub use solana_program::{
    account_info::AccountInfo,
    clock::Clock,
    entrypoint,
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    program_error::ProgramError,
    pubkey::Pubkey,
    sysvar,
    sysvar::Sysvar,
};
pub use thiserror::Error;

/// Prelude module for convenient imports
pub mod prelude {
    pub use super::account::*;
    pub use super::log::*;
    #[cfg(feature = "spl")]
    pub use super::spl::*;
    pub use super::utils::*;

    pub use bytemuck::{Pod, Zeroable};
    pub use num_enum::{IntoPrimitive, TryFromPrimitive};
    #[allow(deprecated)]
    pub use solana_program::system_program;
    pub use solana_program::{
        account_info::AccountInfo,
        clock::Clock,
        entrypoint,
        entrypoint::ProgramResult,
        instruction::{AccountMeta, Instruction},
        program_error::ProgramError,
        pubkey::Pubkey,
        sysvar,
        sysvar::Sysvar,
    };
    pub use thiserror::Error;
}
