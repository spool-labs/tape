//! SPL Mint type abstraction.

use solana_program::{program_option::COption, pubkey::Pubkey};

/// SPL Token mint wrapper.
pub struct Mint(pub spl_token::state::Mint);

impl Mint {
    /// Returns the mint authority.
    pub fn mint_authority(&self) -> COption<Pubkey> {
        self.0.mint_authority
    }

    /// Returns the total supply.
    pub fn supply(&self) -> u64 {
        self.0.supply
    }

    /// Returns the number of decimals.
    pub fn decimals(&self) -> u8 {
        self.0.decimals
    }

    /// Returns whether the mint is initialized.
    pub fn is_initialized(&self) -> bool {
        self.0.is_initialized
    }

    /// Returns the freeze authority.
    pub fn freeze_authority(&self) -> COption<Pubkey> {
        self.0.freeze_authority
    }
}
