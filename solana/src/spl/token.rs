//! SPL Token Account type abstraction.

use solana_program::{program_option::COption, pubkey::Pubkey};

/// SPL Token account wrapper.
pub struct TokenAccount(pub spl_token::state::Account);

impl TokenAccount {
    /// Returns the mint address.
    pub fn mint(&self) -> Pubkey {
        self.0.mint
    }

    /// Returns the owner address.
    pub fn owner(&self) -> Pubkey {
        self.0.owner
    }

    /// Returns the token balance.
    pub fn amount(&self) -> u64 {
        self.0.amount
    }

    /// Returns the delegate address if set.
    pub fn delegate(&self) -> COption<Pubkey> {
        self.0.delegate
    }

    /// Returns whether the account is frozen.
    pub fn is_frozen(&self) -> bool {
        self.0.is_frozen()
    }

    /// Returns whether the account is a native SOL account.
    pub fn is_native(&self) -> COption<u64> {
        self.0.is_native
    }

    /// Returns the delegated amount.
    pub fn delegated_amount(&self) -> u64 {
        self.0.delegated_amount
    }

    /// Returns the close authority if set.
    pub fn close_authority(&self) -> COption<Pubkey> {
        self.0.close_authority
    }
}
