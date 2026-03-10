//! SPL Token validation traits.

use solana_program::{account_info::AccountInfo, program_error::ProgramError, pubkey::Pubkey};
use solana_program::program_pack::Pack;

use crate::{trace, AccountInfoValidation, AccountValidation};

use super::{Mint, TokenAccount};

/// Trait for parsing SPL token accounts.
pub trait AsSpl {
    /// Parse account as an SPL mint.
    fn as_mint(&self) -> Result<Mint, ProgramError>;
    /// Parse account as an SPL token account.
    fn as_token_account(&self) -> Result<TokenAccount, ProgramError>;
    /// Parse and validate as associated token account.
    fn as_associated_token_account(
        &self,
        owner: &Pubkey,
        mint: &Pubkey,
    ) -> Result<TokenAccount, ProgramError>;
}

impl AsSpl for AccountInfo<'_> {
    #[track_caller]
    fn as_mint(&self) -> Result<Mint, ProgramError> {
        if *self.owner != spl_token::ID {
            return Err(ProgramError::InvalidAccountOwner);
        }

        // Validate account data length.
        let data = self.try_borrow_data()?;
        if data.len() != spl_token::state::Mint::LEN {
            return Err(trace(
                "Mint data length is invalid",
                ProgramError::InvalidAccountData,
            ));
        }

        // Deserialize account data.
        unsafe {
            let mint = spl_token::state::Mint::unpack(std::slice::from_raw_parts(
                data.as_ptr(),
                spl_token::state::Mint::LEN,
            ))?;
            Ok(Mint(mint))
        }
    }

    #[track_caller]
    fn as_token_account(&self) -> Result<TokenAccount, ProgramError> {
        if *self.owner != spl_token::ID {
            return Err(ProgramError::InvalidAccountOwner);
        }

        // Validate account data length.
        let data = self.try_borrow_data()?;
        if data.len() != spl_token::state::Account::LEN {
            return Err(trace(
                "Token account data length is invalid",
                ProgramError::InvalidAccountData,
            ));
        }

        // Deserialize account data.
        unsafe {
            let account = spl_token::state::Account::unpack(std::slice::from_raw_parts(
                data.as_ptr(),
                spl_token::state::Account::LEN,
            ))?;
            Ok(TokenAccount(account))
        }
    }

    #[track_caller]
    fn as_associated_token_account(
        &self,
        owner: &Pubkey,
        mint: &Pubkey,
    ) -> Result<TokenAccount, ProgramError> {
        self.has_address(
            &spl_associated_token_account::get_associated_token_address(owner, mint),
        )?
        .as_token_account()
    }
}

impl AccountValidation for Mint {
    #[track_caller]
    fn assert<F>(&self, condition: F) -> Result<&Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        if !condition(self) {
            return Err(trace(
                "Mint data is invalid",
                ProgramError::InvalidAccountData,
            ));
        }
        Ok(self)
    }

    #[track_caller]
    fn assert_err<F>(&self, condition: F, err: ProgramError) -> Result<&Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        if !condition(self) {
            return Err(trace("Mint data is invalid", err));
        }
        Ok(self)
    }

    #[track_caller]
    fn assert_msg<F>(&self, condition: F, msg: &str) -> Result<&Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        if !condition(self) {
            return Err(trace(
                format!("Mint data is invalid: {}", msg).as_str(),
                ProgramError::InvalidAccountData,
            ));
        }
        Ok(self)
    }

    fn assert_mut<F>(&mut self, _condition: F) -> Result<&mut Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        panic!("not implemented")
    }

    fn assert_mut_err<F>(&mut self, _condition: F, _err: ProgramError) -> Result<&mut Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        panic!("not implemented")
    }

    fn assert_mut_msg<F>(&mut self, _condition: F, _msg: &str) -> Result<&mut Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        panic!("not implemented")
    }
}

impl AccountValidation for TokenAccount {
    #[track_caller]
    fn assert<F>(&self, condition: F) -> Result<&Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        if !condition(self) {
            return Err(trace(
                "Token account data is invalid",
                ProgramError::InvalidAccountData,
            ));
        }
        Ok(self)
    }

    #[track_caller]
    fn assert_err<F>(&self, condition: F, err: ProgramError) -> Result<&Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        if !condition(self) {
            return Err(trace("Token account data is invalid", err));
        }
        Ok(self)
    }

    #[track_caller]
    fn assert_msg<F>(&self, condition: F, msg: &str) -> Result<&Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        if !condition(self) {
            return Err(trace(
                format!("Token account data is invalid: {}", msg).as_str(),
                ProgramError::InvalidAccountData,
            ));
        }
        Ok(self)
    }

    fn assert_mut<F>(&mut self, _condition: F) -> Result<&mut Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        panic!("not implemented")
    }

    fn assert_mut_err<F>(&mut self, _condition: F, _err: ProgramError) -> Result<&mut Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        panic!("not implemented")
    }

    fn assert_mut_msg<F>(&mut self, _condition: F, _msg: &str) -> Result<&mut Self, ProgramError>
    where
        F: Fn(&Self) -> bool,
    {
        panic!("not implemented")
    }
}
