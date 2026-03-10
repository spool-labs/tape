//! Lamport transfer helpers.

use solana_program::{account_info::AccountInfo, program_error::ProgramError};

/// Trait for transferring lamports between accounts.
pub trait LamportTransfer<'a, 'info> {
    /// Send lamports to another account (direct transfer, no CPI).
    fn send(&'a self, lamports: u64, to: &'a AccountInfo<'info>);
    /// Collect lamports from another account (via system program CPI).
    fn collect(&'a self, lamports: u64, from: &'a AccountInfo<'info>) -> Result<(), ProgramError>;
}

impl<'a, 'info> LamportTransfer<'a, 'info> for AccountInfo<'info> {
    #[inline(always)]
    fn send(&'a self, lamports: u64, to: &'a AccountInfo<'info>) {
        **self.lamports.borrow_mut() -= lamports;
        **to.lamports.borrow_mut() += lamports;
    }

    #[inline(always)]
    fn collect(&'a self, lamports: u64, from: &'a AccountInfo<'info>) -> Result<(), ProgramError> {
        solana_program::program::invoke(
            &solana_program::system_instruction::transfer(from.key, self.key, lamports),
            &[from.clone(), self.clone()],
        )
    }
}
