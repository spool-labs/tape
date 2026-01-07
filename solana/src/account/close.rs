//! Account closing functionality.

use solana_program::{account_info::AccountInfo, program_error::ProgramError};

/// Trait for closing accounts.
pub trait CloseAccount<'info> {
    /// Close the account and return lamports to recipient.
    fn close(&self, to: &AccountInfo<'info>) -> Result<(), ProgramError>;
}

impl<'info> CloseAccount<'info> for AccountInfo<'info> {
    fn close(&self, to: &AccountInfo<'info>) -> Result<(), ProgramError> {
        // Return rent lamports.
        **to.lamports.borrow_mut() += self.lamports();
        **self.lamports.borrow_mut() = 0;

        // Assign system program as the owner
        self.assign(&solana_program::system_program::ID);

        // Resize data to zero.
        self.resize(0)?;

        Ok(())
    }
}
