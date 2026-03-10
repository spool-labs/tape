use tape_solana::*;

use crate::program::*;
use crate::state::{System, Epoch, Archive, Treasury, SnapshotState};

pub trait AccountInfoLoader {
    fn is_system(&self) -> Result<&Self, ProgramError>;
    fn is_epoch(&self) -> Result<&Self, ProgramError>;
    fn is_archive(&self) -> Result<&Self, ProgramError>;
    fn is_archive_ata(&self) -> Result<&Self, ProgramError>;
    fn is_mint(&self) -> Result<&Self, ProgramError>;
    fn is_metadata(&self) -> Result<&Self, ProgramError>;
    fn is_treasury(&self) -> Result<&Self, ProgramError>;
    fn is_snapshot_state(&self) -> Result<&Self, ProgramError>;
}

impl AccountInfoLoader for AccountInfo<'_> {
    fn is_system(&self) -> Result<&Self, ProgramError> {
        self.has_address(&SYSTEM_ADDRESS)?
            .is_type::<System>(&tapedrive::ID)
    }

    fn is_epoch(&self) -> Result<&Self, ProgramError> {
        self.has_address(&EPOCH_ADDRESS)?
            .is_type::<Epoch>(&tapedrive::ID)
    }

    fn is_archive(&self) -> Result<&Self, ProgramError> {
        self.has_address(&ARCHIVE_ADDRESS)?
            .is_type::<Archive>(&tapedrive::ID)
    }

    fn is_archive_ata(&self) -> Result<&Self, ProgramError> {
        self.has_address(&ARCHIVE_ATA)?
            .has_owner(&spl_token::ID)
    }

    fn is_mint(&self) -> Result<&Self, ProgramError> {
        self.has_address(&MINT_ADDRESS)?
            .has_owner(&spl_token::ID)
    }

    fn is_metadata(&self) -> Result<&Self, ProgramError> {
        self.has_address(&METADATA_ADDRESS)?
            .has_owner(&mpl_token_metadata::ID)
    }

    fn is_treasury(&self) -> Result<&Self, ProgramError> {
        self.has_address(&TREASURY_ADDRESS)?
            .is_type::<Treasury>(&tapedrive::ID)
    }

    fn is_snapshot_state(&self) -> Result<&Self, ProgramError> {
        self.has_address(&SNAPSHOT_STATE_ADDRESS)?
            .is_type::<SnapshotState>(&tapedrive::ID)
    }
}

pub trait AccountInfoHelper {
    fn not_empty(&self) -> Result<&Self, ProgramError>;
}

impl AccountInfoHelper for AccountInfo<'_> {
    fn not_empty(&self) -> Result<&Self, ProgramError> {
        if self.data_is_empty() {
            return Err(ProgramError::UninitializedAccount);
        }
        Ok(self)
    }
}

pub trait FromAccountSlice {
    fn from_slice<T>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        len: usize,
    ) -> Result<&T, ProgramError>
    where
        T: AccountDeserialize + Pod;

    fn from_slice_mut<T>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        len: usize,
    ) -> Result<&mut T, ProgramError>
    where
        T: AccountDeserialize + Pod;
}

impl FromAccountSlice for AccountInfo<'_> {
    #[track_caller]
    fn from_slice<T>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        len: usize,
    ) -> Result<&T, ProgramError>
    where
        T: AccountDeserialize + Pod,
    {
        unsafe {
            // Validate account owner.
            self.has_owner(program_id)?;

            // Skip discriminator
            let offset = offset + 8; 

            // Get account data
            let data = self.try_borrow_data()?;
            let slice = core::slice::from_raw_parts(
                data.as_ptr().add(offset), len);

            // Try into desired type
            bytemuck::try_from_bytes::<T>(slice).or(Err(
                ProgramError::InvalidAccountData
            ))
        }
    }

    #[track_caller]
    fn from_slice_mut<T>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        len: usize,
    ) -> Result<&mut T, ProgramError>
    where
        T: AccountDeserialize + Pod,
    {
        unsafe {
            // Validate account owner.
            self.has_owner(program_id)?;

            // Skip discriminator
            let offset = offset + 8; 

            // Get account data
            let mut data = self.try_borrow_mut_data()?;
            let slice = core::slice::from_raw_parts_mut(
                data.as_mut_ptr().add(offset), len);

            // Try into desired type
            bytemuck::try_from_bytes_mut::<T>(slice).or(Err(
                ProgramError::InvalidAccountData
            ))
        }
    }
}
