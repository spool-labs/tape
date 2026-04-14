use tape_solana::*;

use crate::program::{
    tapedrive, ARCHIVE_ADDRESS, ARCHIVE_ATA, EPOCH_ADDRESS, METADATA_ADDRESS, MINT_ADDRESS,
    SYSTEM_ADDRESS, TREASURY_ADDRESS,
};
use crate::state::{Archive, Epoch, System, Treasury};

pub trait AccountInfoLoader {
    fn is_system(&self) -> Result<&Self, ProgramError>;
    fn is_epoch(&self) -> Result<&Self, ProgramError>;
    fn is_archive(&self) -> Result<&Self, ProgramError>;
    fn is_archive_ata(&self) -> Result<&Self, ProgramError>;
    fn is_mint(&self) -> Result<&Self, ProgramError>;
    fn is_metadata(&self) -> Result<&Self, ProgramError>;
    fn is_treasury(&self) -> Result<&Self, ProgramError>;
}

impl AccountInfoLoader for AccountInfo<'_> {
    fn is_system(&self) -> Result<&Self, ProgramError> {
        let system_address: Pubkey = SYSTEM_ADDRESS.into();

        self.has_address(&system_address)?
            .is_type::<System>(&tapedrive::ID)
    }

    fn is_epoch(&self) -> Result<&Self, ProgramError> {
        let epoch_address: Pubkey = EPOCH_ADDRESS.into();

        self.has_address(&epoch_address)?
            .is_type::<Epoch>(&tapedrive::ID)
    }

    fn is_archive(&self) -> Result<&Self, ProgramError> {
        let archive_address: Pubkey = ARCHIVE_ADDRESS.into();

        self.has_address(&archive_address)?
            .is_type::<Archive>(&tapedrive::ID)
    }

    fn is_archive_ata(&self) -> Result<&Self, ProgramError> {
        let archive_ata: Pubkey = ARCHIVE_ATA.into();

        self.has_address(&archive_ata)?
            .has_owner(&spl_token::ID)
    }

    fn is_mint(&self) -> Result<&Self, ProgramError> {
        let mint_address: Pubkey = MINT_ADDRESS.into();

        self.has_address(&mint_address)?
            .has_owner(&spl_token::ID)
    }

    fn is_metadata(&self) -> Result<&Self, ProgramError> {
        let metadata_address: Pubkey = METADATA_ADDRESS.into();

        self.has_address(&metadata_address)?
            .has_owner(&mpl_token_metadata::ID)
    }

    fn is_treasury(&self) -> Result<&Self, ProgramError> {
        let treasury_address: Pubkey = TREASURY_ADDRESS.into();

        self.has_address(&treasury_address)?
            .is_type::<Treasury>(&tapedrive::ID)
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
