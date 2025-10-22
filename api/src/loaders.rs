use steel::*;

use crate::program::*;
use crate::state::{System, Epoch, Archive, Treasury};

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
}
