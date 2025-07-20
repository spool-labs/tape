use steel::*;

use crate::consts::*;
use crate::state::{Archive, Epoch, Treasury};

pub trait AccountInfoLoader {
    fn is_archive(&self) -> Result<&Self, ProgramError>;
    fn is_epoch(&self) -> Result<&Self, ProgramError>;
    fn is_block(&self) -> Result<&Self, ProgramError>;
    fn is_treasury(&self) -> Result<&Self, ProgramError>;
    fn is_treasury_ata(&self) -> Result<&Self, ProgramError>;
}

impl AccountInfoLoader for AccountInfo<'_> {
    fn is_archive(&self) -> Result<&Self, ProgramError> {
        self.has_address(&ARCHIVE_ADDRESS)?
            .is_type::<Archive>(&crate::ID)
    }

    fn is_epoch(&self) -> Result<&Self, ProgramError> {
        self.has_address(&EPOCH_ADDRESS)?
            .is_type::<Epoch>(&crate::ID)
    }

    fn is_block(&self) -> Result<&Self, ProgramError> {
        self.has_address(&BLOCK_ADDRESS)?
            .is_type::<crate::state::Block>(&crate::ID)
    }

    fn is_treasury(&self) -> Result<&Self, ProgramError> {
        self.has_address(&TREASURY_ADDRESS)?
            .is_type::<Treasury>(&crate::ID)
    }

    fn is_treasury_ata(&self) -> Result<&Self, ProgramError> {
        self.has_address(&TREASURY_ATA)
    }
}
