use steel::*;

use crate::consts::*;
use crate::state::{System, Archive, Epoch};

pub trait AccountInfoLoader {
    fn is_tape_system(&self) -> Result<&Self, ProgramError>;
    fn is_tape_archive(&self) -> Result<&Self, ProgramError>;
    fn is_tape_epoch(&self) -> Result<&Self, ProgramError>;
    fn is_tape_treasury(&self) -> Result<&Self, ProgramError>;
    fn is_tape_mint(&self) -> Result<&Self, ProgramError>;
    fn is_tape_metadata(&self) -> Result<&Self, ProgramError>;
}

impl AccountInfoLoader for AccountInfo<'_> {
    fn is_tape_system(&self) -> Result<&Self, ProgramError> {
        self.has_address(&SYSTEM_ADDRESS)?
            .is_type::<System>(&crate::ID)
    }

    fn is_tape_archive(&self) -> Result<&Self, ProgramError> {
        self.has_address(&ARCHIVE_ADDRESS)?
            .is_type::<Archive>(&crate::ID)
    }

    fn is_tape_epoch(&self) -> Result<&Self, ProgramError> {
        self.has_address(&EPOCH_ADDRESS)?
            .is_type::<Epoch>(&crate::ID)
    }

    fn is_tape_treasury(&self) -> Result<&Self, ProgramError> {
        self.has_address(&TREASURY_ADDRESS)?
            .is_type::<crate::state::Treasury>(&crate::ID)
    }

    fn is_tape_mint(&self) -> Result<&Self, ProgramError> {
        self.has_address(&MINT_ADDRESS)?
            .has_owner(&spl_token::ID)
    }

    fn is_tape_metadata(&self) -> Result<&Self, ProgramError> {
        self.has_address(&METADATA_ADDRESS)?
            .has_owner(&mpl_token_metadata::ID)
    }
}
