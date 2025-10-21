//use steel::*;

//use crate::state::{System, Epoch, Committee, Archive};
//
//pub trait AccountInfoLoader {
//    fn is_system(&self) -> Result<&Self, ProgramError>;
//    fn is_epoch(&self) -> Result<&Self, ProgramError>;
//    fn is_archive(&self) -> Result<&Self, ProgramError>;
//    fn is_archive_ata(&self) -> Result<&Self, ProgramError>;
//    fn is_current_committee(&self) -> Result<&Self, ProgramError>;
//    fn is_previous_committee(&self) -> Result<&Self, ProgramError>;
//    fn is_mint(&self) -> Result<&Self, ProgramError>;
//    fn is_metadata(&self) -> Result<&Self, ProgramError>;
//}
//
//impl AccountInfoLoader for AccountInfo<'_> {
//    fn is_system(&self) -> Result<&Self, ProgramError> {
//        self.has_address(&SYSTEM_ADDRESS)?
//            .is_type::<System>(&crate::ID)
//    }
//
//    fn is_epoch(&self) -> Result<&Self, ProgramError> {
//        self.has_address(&EPOCH_ADDRESS)?
//            .is_type::<Epoch>(&crate::ID)
//    }
//
//    fn is_archive(&self) -> Result<&Self, ProgramError> {
//        self.has_address(&ARCHIVE_ADDRESS)?
//            .is_type::<Archive>(&crate::ID)
//    }
//
//    fn is_archive_ata(&self) -> Result<&Self, ProgramError> {
//        self.has_address(&ARCHIVE_ATA)?
//            .has_owner(&spl_token::ID)
//    }
//
//    fn is_current_committee(&self) -> Result<&Self, ProgramError> {
//        let (committee_address, _) = current_committee_pda();
//        self.has_address(&committee_address)?
//            .is_type::<Committee>(&crate::ID)
//    }
//
//    fn is_previous_committee(&self) -> Result<&Self, ProgramError> {
//        let (prev_committee_address, _) = previous_committee_pda();
//        self.has_address(&prev_committee_address)?
//            .is_type::<Committee>(&crate::ID)
//    }
//
//    fn is_mint(&self) -> Result<&Self, ProgramError> {
//        self.has_address(&MINT_ADDRESS)?
//            .has_owner(&spl_token::ID)
//    }
//
//    fn is_metadata(&self) -> Result<&Self, ProgramError> {
//        self.has_address(&METADATA_ADDRESS)?
//            .has_owner(&mpl_token_metadata::ID)
//    }
//}
