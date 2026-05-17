use tape_solana::*;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;

use crate::program::{
    tapedrive, ARCHIVE_ADDRESS, ARCHIVE_ATA, METADATA_ADDRESS, MINT_ADDRESS,
    PEER_SET_ADDRESS, SYSTEM_ADDRESS, TREASURY_ADDRESS,
};
use crate::errors::TapeError;
use crate::program::tapedrive::{committee_pda, epoch_pda, group_pda, snapshot_tape_pda};
use crate::state::{Archive, Committee, Epoch, Group, PeerSet, System, Tape, Treasury};

pub trait AccountInfoLoader {
    fn is_system(&self) -> Result<&Self, ProgramError>;
    fn is_epoch(&self, epoch: EpochNumber) -> Result<&Self, ProgramError>;
    fn is_committee(&self, epoch: EpochNumber) -> Result<&Self, ProgramError>;
    fn is_group(&self, epoch: EpochNumber, group: SpoolGroup) -> Result<&Self, ProgramError>;
    fn is_snapshot_tape(&self, epoch: EpochNumber) -> Result<&Self, ProgramError>;
    fn is_peer_set(&self) -> Result<&Self, ProgramError>;
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

    fn is_epoch(&self, epoch: EpochNumber) -> Result<&Self, ProgramError> {
        let (epoch_address, _) = epoch_pda(epoch);
        let epoch_address: Pubkey = epoch_address.into();

        self.has_address(&epoch_address)?
            .is_type::<Epoch>(&tapedrive::ID)
    }

    fn is_committee(&self, epoch: EpochNumber) -> Result<&Self, ProgramError> {
        let (committee_address, _) = committee_pda(epoch);
        let committee_address: Pubkey = committee_address.into();

        self.has_address(&committee_address)?
            .is_type::<Committee>(&tapedrive::ID)
    }

    fn is_group(&self, epoch: EpochNumber, group: SpoolGroup) -> Result<&Self, ProgramError> {
        let (group_address, _) = group_pda(epoch, group);
        let group_address: Pubkey = group_address.into();

        self.has_address(&group_address)?
            .is_type::<Group>(&tapedrive::ID)
    }

    fn is_snapshot_tape(&self, epoch: EpochNumber) -> Result<&Self, ProgramError> {
        let (snapshot_tape_address, _) = snapshot_tape_pda(epoch);
        let snapshot_tape_address: Pubkey = snapshot_tape_address.into();

        let tape = self
            .has_address(&snapshot_tape_address)?
            .is_type::<Tape>(&tapedrive::ID)?
            .as_account::<Tape>(&tapedrive::ID)?;

        if !tape.is_snapshot_tape(epoch) {
            return Err(TapeError::UnexpectedState.into());
        }

        Ok(self)
    }

    fn is_peer_set(&self) -> Result<&Self, ProgramError> {
        let peer_set_address: Pubkey = PEER_SET_ADDRESS.into();

        self.has_address(&peer_set_address)?
            .is_type::<PeerSet>(&tapedrive::ID)
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

    fn from_slice_array<B>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        capacity: usize,
    ) -> Result<&[B], ProgramError>
    where
        B: Pod;

    fn from_slice_array_mut<B>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        capacity: usize,
    ) -> Result<&mut [B], ProgramError>
    where
        B: Pod;
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

    #[track_caller]
    fn from_slice_array<B>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        capacity: usize,
    ) -> Result<&[B], ProgramError>
    where
        B: Pod,
    {
        unsafe {
            self.has_owner(program_id)?;

            // Skip discriminator
            let offset = offset + 8;

            let data = self.try_borrow_data()?;
            let bytes = capacity.checked_mul(core::mem::size_of::<B>())
                .ok_or(ProgramError::InvalidAccountData)?;
            if data.len() < offset + bytes {
                return Err(ProgramError::AccountDataTooSmall);
            }

            Ok(core::slice::from_raw_parts(
                data.as_ptr().add(offset) as *const B,
                capacity,
            ))
        }
    }

    #[track_caller]
    fn from_slice_array_mut<B>(
        &self,
        program_id: &Pubkey,
        offset: usize,
        capacity: usize,
    ) -> Result<&mut [B], ProgramError>
    where
        B: Pod,
    {
        unsafe {
            self.has_owner(program_id)?;

            // Skip discriminator
            let offset = offset + 8;

            let mut data = self.try_borrow_mut_data()?;
            let bytes = capacity.checked_mul(core::mem::size_of::<B>())
                .ok_or(ProgramError::InvalidAccountData)?;
            if data.len() < offset + bytes {
                return Err(ProgramError::AccountDataTooSmall);
            }

            Ok(core::slice::from_raw_parts_mut(
                data.as_mut_ptr().add(offset) as *mut B,
                capacity,
            ))
        }
    }
}
