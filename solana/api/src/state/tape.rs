use tape_solana::*;
use tape_core::prelude::*;
use tape_core::tape::{
    blacklist_tape_number, history_tape_number, snapshot_tape_number, TapeFlags,
};
use tape_core::track::archive::TrackArchive;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_crypto::address::Address;

use crate::errors::TapeError;
use crate::program::tapedrive::SYSTEM_ADDRESS;
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    /// The unique identifier for this tape.
    pub id: TapeNumber,

    /// Tape behavior flags.
    pub flags: u64,

    /// The authority that owns this tape.
    pub authority: Address,

    /// The amount of storage reserved.
    pub capacity: StorageUnits,

    /// The amount of storage used.
    pub used: StorageUnits,

    /// The epoch when this cassette is active.
    pub active_epoch: EpochNumber,

    /// The epoch when this cassette expires.
    pub expiry_epoch: EpochNumber,

    /// A merkle tree of compressed tracks that store the tape data
    pub tracks: TrackArchive,
}

impl Tape {
    pub fn snapshot(epoch: EpochNumber) -> Self {
        Self {
            id: snapshot_tape_number(epoch),
            flags: TapeFlags::SYSTEM,
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            active_epoch: epoch,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Self::zeroed()
        }
    }

    pub fn history(node: NodeId, active_epoch: EpochNumber) -> Self {
        Self {
            id: history_tape_number(node),
            flags: TapeFlags::SYSTEM,
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            active_epoch,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Self::zeroed()
        }
    }

    pub fn blacklist(node: NodeId, active_epoch: EpochNumber) -> Self {
        Self {
            id: blacklist_tape_number(node),
            flags: TapeFlags::SYSTEM,
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            active_epoch,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Self::zeroed()
        }
    }

    pub fn is_snapshot_tape(&self, epoch: EpochNumber) -> bool {
        self.id == snapshot_tape_number(epoch)
            && self.is_system()
            && self.authority == SYSTEM_ADDRESS
            && self.capacity == StorageUnits(u64::MAX)
            && self.active_epoch == epoch
            && self.expiry_epoch == EpochNumber(u64::MAX)
    }

    pub fn is_history_tape(&self, node: NodeId) -> bool {
        self.id == history_tape_number(node)
            && self.is_system()
            && self.authority == SYSTEM_ADDRESS
            && self.capacity == StorageUnits(u64::MAX)
            && self.expiry_epoch == EpochNumber(u64::MAX)
    }

    pub fn is_blacklist_tape(&self, node: NodeId) -> bool {
        self.id == blacklist_tape_number(node)
            && self.is_system()
            && self.authority == SYSTEM_ADDRESS
            && self.capacity == StorageUnits(u64::MAX)
            && self.expiry_epoch == EpochNumber(u64::MAX)
    }

    #[inline(always)]
    pub fn is_system(&self) -> bool {
        TapeFlags::is_system(self.flags)
    }

    pub fn write_track(&mut self, track: &CompressedTrack) -> ProgramResult {
        let new_used = self
            .used
            .checked_add(track.size)
            .ok_or(ProgramError::ArithmeticOverflow)?;

        if new_used > self.capacity {
            return Err(TapeError::NoSpace.into());
        }

        self.tracks
            .append(track)
            .map_err(|_| ProgramError::InvalidInstructionData)?;

        self.used = new_used;
        Ok(())
    }

    pub fn delete_track(&mut self, proof: &CompressedTrackProof) -> ProgramResult {
        self.tracks
            .remove(proof)
            .map_err(|_| ProgramError::InvalidInstructionData)?;

        self.used = self
            .used
            .checked_sub(proof.state.size)
            .ok_or(ProgramError::ArithmeticOverflow)?;

        Ok(())
    }

    pub fn update_track(
        &mut self,
        proof: &CompressedTrackProof,
        updated_track: &CompressedTrack,
    ) -> ProgramResult {
        self.tracks
            .update(proof, updated_track)
            .map_err(|_| ProgramError::InvalidInstructionData)?;
        Ok(())
    }
}

tape_solana::state!(AccountType, Tape);
