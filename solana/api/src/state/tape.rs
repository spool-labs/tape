use tape_solana::*;
use tape_core::prelude::*;
use tape_core::track::archive::TrackArchive;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_crypto::address::Address;

use crate::errors::TapeError;
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    /// The unique identifier for this tape.
    pub id: TapeNumber,

    /// The authority that owns this tape.
    pub authority: Address,

    /// The amount of storage reserved.
    pub capacity: StorageUnits,

    /// The amount of storage used.
    pub used: StorageUnits,

    /// The epoch when this resource is active.
    pub active_epoch: EpochNumber,

    /// The epoch when this resource expires.
    pub expiry_epoch: EpochNumber,

    /// A merkle tree of compressed tracks that store the tape data
    pub tracks: TrackArchive,
}

impl Tape {
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
