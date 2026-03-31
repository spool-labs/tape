//! Instruction parsing from Solana transactions.

use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, PoolAdvanced, TapeDestroyed,
    TapeReserved, TrackCertified, TrackDeleted, TrackInvalidated, TrackWritten,
};
use tape_api::instruction::{self as ix, TapeInstruction};
use tape_api::program::tapedrive::{track_pda, ID as TAPE_DRIVE_PROGRAM_ID};
use bs58::decode as bs58_decode;
use tape_core::track::data::{TrackData, TrackDataSlice};
use tape_crypto::Hash;

use crate::error::ParseError;

/// Raw instruction before event matching.
///
/// This is an intermediate representation used during parsing.
/// It contains all instruction data but no associated events.
/// Use `merge()` to combine with events.
#[derive(Debug, Clone)]
pub enum RawInstruction {
    AdvanceEpoch,
    SyncEpoch,
    AdvancePool {
        node: Pubkey,
    },
    TrackWrite {
        authority: Pubkey,
        key: Hash,
        value: TrackData,
    },
    DeleteTrack {
        owner: Pubkey,
        track: Pubkey,
    },
    CertifyTrack {
        track: Pubkey,
    },
    InvalidateTrack {
        track: Pubkey,
    },
    ReserveTape {
        owner: Pubkey,
        tape: Pubkey,
    },
    DestroyTape {
        owner: Pubkey,
        tape: Pubkey,
    },
    RegisterNode {
        authority: Pubkey,
        node: Pubkey,
    },
    JoinNetwork {
        node: Pubkey,
    },
}

/// Parsed tapedrive instruction with associated event data.
///
/// Instructions include event data that contains execution-time state,
/// eliminating the need to query current RPC state during catch-up.
#[derive(Debug, Clone)]
pub enum ParsedInstruction {
    // Epoch management
    AdvanceEpoch {
        event: EpochAdvanced,
    },
    SyncEpoch {
        event: NodeSynced,
    },
    AdvancePool {
        node: Pubkey,
        event: PoolAdvanced,
    },

    // Track management
    TrackWrite {
        authority: Pubkey,
        track: Pubkey,
        key: Hash,
        value: TrackData,
        event: TrackWritten,
    },
    DeleteTrack {
        owner: Pubkey,
        track: Pubkey,
        event: TrackDeleted,
    },
    CertifyTrack {
        track: Pubkey,
        event: TrackCertified,
    },
    InvalidateTrack {
        track: Pubkey,
        event: TrackInvalidated,
    },

    // Tape management
    ReserveTape {
        owner: Pubkey,
        tape: Pubkey,
        event: TapeReserved,
    },
    DestroyTape {
        owner: Pubkey,
        tape: Pubkey,
        event: TapeDestroyed,
    },

    // Node management
    RegisterNode {
        authority: Pubkey,
        node: Pubkey,
        event: NodeRegistered,
    },
    JoinNetwork {
        node: Pubkey,
        event: NodeJoinedCommittee,
    },
}

/// Parse a single compiled instruction into a RawInstruction.
pub fn parse_raw_instruction(
    ix: &UiCompiledInstruction,
    account_keys: &[String],
) -> Result<Option<RawInstruction>, ParseError> {
    // Get the program ID
    let program_id_index = ix.program_id_index as usize;
    if program_id_index >= account_keys.len() {
        return Ok(None);
    }

    let program_id: Pubkey = account_keys[program_id_index]
        .parse()
        .map_err(|_| ParseError::InvalidPubkey)?;

    // Only process tapedrive program instructions
    if program_id != TAPE_DRIVE_PROGRAM_ID {
        return Ok(None);
    }

    // Decode instruction data
    let ix_data = bs58_decode(&ix.data)
        .into_vec()
        .map_err(|_| ParseError::InvalidData)?;

    if ix_data.is_empty() {
        return Ok(None);
    }

    // Parse based on discriminator
    let discriminator = ix_data[0];
    let ix_type = TapeInstruction::try_from(discriminator).ok();

    let Some(ix_type) = ix_type else {
        return Ok(None);
    };

    // Helper to get account pubkey at index
    let get_account = |idx: usize| -> Result<Pubkey, ParseError> {
        let account_idx =
            *ix.accounts.get(idx).ok_or(ParseError::MissingAccount("account"))? as usize;
        if account_idx >= account_keys.len() {
            return Err(ParseError::MissingAccount("account index out of bounds"));
        }
        account_keys[account_idx]
            .parse()
            .map_err(|_| ParseError::InvalidPubkey)
    };

    match ix_type {
        TapeInstruction::AdvanceEpoch => Ok(Some(RawInstruction::AdvanceEpoch)),

        TapeInstruction::SyncEpoch => Ok(Some(RawInstruction::SyncEpoch)),

        TapeInstruction::TrackWrite => {
            let authority = get_account(1)?;
            let (header, payload) = ix::parse_track_write(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            let value = match payload {
                TrackDataSlice::Raw(bytes) => TrackData::Raw(bytes.to_vec()),
                TrackDataSlice::Blob(blob) => TrackData::Blob(blob),
            };
            value
                .meta()
                .ok_or(ParseError::Deserialization("invalid track commitment".to_string()))?;
            Ok(Some(RawInstruction::TrackWrite {
                authority,
                key: header.key,
                value,
            }))
        }

        TapeInstruction::DeleteTrack => {
            let owner = get_account(1)?;
            let args = ix::parse_delete_track(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            let track = track_pda(args.track.state.tape, args.track.state.track_number).0;
            Ok(Some(RawInstruction::DeleteTrack { owner, track }))
        }

        TapeInstruction::CertifyTrack => {
            let args = ix::parse_certify_track(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            let track = track_pda(args.track.state.tape, args.track.state.track_number).0;
            Ok(Some(RawInstruction::CertifyTrack { track }))
        }

        TapeInstruction::InvalidateTrack => {
            let args = ix::parse_invalidate_track(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            let track = track_pda(args.track.state.tape, args.track.state.track_number).0;
            Ok(Some(RawInstruction::InvalidateTrack { track }))
        }

        TapeInstruction::ReserveTape => {
            let owner = get_account(0)?;
            let tape = get_account(3)?;
            Ok(Some(RawInstruction::ReserveTape { owner, tape }))
        }

        TapeInstruction::DestroyTape => {
            let owner = get_account(0)?;
            let tape = get_account(2)?;
            Ok(Some(RawInstruction::DestroyTape { owner, tape }))
        }

        TapeInstruction::RegisterNode => {
            let authority = get_account(1)?;
            let node = get_account(5)?;
            Ok(Some(RawInstruction::RegisterNode { authority, node }))
        }

        TapeInstruction::JoinNetwork => {
            let node = get_account(4)?;
            Ok(Some(RawInstruction::JoinNetwork { node }))
        }

        TapeInstruction::AdvancePool => {
            let node = get_account(5)?;
            Ok(Some(RawInstruction::AdvancePool { node }))
        }

        // Instructions we don't need to track
        _ => Ok(None),
    }
}
