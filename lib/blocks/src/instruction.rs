//! Instruction parsing from Solana transactions.

use solana_transaction_status::UiCompiledInstruction;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, PoolAdvanced,
    SnapshotCertified, SnapshotFinalized, SnapshotInit, TapeDestroyed, TapeReserved,
    TrackCertified, TrackDeleted, TrackInvalidated, TrackWritten,
};
use tape_api::instruction::{self as ix, TapeInstruction};
use tape_api::program::tapedrive::{track_pda, ID as TAPE_PROGRAM_ID};
use bs58::decode as bs58_decode;
use tape_core::track::data::{TrackData, TrackDataSlice};
use tape_crypto::address::Address;
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
    InitSnapshotEpoch,
    CertifySnapshotGroup,
    FinalizeSnapshotEpoch,
    AdvancePool {
        node: Address,
    },
    TrackWrite {
        authority: Address,
        key: Hash,
        value: TrackData,
    },
    DeleteTrack {
        owner: Address,
        track: Address,
    },
    CertifyTrack {
        track: Address,
    },
    InvalidateTrack {
        track: Address,
    },
    ReserveTape {
        owner: Address,
        tape: Address,
    },
    DestroyTape {
        owner: Address,
        tape: Address,
    },
    RegisterNode {
        authority: Address,
        node: Address,
    },
    JoinNetwork {
        node: Address,
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
    InitSnapshotEpoch {
        event: SnapshotInit,
    },
    CertifySnapshotGroup {
        event: SnapshotCertified,
    },
    FinalizeSnapshotEpoch {
        event: SnapshotFinalized,
    },
    AdvancePool {
        node: Address,
        event: PoolAdvanced,
    },

    // Track management
    TrackWrite {
        authority: Address,
        track: Address,
        key: Hash,
        value: TrackData,
        event: TrackWritten,
    },
    DeleteTrack {
        owner: Address,
        track: Address,
        event: TrackDeleted,
    },
    CertifyTrack {
        track: Address,
        event: TrackCertified,
    },
    InvalidateTrack {
        track: Address,
        event: TrackInvalidated,
    },

    // Tape management
    ReserveTape {
        owner: Address,
        tape: Address,
        event: TapeReserved,
    },
    DestroyTape {
        owner: Address,
        tape: Address,
        event: TapeDestroyed,
    },

    // Node management
    RegisterNode {
        authority: Address,
        node: Address,
        event: NodeRegistered,
    },
    JoinNetwork {
        node: Address,
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

    let program_id: Address = account_keys[program_id_index]
        .parse()
        .map_err(|_| ParseError::InvalidPubkey)?;

    // Only process tapedrive program instructions
    if program_id != Address::from(TAPE_PROGRAM_ID) {
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
    let get_account = |idx: usize| -> Result<Address, ParseError> {
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

        TapeInstruction::InitSnapshotEpoch => Ok(Some(RawInstruction::InitSnapshotEpoch)),

        TapeInstruction::CertifySnapshotGroup => Ok(Some(RawInstruction::CertifySnapshotGroup)),

        TapeInstruction::FinalizeSnapshotEpoch => Ok(Some(RawInstruction::FinalizeSnapshotEpoch)),

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

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use crate::event::TapedriveEvent;
    use crate::merge::merge;
    use solana_sdk::instruction::Instruction;
    use solana_transaction_status::UiCompiledInstruction;
    use tape_api::event::{
        SnapshotCertified, SnapshotFinalized, SnapshotInit,
    };
    use tape_api::instruction::{
        build_certify_snapshot_group_ix, build_finalize_snapshot_epoch_ix,
        build_init_snapshot_epoch_ix,
    };
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{CommitteeBitmap, EpochNumber, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_api::program::tapedrive::ID as TAPE_PROGRAM_ID;

    fn compiled_instruction(instruction: &Instruction) -> (UiCompiledInstruction, Vec<String>) {
        let account_keys = vec![TAPE_PROGRAM_ID.to_string()];
        (
            UiCompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: bs58::encode(&instruction.data).into_string(),
                stack_height: None,
            },
            account_keys,
        )
    }

    #[test]
    fn parses_snapshot_instructions() {
        let (ix, keys) = compiled_instruction(&build_init_snapshot_epoch_ix(
            Address::new_unique(),
            EpochNumber(7),
        ));
        assert!(matches!(
            parse_raw_instruction(&ix, &keys).unwrap(),
            Some(RawInstruction::InitSnapshotEpoch)
        ));

        let (ix, keys) = compiled_instruction(
            &build_certify_snapshot_group_ix(
                Address::new_unique(),
                EpochNumber(7),
                EpochNumber(8),
                SpoolGroup(3),
                StorageUnits::from_bytes(1_025),
                Hash::from([0x10; 32]),
                Hash::from([0x11; 32]),
                EncodingProfile::basic_default(),
                StorageUnits::from_bytes(512),
                StripeCount(4),
                [Hash::from([0x22; 32]); SPOOL_GROUP_SIZE],
                CommitteeBitmap::zeroed(),
                BlsSignature::zeroed(),
            ));

        assert!(matches!(
            parse_raw_instruction(&ix, &keys).unwrap(),
            Some(RawInstruction::CertifySnapshotGroup)
        ));

        let (ix, keys) = compiled_instruction(&build_finalize_snapshot_epoch_ix(
            Address::new_unique(),
            EpochNumber(7),
        ));
        assert!(matches!(
            parse_raw_instruction(&ix, &keys).unwrap(),
            Some(RawInstruction::FinalizeSnapshotEpoch)
        ));
    }

    #[test]
    fn parses_snapshot_events() {
        let init = SnapshotInit {
            parent: EpochNumber(6),
            current: EpochNumber(7),
        };
        let cert = SnapshotCertified {
            epoch: EpochNumber(7),
            group: SpoolGroup(4),
            track: TrackNumber(9),
            commitment: Hash::from([0x44; 32]),
            signer_count: [2; 8],
            signer_weight: [3; 8],
        };
        let finalized = SnapshotFinalized {
            parent: EpochNumber(6),
            current: EpochNumber(7),
        };

        let instructions = vec![
            RawInstruction::InitSnapshotEpoch,
            RawInstruction::CertifySnapshotGroup,
            RawInstruction::FinalizeSnapshotEpoch,
        ];
        let events = vec![
            TapedriveEvent::SnapshotInit(init),
            TapedriveEvent::SnapshotCertified(cert),
            TapedriveEvent::SnapshotFinalized(finalized),
        ];

        let merged = merge(instructions, events).expect("merge snapshot instructions");

        assert!(matches!(
            merged.as_slice(),
            [
                ParsedInstruction::InitSnapshotEpoch { event: decoded_init },
                ParsedInstruction::CertifySnapshotGroup { event: decoded_cert },
                ParsedInstruction::FinalizeSnapshotEpoch { event: decoded_finalized },
            ] if decoded_init.parent == init.parent
                && decoded_init.current == init.current
                && decoded_cert.epoch == cert.epoch
                && decoded_cert.group == cert.group
                && decoded_cert.track == cert.track
                && decoded_cert.commitment == cert.commitment
                && decoded_finalized.parent == finalized.parent
                && decoded_finalized.current == finalized.current
        ));
    }
}
