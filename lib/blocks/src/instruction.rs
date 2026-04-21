//! Instruction parsing from Solana transactions.

use solana_transaction_status::UiCompiledInstruction;
use tape_api::event::{
    EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, PoolAdvanced,
    SnapshotReserved, SnapshotSigned, SnapshotWritten, TapeDestroyed, TapeReserved,
    TrackCertified, TrackDeleted, TrackInvalidated, TrackWritten, VoteClosed,
};
use tape_api::instruction::{self as ix, TapeInstruction, WriteSnapshot};
use tape_api::program::tapedrive::{track_pda, ID as TAPE_PROGRAM_ID};
use bs58::decode as bs58_decode;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::{TrackData, TrackDataSlice};
use tape_core::types::ChunkNumber;
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
    ReserveSnapshot,
    WriteSnapshot {
        group: SpoolGroup,
        chunk: ChunkNumber,
        blob: BlobInfo,
    },
    SignSnapshot,
    CloseVote {
        vote: Address,
    },
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
    ReserveSnapshot {
        event: SnapshotReserved,
    },
    WriteSnapshot {
        group: SpoolGroup,
        chunk: ChunkNumber,
        blob: BlobInfo,
        event: SnapshotWritten,
    },
    SignSnapshot {
        event: SnapshotSigned,
    },
    CloseVote {
        vote: Address,
        event: VoteClosed,
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

        TapeInstruction::ReserveSnapshot => Ok(Some(RawInstruction::ReserveSnapshot)),

        TapeInstruction::WriteSnapshot => {
            let args = WriteSnapshot::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("write_snapshot: {e:?}")))?;
            let group = SpoolGroup::unpack(args.group);
            let chunk = ChunkNumber::unpack(args.chunk);
            let blob = BlobInfo::unpack(args.snapshot);
            Ok(Some(RawInstruction::WriteSnapshot {
                group,
                chunk,
                blob,
            }))
        }

        TapeInstruction::SignSnapshot => Ok(Some(RawInstruction::SignSnapshot)),

        TapeInstruction::CloseVote => {
            ix::CloseVote::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("close_vote: {e:?}")))?;
            let vote = get_account(4)?;
            Ok(Some(RawInstruction::CloseVote { vote }))
        }

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
    use tape_api::instruction::{
        build_close_vote_ix, build_reserve_snapshot_ix, build_sign_snapshot_ix,
        build_write_snapshot_ix,
    };
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SLICE_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{
        EpochNumber, SpoolGroupBitmap, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_crypto::Hash;
    use tape_api::program::tapedrive::ID as TAPE_PROGRAM_ID;

    fn compiled_instruction(instruction: &Instruction) -> (UiCompiledInstruction, Vec<String>) {
        let mut account_keys = vec![TAPE_PROGRAM_ID.to_string()];
        let accounts = instruction
            .accounts
            .iter()
            .map(|meta| {
                account_keys.push(meta.pubkey.to_string());
                (account_keys.len() - 1) as u8
            })
            .collect();
        (
            UiCompiledInstruction {
                program_id_index: 0,
                accounts,
                data: bs58::encode(&instruction.data).into_string(),
                stack_height: None,
            },
            account_keys,
        )
    }

    fn valid_blob() -> BlobInfo {
        let leaves: [Hash; SPOOL_GROUP_SIZE] =
            core::array::from_fn(|i| hash_leaf(&vec![i as u8; 64]));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        BlobInfo {
            size: StorageUnits::from_bytes(64 * SPOOL_GROUP_SIZE as u64),
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(SPOOL_GROUP_SIZE as u64),
            leaves,
        }
    }

    #[test]
    fn parses_snapshot_instructions() {
        // ReserveSnapshot
        let (ix, keys) = compiled_instruction(&build_reserve_snapshot_ix(
            Address::new_unique(),
            EpochNumber(7),
        ));
        assert!(matches!(
            parse_raw_instruction(&ix, &keys).unwrap(),
            Some(RawInstruction::ReserveSnapshot)
        ));

        let blob = valid_blob();
        let (ix, keys) = compiled_instruction(&build_write_snapshot_ix(
            Address::new_unique(),
            Address::new_unique(),
            EpochNumber(7),
            SpoolGroup(3),
            ChunkNumber(5),
            SpoolGroupBitmap::zeroed(),
            BlsSignature::zeroed(),
            &blob,
        ));

        let parsed = parse_raw_instruction(&ix, &keys).unwrap();
        match parsed {
            Some(RawInstruction::WriteSnapshot {
                group,
                chunk,
                blob: parsed_blob,
            }) => {
                assert_eq!(group, SpoolGroup(3));
                assert_eq!(chunk, ChunkNumber(5));
                assert_eq!(parsed_blob, blob);
            }
            other => panic!("expected RawInstruction::WriteSnapshot, got {other:?}"),
        }

        // SignSnapshot
        let (ix, keys) = compiled_instruction(&build_sign_snapshot_ix(
            Address::new_unique(),
            EpochNumber(7),
            SpoolGroup(3),
            SpoolGroupBitmap::zeroed(),
            BlsSignature::zeroed(),
        ));
        assert!(matches!(
            parse_raw_instruction(&ix, &keys).unwrap(),
            Some(RawInstruction::SignSnapshot)
        ));
    }

    #[test]
    fn parses_close_vote_instruction() {
        let vote = Address::new_unique();
        let (ix, keys) = compiled_instruction(&build_close_vote_ix(
            Address::new_unique(),
            Address::new_unique(),
            Address::new_unique(),
            vote,
        ));

        assert!(matches!(
            parse_raw_instruction(&ix, &keys).unwrap(),
            Some(RawInstruction::CloseVote { vote: parsed_vote }) if parsed_vote == vote
        ));
    }

    #[test]
    fn parses_snapshot_events() {
        let blob = valid_blob();
        let reserved = SnapshotReserved {
            epoch: EpochNumber(7),
        };
        let written = SnapshotWritten {
            epoch: EpochNumber(7),
            group: SpoolGroup(4),
            track: Address::new_unique(),
            track_number: TrackNumber(9),
            track_hash: Hash::from([0x44; 32]),
        };
        let signed = SnapshotSigned {
            epoch: EpochNumber(7),
            group: SpoolGroup(4),
            state: 0,
        };

        let instructions = vec![
            RawInstruction::ReserveSnapshot,
            RawInstruction::WriteSnapshot {
                group: SpoolGroup(4),
                chunk: ChunkNumber(0),
                blob: blob.clone(),
            },
            RawInstruction::SignSnapshot,
        ];
        let events = vec![
            TapedriveEvent::SnapshotReserved(reserved),
            TapedriveEvent::SnapshotWritten(written),
            TapedriveEvent::SnapshotSigned(signed),
        ];

        let merged = merge(instructions, events).expect("merge snapshot instructions");

        assert_eq!(merged.len(), 3);
        match &merged[0] {
            ParsedInstruction::ReserveSnapshot { event } => {
                assert_eq!(event.epoch, reserved.epoch);
            }
            other => panic!("expected ReserveSnapshot, got {other:?}"),
        }
        match &merged[1] {
            ParsedInstruction::WriteSnapshot {
                group,
                chunk,
                blob: parsed_blob,
                event,
            } => {
                assert_eq!(*group, SpoolGroup(4));
                assert_eq!(*chunk, ChunkNumber(0));
                assert_eq!(*parsed_blob, blob);
                assert_eq!(event.epoch, written.epoch);
                assert_eq!(event.track_number, written.track_number);
                assert_eq!(event.track_hash, written.track_hash);
            }
            other => panic!("expected WriteSnapshot, got {other:?}"),
        }
        match &merged[2] {
            ParsedInstruction::SignSnapshot { event } => {
                assert_eq!(event.epoch, signed.epoch);
                assert_eq!(event.group, signed.group);
            }
            other => panic!("expected SignSnapshot, got {other:?}"),
        }
    }
}
