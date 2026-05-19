//! Instruction parsing from Solana transactions.

use solana_transaction_status::UiCompiledInstruction;
use tape_api::event::{
    AssignmentGroupFinalized, CommitteeCreated, CommitteeResized, EpochAdvanced, EpochCommitted,
    EpochCreated, NodeJoinedCommittee, NodeRegistered, PeerSetResized, PoolAdvanced, SnapshotFinalized,
    SpoolSettled, SpoolSynced, TapeDestroyed, TapeReserved, TrackCertified, TrackDeleted,
    TrackInvalidated, TrackWritten, VoteProposed, VoteRecorded,
};
use tape_api::instruction::{
    self as ix, CreateCommittee, CreateEpoch, ResizeCommittee, ResizePeerSet, SettleSpool,
    SyncSpool, TapeInstruction,
};
use tape_api::program::tapedrive::{track_pda, ID as TAPE_PROGRAM_ID};
use bs58::decode as bs58_decode;
use tape_core::spooler::GroupIndex;
use tape_core::track::data::{TrackData, TrackDataSlice};
use tape_core::types::EpochNumber;
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
    CreateEpoch {
        epoch: EpochNumber,
    },
    CreateCommittee {
        epoch: EpochNumber,
    },
    ResizeCommittee {
        epoch: EpochNumber,
    },
    ResizePeerSet,
    CommitEpoch,
    AdvanceEpoch,
    SyncSpool {
        node: Address,
        spool: u64,
    },
    ProposeSnapshot {
        hash: Hash,
    },
    VoteSnapshot {
        hash: Hash,
        group: GroupIndex,
    },
    FinalizeSnapshot {
        epoch: EpochNumber,
    },
    ProposeAssignment {
        hash: Hash,
    },
    VoteAssignment {
        hash: Hash,
        group: GroupIndex,
    },
    FinalizeGroup {
        epoch: EpochNumber,
        group: GroupIndex,
    },
    SettleSpool {
        node: Address,
        spool: u64,
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
    JoinCommittee {
        node: Address,
    },
}

/// Parsed tapedrive instruction with associated event data.
///
/// Instructions include event data that contains execution-time state,
/// eliminating the need to query current RPC state during catch-up.
#[derive(Debug, Clone)]
pub enum ParsedInstruction {
    // Epoch boundary
    CreateEpoch {
        epoch: EpochNumber,
        event: EpochCreated,
    },
    CreateCommittee {
        epoch: EpochNumber,
        event: CommitteeCreated,
    },
    ResizeCommittee {
        epoch: EpochNumber,
        event: CommitteeResized,
    },
    ResizePeerSet {
        event: PeerSetResized,
    },
    CommitEpoch {
        event: EpochCommitted,
    },
    AdvanceEpoch {
        event: EpochAdvanced,
    },
    SyncSpool {
        node: Address,
        spool: u64,
        event: SpoolSynced,
    },
    ProposeSnapshot {
        hash: Hash,
        event: VoteProposed,
    },
    VoteSnapshot {
        hash: Hash,
        group: GroupIndex,
        event: VoteRecorded,
    },
    FinalizeSnapshot {
        epoch: EpochNumber,
        event: SnapshotFinalized,
    },
    ProposeAssignment {
        hash: Hash,
        event: VoteProposed,
    },
    VoteAssignment {
        hash: Hash,
        group: GroupIndex,
        event: VoteRecorded,
    },
    FinalizeGroup {
        epoch: EpochNumber,
        group: GroupIndex,
        event: AssignmentGroupFinalized,
    },
    SettleSpool {
        node: Address,
        spool: u64,
        event: SpoolSettled,
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
    JoinCommittee {
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
        TapeInstruction::CreateEpoch => {
            let args = CreateEpoch::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("create_epoch: {e:?}")))?;
            Ok(Some(RawInstruction::CreateEpoch {
                epoch: EpochNumber::unpack(args.epoch),
            }))
        }

        TapeInstruction::CreateCommittee => {
            let args = CreateCommittee::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("create_committee: {e:?}")))?;
            Ok(Some(RawInstruction::CreateCommittee {
                epoch: EpochNumber::unpack(args.epoch),
            }))
        }

        TapeInstruction::ResizeCommittee => {
            let args = ResizeCommittee::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("resize_committee: {e:?}")))?;
            Ok(Some(RawInstruction::ResizeCommittee {
                epoch: EpochNumber::unpack(args.epoch),
            }))
        }

        TapeInstruction::ResizePeerSet => {
            let _args = ResizePeerSet::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("resize_peer_set: {e:?}")))?;
            Ok(Some(RawInstruction::ResizePeerSet))
        }

        TapeInstruction::CommitEpoch => Ok(Some(RawInstruction::CommitEpoch)),

        TapeInstruction::AdvanceEpoch => Ok(Some(RawInstruction::AdvanceEpoch)),

        TapeInstruction::SyncSpool => {
            let args = SyncSpool::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("sync_spool: {e:?}")))?;
            // Account layout from build_sync_spool_ix: [fee_payer, authority, system, epoch, group, node]
            let node = get_account(5)?;
            let spool = u64::from_le_bytes(args.spool);
            Ok(Some(RawInstruction::SyncSpool { node, spool }))
        }

        TapeInstruction::ProposeSnapshot => {
            let args = ix::ProposeSnapshot::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("propose_snapshot: {e:?}")))?;
            Ok(Some(RawInstruction::ProposeSnapshot { hash: args.hash }))
        }

        TapeInstruction::VoteSnapshot => {
            let args = ix::VoteSnapshot::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("vote_snapshot: {e:?}")))?;
            let group = GroupIndex::unpack(args.group);
            Ok(Some(RawInstruction::VoteSnapshot { hash: args.hash, group }))
        }

        TapeInstruction::FinalizeSnapshot => {
            let args = ix::FinalizeSnapshot::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("finalize_snapshot: {e:?}")))?;
            Ok(Some(RawInstruction::FinalizeSnapshot {
                epoch: EpochNumber::unpack(args.epoch),
            }))
        }

        TapeInstruction::ProposeAssignment => {
            let args = ix::ProposeAssignment::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("propose_assignment: {e:?}")))?;
            Ok(Some(RawInstruction::ProposeAssignment { hash: args.hash }))
        }

        TapeInstruction::VoteAssignment => {
            let args = ix::VoteAssignment::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("vote_assignment: {e:?}")))?;
            let group = GroupIndex::unpack(args.group);
            Ok(Some(RawInstruction::VoteAssignment {
                hash: args.hash,
                group,
            }))
        }

        TapeInstruction::FinalizeGroup => {
            let args = ix::FinalizeGroup::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("finalize_group: {e:?}")))?;
            Ok(Some(RawInstruction::FinalizeGroup {
                epoch: EpochNumber::unpack(args.epoch),
                group: args.payload.group(),
            }))
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
            let args = ix::DeleteTrack::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            let track = track_pda(args.track.state.tape, args.track.state.track_number).0;
            Ok(Some(RawInstruction::DeleteTrack { owner, track }))
        }

        TapeInstruction::CertifyTrack => {
            let args = ix::CertifyTrack::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            let track = track_pda(args.track.state.tape, args.track.state.track_number).0;
            Ok(Some(RawInstruction::CertifyTrack { track }))
        }

        TapeInstruction::InvalidateTrack => {
            let args = ix::InvalidateTrack::try_from_bytes(&ix_data[1..])
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

        TapeInstruction::JoinCommittee => {
            // Account layout: [fee_payer, authority, system, curr_epoch, next_committee, peer_set, node]
            let node = get_account(6)?;
            Ok(Some(RawInstruction::JoinCommittee { node }))
        }

        TapeInstruction::SettleSpool => {
            let args = SettleSpool::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(format!("settle_spool: {e:?}")))?;
            // Account layout from build_settle_spool_ix:
            // [fee_payer, system, archive, curr_epoch, prev_epoch, prev_group, pool]
            let node = get_account(6)?;
            let spool = u64::from_le_bytes(args.spool);
            Ok(Some(RawInstruction::SettleSpool { node, spool }))
        }

        TapeInstruction::AdvancePool => {
            // Account layout from build_advance_pool_ix:
            // [fee_payer, system, prev_committee, pool, history]
            let node = get_account(3)?;
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
        build_finalize_group_ix, build_vote_assignment_ix, build_vote_snapshot_ix,
    };
    use tape_core::bls::BlsSignature;
    use tape_core::cert::{AssignmentGroupPayload, ASSIGNMENT_TREE_HEIGHT};
    use tape_core::spooler::GroupIndex;
    use tape_core::system::VoteKind;
    use tape_core::types::{
        EpochNumber, SpoolBitmap, StorageUnits,
    };
    use tape_crypto::address::Address;
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

    #[test]
    fn parses_vote_snapshot() {
        let (ix, keys) = compiled_instruction(&build_vote_snapshot_ix(
            Address::new_unique(),
            EpochNumber(7),
            Hash::from([0x55; 32]),
            GroupIndex(3),
            SpoolBitmap::zeroed(),
            BlsSignature::zeroed(),
        ));
        match parse_raw_instruction(&ix, &keys).unwrap() {
            Some(RawInstruction::VoteSnapshot { hash, group }) => {
                assert_eq!(hash, Hash::from([0x55; 32]));
                assert_eq!(group, GroupIndex(3));
            }
            other => panic!("expected RawInstruction::VoteSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn parses_vote_assignment() {
        let (ix, keys) = compiled_instruction(&build_vote_assignment_ix(
            Address::new_unique(),
            EpochNumber(7),
            Hash::from([0x66; 32]),
            GroupIndex(3),
            SpoolBitmap::zeroed(),
            BlsSignature::zeroed(),
        ));
        match parse_raw_instruction(&ix, &keys).unwrap() {
            Some(RawInstruction::VoteAssignment { hash, group }) => {
                assert_eq!(hash, Hash::from([0x66; 32]));
                assert_eq!(group, GroupIndex(3));
            }
            other => panic!("expected RawInstruction::VoteAssignment, got {other:?}"),
        }
    }

    #[test]
    fn parses_finalize_group() {
        let group = GroupIndex(3);
        let payload = AssignmentGroupPayload::new(
            group,
            core::array::from_fn(|i| i as u64),
            StorageUnits::mb(100),
        );
        let proof = [Hash::zeroed(); ASSIGNMENT_TREE_HEIGHT];
        let (ix, keys) = compiled_instruction(&build_finalize_group_ix(
            Address::new_unique(),
            EpochNumber(8),
            payload,
            proof,
        ));

        match parse_raw_instruction(&ix, &keys).unwrap() {
            Some(RawInstruction::FinalizeGroup { epoch, group: parsed_group }) => {
                assert_eq!(epoch, EpochNumber(8));
                assert_eq!(parsed_group, group);
            }
            other => panic!("expected RawInstruction::FinalizeGroup, got {other:?}"),
        }
    }

    #[test]
    fn parses_snapshot_events() {
        let voted = VoteRecorded {
            kind: VoteKind::Snapshot as u64,
            vote: Address::new_unique(),
            voting_epoch: EpochNumber(8),
            target_epoch: EpochNumber(7),
            hash: Hash::from([0x55; 32]),
            group: GroupIndex(4),
            signer_count: [14, 0, 0, 0, 0, 0, 0, 0],
            signed_groups: 1u64.to_le_bytes(),
            total_groups: 5u64.to_le_bytes(),
        };

        let instructions = vec![
            RawInstruction::VoteSnapshot {
                hash: Hash::from([0x55; 32]),
                group: GroupIndex(4),
            },
        ];
        let events = vec![TapedriveEvent::VoteRecorded(voted)];

        let merged = merge(instructions, events).expect("merge snapshot instructions");

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::VoteSnapshot { group, event, .. } => {
                assert_eq!(*group, GroupIndex(4));
                assert_eq!(event.target_epoch, voted.target_epoch);
                assert_eq!(event.group, voted.group);
            }
            other => panic!("expected VoteSnapshot, got {other:?}"),
        }
    }
}
