use tape_solana::*;
use tape_api::event::TrackWritten;
use tape_api::program::prelude::*;
use tape_core::spooler::GroupIndex;
use tape_core::track::types::CompressedTrack;
use tape_crypto::Hash;

use crate::error::TapeError;

pub fn process_track_write(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let (args, data) = parse_track_write(data)?;
    let meta = data
        .meta()
        .ok_or(TapeError::InvalidCommitment)?;

    let [
        fee_payer_info,
        authority_info,
        system_info,
        tape_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda((*authority_info.key).into());

    let tape = tape_info
        .is_writable()?
        .has_address(&tape_address.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let curr = current_epoch(system);
    if curr < tape.active_epoch || tape.expiry_epoch <= curr {
        return Err(TapeError::TapeExpired.into());
    }

    if system.live_group_count == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let track_number = tape.tracks.next_number();
    let group = select_group(
        tape.id,
        track_number,
        slot_hash_seed(slot_hashes_info)?,
        system.live_group_count,
    )?;

    let track = CompressedTrack {
        tape: tape_address,
        key: args.key,
        track_number,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        group,
        value_hash: meta.value_hash,
    };

    let track_address = track_pda(track.tape, track.track_number).0;
    let track_hash = track.get_hash();

    tape.write_track(&track)?;

    TrackWritten {
        epoch: curr,
        track: track_address,
        tape: tape_address,
        group,
        track_number,
        track_hash,
    }.log();

    Ok(())
}

fn select_group(
    tape_id: TapeNumber,
    track_number: TrackNumber,
    seed: Hash,
    spool_groups: u64,
) -> Result<GroupIndex, ProgramError> {
    let tape_number: u64 = tape_id.into();
    let mixed = u64::from_le_bytes(
        seed.0[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    )
        .wrapping_add(tape_number)
        .wrapping_add(track_number.0);

    Ok(GroupIndex(mixed % spool_groups))
}

fn slot_hash_seed(slot_hashes_info: &AccountInfo<'_>) -> Result<Hash, ProgramError> {
    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;
    let slot_hashes_data = slot_hashes_info.try_borrow_data()?;
    let seed = Hash(
        slot_hashes_data
            .get(16..48)
            .ok_or(TapeError::UnexpectedState)?
            .try_into()
            .map_err(|_| TapeError::UnexpectedState)?
    );
    Ok(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{TrackKind, TrackState};
    use solana_sdk::account::Account;
    use tape_crypto::merkle::{MerkleTree, root_from_leaf_hashes};
    use tape_test::*;

    fn slot_hashes_account() -> (Pubkey, Account) {
        let mut data = vec![0u8; 48];
        data[0] = 1; // count = 1
        (sysvar::slot_hashes::ID, Account {
            lamports: 1,
            data,
            owner: sysvar::ID,
            executable: false,
            rent_epoch: 0,
        })
    }

    #[test]
    fn test_register_track() {
        use tape_core::encoding::EncodingProfile;

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let storage_units = StorageUnits::mb(100);

        let bucket_hash = Hash::new_unique();
        let profile = EncodingProfile::clay_default();

        let leaves = [Hash::default(); GROUP_SIZE];
        // Compute valid commitment from leaves
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        let blob = BlobInfo {
            size: storage_units,
            commitment,
            profile,
            stripe_size: StorageUnits::from_bytes(1024),
            stripe_count: StripeCount(1),
            leaves,
        };

        let instruction = build_track_write_blob_ix(fee_payer.into(), authority.into(),
            bucket_hash,
            blob,
        )
        .expect("valid blob write instruction");

        let (system_address, _) = system_pda();
        let (tape_address, _) = tape_pda(authority.into());

        let system = System {
            current_epoch: EpochNumber(0),
            live_group_count: 50,
            ..System::zeroed()
        };
        let tape = Tape {
            id: TapeNumber(1),
            authority: authority.into(),
            capacity: StorageUnits::mb(1000),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(100),
            tracks: TrackArchive {
                tree: MerkleTree::<TRACK_TREE_HEIGHT>::new(),
                next_number: TrackNumber(0),
                num_tracks: 0,
            },
            ..Tape::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        let mut expected_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number: TrackNumber(0),
            kind: TrackKind::Blob as u64,
            state: TrackState::Registered as u64,
            size: storage_units,
            group: GroupIndex(1),
            value_hash: blob.get_hash(),
        };
        let track_hash = track.get_hash();
        expected_tree.add_leaf_hash(track_hash).unwrap();

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address)).data(
                    Tape {
                        id: tape.id,
                        authority: authority.into(),
                        capacity: tape.capacity,
                        used: storage_units,
                        active_epoch: tape.active_epoch,
                        expiry_epoch: tape.expiry_epoch,
                        tracks: TrackArchive {
                            tree: expected_tree,
                            next_number: TrackNumber(1),
                            num_tracks: 1,
                        },
                        ..Tape::zeroed()
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
