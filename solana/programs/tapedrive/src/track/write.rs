use tape_solana::*;
use tape_api::event::TrackWritten;
use tape_api::prelude::*;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::spooler::SpoolGroup;
use tape_core::track::types::CompressedTrack;
use tape_crypto::Hash;
use crate::error::*;

pub fn process_track_write(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let (args, payload) = parse_track_write(data)?;
    let meta = payload
        .meta()
        .ok_or(TapeError::InvalidCommitment)?;
    let [
        fee_payer_info,
        authority_info,

        epoch_info,
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

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(*authority_info.key);

    let tape = tape_info
        .is_writable()?
        .has_address(&tape_address)?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.expiry_epoch <= current_epoch(epoch) {
        return Err(TapeError::TapeExpired.into());
    }

    let track_number = tape.tracks.next_number();
    let spool_group = get_spool_group(
        tape.id,
        track_number,
        slot_hash_seed(slot_hashes_info)?,
    )?;

    let track = CompressedTrack {
        tape: tape_address,
        key: args.key,
        track_number,
        kind: meta.kind as u64,
        state: meta.initial_state as u64,
        size: meta.size,
        spool_group,
        value_hash: meta.value_hash,
    };
    let track_address = track_pda(track.tape, track.track_number).0;
    let track_hash = track.get_hash();

    tape.write_track(&track)?;

    TrackWritten {
        epoch: current_epoch(epoch),
        track: track_address,
        tape: tape_address,
        spool_group: spool_group.0.to_le_bytes(),
        track_number,
        track_hash,
    }.log();

    Ok(())
}

fn get_spool_group(
    tape_id: TapeNumber,
    track_number: TrackNumber,
    seed: Hash,
) -> Result<SpoolGroup, ProgramError> {
    let tape_number: u64 = tape_id.into();
    let mixed = u64::from_le_bytes(
        seed.0[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    )
        .wrapping_add(tape_number)
        .wrapping_add(track_number.0);

    Ok(SpoolGroup(mixed % SPOOL_GROUP_COUNT as u64))
}

fn slot_hash_seed(slot_hashes_info: &AccountInfo<'_>) -> Result<Hash, ProgramError> {
    slot_hashes_info.is_sysvar(&sysvar::slot_hashes::ID)?;
    let slot_hashes_data = slot_hashes_info.try_borrow_data()?;
    let seed = Hash(
        slot_hashes_data[16..48]
            .try_into()
            .map_err(|_| TapeError::UnexpectedState)?
    );
    Ok(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::store::TrackStore;
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

        let data_root = Hash::new_unique();
        let bucket_hash = Hash::new_unique();
        let profile = EncodingProfile::clay_default();

        let leaves = [Hash::default(); SPOOL_GROUP_SIZE];
        // Compute valid commitment from leaves
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);
        let blob = BlobInfo {
            size: storage_units,
            root: data_root,
            commitment,
            profile,
            stripe_size: 1024,
            stripe_count: 1,
            leaves,
        };

        let instruction = build_track_write_blob_ix(
            fee_payer,
            authority,
            bucket_hash,
            blob,
        )
        .expect("valid blob write instruction");

        let (epoch_address, _) = epoch_pda();
        let (tape_address, _) = tape_pda(authority);

        // Setup existing accounts

        let epoch = Epoch::zeroed();
        let tape = Tape {
            id: TapeNumber(1),
            authority: authority,
            capacity: StorageUnits::mb(1000),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(100),
            tracks: TrackStore {
                tree: MerkleTree::<TRACK_TREE_HEIGHT>::new(),
                next_number: TrackNumber(0),
                live_count: 0,
            },
            ..Tape::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(epoch_address, epoch.pack(), tapedrive::ID),
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
            spool_group: SpoolGroup(1),
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
                Check::account(&tape_address).data(
                    Tape {
                        id: tape.id,
                        authority: authority,
                        capacity: tape.capacity,
                        used: storage_units,
                        active_epoch: tape.active_epoch,
                        expiry_epoch: tape.expiry_epoch,
                        tracks: TrackStore {
                            tree: expected_tree,
                            next_number: TrackNumber(1),
                            live_count: 1,
                        },
                        ..Tape::zeroed()
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
