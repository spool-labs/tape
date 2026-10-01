use tape_api::program::prelude::*;
use tape_core::track::data::track_key;

use crate::error::TapeError;
use crate::tape::helpers::{authorize_tape_operator, verified_tape_address};
use crate::track::helpers::append_track;

pub fn process_track_write(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let (header, blob) = parse_track_write(data)?;
    let visibility = header.visibility()?;
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

    // A coded blob commits to one leaf per slice, so the root covers the
    // network's n leaves and every leaf past them has to be zero.
    let meta = blob
        .data
        .meta(usize::from(system.code()?.n()))
        .ok_or(TapeError::InvalidCommitment)?;

    let key = track_key(blob.name(), &blob.data);

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    let tape_address = verified_tape_address(tape_info, tape)?;
    authorize_tape_operator(tape, (*authority_info.key).into())?;

    append_track(
        system,
        tape,
        slot_hashes_info,
        tape_address,
        key,
        meta,
        visibility,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_core::encoding::RSParams;
    use tape_core::erasure::{MAX_GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::{BlobData, BlobInfo};
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState, TrackVisibility};
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::types::ContentType;
    use tape_crypto::hash::{hash, hashv};
    use tape_crypto::merkle::{root_from_leaf_hashes, MerkleTree};
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

    const NARROW: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

    // A blob whose first `set` leaves are filled, committed over the first `width`.
    fn blob_of(set: usize, width: usize) -> BlobEncoding {
        let mut leaves = [Hash::default(); MAX_GROUP_SIZE];
        leaves[..set].fill(Hash::from([0x11; 32]));

        BlobEncoding {
            size: StorageUnits::mb(100),
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves[..width]),
            stripe_size: StorageUnits::from_bytes(1024),
            stripe_count: StripeCount(1),
            leaves,
        }
    }

    #[test]
    fn register_track() {
        write_case(permissionless_system(), blob_of(MAX_GROUP_SIZE, MAX_GROUP_SIZE), None);
    }

    // a 3-of-4 cluster registers a blob committed over its four live leaves
    #[test]
    fn narrow_commitment() {
        write_case(trusted_system(NARROW), blob_of(4, 4), None);
    }

    // a leaf set past the width, or a root over the full array, is no commitment
    #[test]
    fn padded_commitment() {
        let refused = Some(TapeError::InvalidCommitment.into());

        write_case(trusted_system(NARROW), blob_of(5, 4), refused.clone());
        write_case(trusted_system(NARROW), blob_of(4, MAX_GROUP_SIZE), refused);
    }

    fn write_case(base: System, blob: BlobEncoding, error: Option<ProgramError>) {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let storage_units = blob.size;

        let name = b"photos/cat.jpg".to_vec();
        let key = hash(&name);

        let (system_address, _) = system_pda();
        let (tape_address, _) = tape_pda(authority.into());

        let instruction = build_track_write_ix(
            fee_payer.into(),
            delegate.into(),
            tape_address,
            BlobInfo {
                object: Some(tape_core::track::data::TrackObjectInfo {
                    name,
                    content_type: ContentType::ImageJpeg,
                    logical_size: storage_units,
                }),
                data: BlobData::Coded(blob),
            },
            TrackVisibility::Public,
        )
        .expect("valid blob write instruction");

        let system = System {
            current_epoch: EpochNumber(0),
            live_group_count: 50,
            ..base
        };
        let tape = Tape {
            id: TapeNumber(1),
            authority: authority.into(),
            delegate: delegate.into(),
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
            sol(delegate, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            slot_hashes_account(),
        ];

        let mut expected_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        let track_number = TrackNumber(0);
        let mixed_hash = hashv(&[
            Hash::default().as_ref(),
            tape_address.as_ref(),
            &tape.id.pack(),
            &track_number.pack(),
        ]);

        let group = GroupIndex(
            u64::from_le_bytes(mixed_hash.0[..8].try_into().unwrap()) % system.live_group_count,
        );

        let track = CompressedTrack {
            tape: tape_address,
            key,
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            visibility: TrackVisibility::Public as u64,
            size: storage_units,
            group,
            value_hash: blob.get_hash(),
        };
        let track_hash = track.get_hash();
        expected_tree.add_leaf_hash(track_hash).unwrap();

        let env = test_env();
        if let Some(error) = error {
            env.process_instruction(&instruction, &accounts, &[Check::err(error)]);
            return;
        }

        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address)).data(
                    Tape {
                        used: storage_units,
                        tracks: TrackArchive {
                            tree: expected_tree,
                            next_number: TrackNumber(1),
                            num_tracks: 1,
                        },
                        ..tape
                    }
                    .pack()
                    .as_ref(),
                )
                .build(),
            ],
        );
    }
}
