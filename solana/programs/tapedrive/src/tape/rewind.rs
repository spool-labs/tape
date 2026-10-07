use tape_api::event::TapeRewound;
use tape_api::program::prelude::*;

use crate::tape::helpers::{
    authorize_tape_authority,
    verified_tape_address,
};
use crate::track::helpers::slot_hash_seed;

pub fn process_rewind_tape(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = RewindTape::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        tape_info,
        system_info,
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

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    let tape_address = verified_tape_address(tape_info, tape)?;
    authorize_tape_authority(tape, (*authority_info.key).into())?;

    if tape.is_immutable() {
        return Err(TapeError::TapeImmutable.into());
    }

    // An expired tape is cleared by renewing it.
    let epoch = current_epoch(system);
    if tape.is_expired(epoch) {
        return Err(TapeError::TapeExpired.into());
    }

    tape.rewind(slot_hash_seed(slot_hashes_info)?);

    TapeRewound {
        tape: tape_address,
        id: tape.id,
        epoch,
        tree_start: tape.tracks.tree_start(),
        nonce: tape.nonce,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::Account;
    use tape_core::erasure::{MAX_GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::{BlobData, BlobInfo, TrackObjectInfo};
    use tape_core::track::types::{
        CompressedTrack,
        CompressedTrackProof,
        TrackFlags,
        TrackKind,
        TrackState,
    };
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::types::ContentType;
    use tape_crypto::hash::{hash, hashv};
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, root_from_leaf_hashes, MerkleTree};
    use tape_test::*;

    use crate::track::helpers::select_group;

    const CURRENT_EPOCH: EpochNumber = EpochNumber(60);
    const SLOT_HASH: Hash = Hash([0x5A; 32]);
    const NAME: &[u8] = b"photos/cat.jpg";

    fn slot_hashes_account() -> (Pubkey, Account) {
        let mut data = vec![0u8; 48];
        data[0] = 1;
        data[16..48].copy_from_slice(&SLOT_HASH.to_bytes());
        (sysvar::slot_hashes::ID, Account {
            lamports: 1,
            data,
            owner: sysvar::ID,
            executable: false,
            rent_epoch: 0,
        })
    }

    fn system() -> System {
        System {
            current_epoch: CURRENT_EPOCH,
            live_group_count: 50,
            target_group_count: 50,
            ..permissionless_system()
        }
    }

    /// A live user tape and the two tracks it holds, the second one immutable
    fn live_tape(authority: Pubkey) -> (Tape, [CompressedTrack; 2]) {
        let (tape_address, _) = tape_pda(authority.into());
        let mut tape = Tape {
            id: TapeNumber(7),
            authority: authority.into(),
            capacity: StorageUnits::mb(100),
            active_epoch: EpochNumber(40),
            expiry_epoch: EpochNumber(80),
            nonce: Hash([0x04; 32]),
            ..Tape::zeroed()
        };

        let tracks = [TrackFlags::NONE, TrackFlags::IMMUTABLE].map(|flags| {
            let track = CompressedTrack {
                tape: tape_address,
                tape_number: tape.id,
                track_number: tape.tracks.next_number(),
                key: Hash::new_unique(),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                flags,
                size: StorageUnits::mb(1),
                group: VirtualGroup(0),
                value_hash: Hash([0x22; 32]),
            };
            tape.write_track(&track).expect("write track");
            track
        });

        (tape, tracks)
    }

    /// The tape a rewind leaves: no tracks, nothing used, the counter where it
    /// was and the next nonce
    fn after_rewind(tape: Tape) -> Tape {
        Tape {
            used: StorageUnits::zero(),
            nonce: hashv(&[&tape.nonce.to_bytes(), &SLOT_HASH.to_bytes()]),
            tracks: TrackArchive {
                tree: MerkleTree::zeroed(),
                next_number: tape.tracks.next_number(),
                num_tracks: 0,
            },
            ..tape
        }
    }

    fn accounts(fee_payer: Pubkey, signer: Pubkey, tape: Tape) -> Vec<(Pubkey, Account)> {
        let (tape_address, _) = tape_pda(tape.authority);
        let (system_address, _) = system_pda();

        vec![
            sol(fee_payer, 1_000_000_000),
            sol(signer, 0),
            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(system_address, system().pack(), tapedrive::ID),
            slot_hashes_account(),
        ]
    }

    fn rewind_case(tape: Tape, signer: Pubkey, outcome: Result<(), ProgramError>) {
        let fee_payer = Pubkey::new_unique();
        let (tape_address, _) = tape_pda(tape.authority);
        let instruction = build_rewind_tape_ix(fee_payer.into(), signer.into(), tape_address);

        let (result, expected) = match outcome {
            Ok(()) => (Check::success(), after_rewind(tape)),
            Err(error) => (Check::err(error), tape),
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts(fee_payer, signer, tape),
            &[
                result,
                Check::account(&Pubkey::from(tape_address))
                    .data(expected.pack().as_ref())
                    .build(),
            ],
        );
    }

    // a rewind empties a tape that holds an immutable track, keeps the track
    // counter and the reservation, and moves to the next nonce
    #[test]
    fn rewind_tape() {
        let authority = Pubkey::new_unique();
        let (tape, tracks) = live_tape(authority);

        rewind_case(tape, authority, Ok(()));

        // Every old track is numbered below the tree's new start.
        assert_eq!(after_rewind(tape).tracks.tree_start(), tracks[1].track_number.next());
    }

    // a delegate can delete single tracks and can't rewind the tape
    #[test]
    fn rejects_delegate() {
        let authority = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let tape = Tape {
            delegate: delegate.into(),
            ..live_tape(authority).0
        };

        rewind_case(tape, delegate, Err(ProgramError::InvalidAccountData));
    }

    // a system tape is never rewound
    #[test]
    fn rejects_system_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            flags: TapeFlags::SYSTEM,
            ..live_tape(authority).0
        };

        rewind_case(tape, authority, Err(TapeError::UnexpectedState.into()));
    }

    // an immutable tape keeps its tracks
    #[test]
    fn immutable_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            flags: TapeFlags::IMMUTABLE,
            ..live_tape(authority).0
        };

        rewind_case(tape, authority, Err(TapeError::TapeImmutable.into()));
    }

    // an expired tape is renewed, and a rewind refuses it
    #[test]
    fn expired_tape() {
        let authority = Pubkey::new_unique();
        let tape = Tape {
            expiry_epoch: CURRENT_EPOCH,
            ..live_tape(authority).0
        };

        rewind_case(tape, authority, Err(TapeError::TapeExpired.into()));
    }

    fn blob() -> BlobEncoding {
        let leaves = [Hash::from([0x11; 32]); MAX_GROUP_SIZE];

        BlobEncoding {
            size: StorageUnits::mb(5),
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(
                &leaves[..usize::from(TAPENET_PROFILE.n())],
            ),
            stripe_size: StorageUnits::from_bytes(1024),
            stripe_count: StripeCount(1),
            leaves,
        }
    }

    fn write_ix(fee_payer: Pubkey, authority: Pubkey, tape_address: Address) -> Instruction {
        build_track_write_ix(
            fee_payer.into(),
            authority.into(),
            tape_address,
            BlobInfo {
                object: Some(TrackObjectInfo {
                    name: NAME.to_vec(),
                    content_type: ContentType::ImageJpeg,
                    logical_size: blob().size,
                }),
                data: BlobData::Coded(blob()),
            },
            TrackFlags::NONE,
        )
        .expect("valid blob write instruction")
    }

    /// The track a write lands as on `tape`
    fn written_track(tape: &Tape) -> CompressedTrack {
        let (tape_address, _) = tape_pda(tape.authority);
        let track_number = tape.tracks.next_number();
        let group = select_group(&system(), tape_address, tape.id, track_number, SLOT_HASH)
            .expect("a virtual group");

        CompressedTrack {
            tape: tape_address,
            tape_number: tape.id,
            track_number,
            key: hash(NAME),
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            flags: TrackFlags::NONE,
            size: blob().size,
            group,
            value_hash: blob().get_hash(),
        }
    }

    fn proof_at(leaves: &[Hash], position: usize, state: CompressedTrack) -> CompressedTrackProof {
        let proof = create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(leaves, position)
            .expect("track proof is valid")
            .try_into()
            .expect("proof has correct length");

        CompressedTrackProof { state, proof }
    }

    // after a rewind a write takes the next track number and lands at the
    // tree's first position, and a delete finds it there
    #[test]
    fn tracks_after_rewind() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape, tracks) = live_tape(authority);
        let (tape_address, _) = tape_pda(authority.into());

        let rewound = after_rewind(tape);
        let track = written_track(&rewound);
        assert_eq!(track.track_number, tracks[1].track_number.next());

        let mut written = rewound;
        written.write_track(&track).expect("write track");
        assert_eq!(written.tracks.position(track.track_number), Some(0));

        let proof = proof_at(&[track.get_hash()], 0, track);
        let mut deleted = written;
        deleted.delete_track(&proof).expect("delete track");

        let tape_key = Pubkey::from(tape_address);
        let env = test_env();
        env.process_chain(
            &[
                (
                    &build_rewind_tape_ix(fee_payer.into(), authority.into(), tape_address),
                    &[
                        Check::success(),
                        Check::account(&tape_key).data(rewound.pack().as_ref()).build(),
                    ],
                ),
                (
                    &write_ix(fee_payer, authority, tape_address),
                    &[
                        Check::success(),
                        Check::account(&tape_key).data(written.pack().as_ref()).build(),
                    ],
                ),
                (
                    &build_delete_track_ix(fee_payer.into(), authority.into(), proof),
                    &[
                        Check::success(),
                        Check::account(&tape_key).data(deleted.pack().as_ref()).build(),
                    ],
                ),
            ],
            &accounts(fee_payer, authority, tape),
        );
    }

    // a proof for a track from before the rewind is refused after it
    #[test]
    fn stale_proof() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape, tracks) = live_tape(authority);
        let (tape_address, _) = tape_pda(authority.into());

        let leaves = tracks.map(|track| track.get_hash());
        let old = proof_at(&leaves, 0, tracks[0]);
        tape.tracks.verify(&old).expect("the proof holds before the rewind");

        let rewound = after_rewind(tape);
        let tape_key = Pubkey::from(tape_address);
        let env = test_env();
        env.process_chain(
            &[
                (
                    &build_rewind_tape_ix(fee_payer.into(), authority.into(), tape_address),
                    &[Check::success()],
                ),
                (
                    &build_delete_track_ix(fee_payer.into(), authority.into(), old),
                    &[
                        Check::err(TapeError::BadProof.into()),
                        Check::account(&tape_key).data(rewound.pack().as_ref()).build(),
                    ],
                ),
            ],
            &accounts(fee_payer, authority, tape),
        );
    }

    // a second rewind clears the tracks written since the first, and the
    // counter still counts on
    #[test]
    fn rewind_twice() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (tape, _) = live_tape(authority);
        let (tape_address, _) = tape_pda(authority.into());

        let first = after_rewind(tape);
        let track = written_track(&first);
        let mut written = first;
        written.write_track(&track).expect("write track");
        let second = after_rewind(written);
        assert_eq!(second.tracks.tree_start(), track.track_number.next());

        let rewind = build_rewind_tape_ix(fee_payer.into(), authority.into(), tape_address);
        let tape_key = Pubkey::from(tape_address);
        let env = test_env();
        env.process_chain(
            &[
                (&rewind, &[Check::success()]),
                (&write_ix(fee_payer, authority, tape_address), &[Check::success()]),
                (
                    &rewind,
                    &[
                        Check::success(),
                        Check::account(&tape_key).data(second.pack().as_ref()).build(),
                    ],
                ),
            ],
            &accounts(fee_payer, authority, tape),
        );
    }
}
