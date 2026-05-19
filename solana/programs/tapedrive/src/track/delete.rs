use tape_api::program::prelude::*;
use tape_api::event::TrackDeleted;

pub fn process_delete_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = DeleteTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(tape.authority);

    let proof = args.track;
    let track_address = track_pda(proof.state.tape, proof.state.track_number).0;

    if tape.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    if tape_address != (*tape_info.key).into() || proof.state.tape != (*tape_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let size = proof.state.size;
    tape.delete_track(&proof)
        .map_err(|_| TapeError::BadProof)?;

    TrackDeleted {
        track: track_address,
        tape: tape_address,
        key: proof.state.key,
        size,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, CompressedTrackProof, TrackKind, TrackState};
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, MerkleTree};
    use tape_crypto::Hash;
    use tape_test::*;

    #[test]
    fn test_delete_track() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(authority.into());
        let track_number = TrackNumber(0);
        let size = StorageUnits::mb(250);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size,
            group: GroupIndex(7),
            value_hash: Hash::new_unique(),
        };
        let track_hash = track.get_hash();
        let mut track_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        track_tree.add_leaf_hash(track_hash).unwrap();
        let proof: [Hash; TRACK_TREE_HEIGHT] = create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(
                &[track_hash],
                track_number.0 as usize,
            )
            .expect("track proof is valid")
            .try_into()
            .expect("proof has correct length");
        let mut expected_tree = track_tree;
        expected_tree.remove_leaf_hash(track_number.0, &proof, track_hash).unwrap();

        let tape = Tape {
            authority: authority.into(),
            capacity: StorageUnits::mb(1000),
            used: size,
            active_epoch: EpochNumber(15),
            expiry_epoch: EpochNumber(100),
            tracks: TrackArchive {
                tree: track_tree,
                next_number: TrackNumber(1),
                num_tracks: 1,
            },
            ..Tape::zeroed()
        };

        let instruction = build_delete_track_ix(fee_payer.into(), authority.into(),
            CompressedTrackProof { state: track, proof },
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(tape_address)).data(
                    Tape {
                        used: StorageUnits(0),
                        tracks: TrackArchive {
                            tree: expected_tree,
                            next_number: TrackNumber(1),
                            num_tracks: 0,
                        },
                        ..tape
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }
}
