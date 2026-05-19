use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_add_to_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = AddToBlacklist::try_from_bytes(data)?;
    let proof = args.0;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    let tape = tape_info
        .is_type::<Tape>(&tapedrive::ID)?
        .as_account::<Tape>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    if proof.state.tape != (*tape_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    tape.tracks
        .verify(&proof)
        .map_err(|_| TapeError::BadProof)?;

    node.blacklist
        .add(proof.state.key, proof.state.size)
        .map_err(|_| TapeError::ListFull)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::Hash;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, CompressedTrackProof, TrackKind, TrackState};
    use tape_test::*;

    #[test]
    fn test_add_to_blacklist() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        // PDAs
        let blob_hash = Hash::new_unique();
        let (node_address, _) = node_pda(authority.into());
        let (tape_address, _) = tape_pda(authority.into());
        let track_number = TrackNumber(0);
        let size = StorageUnits::mb(500);
        let track = CompressedTrack {
            tape: tape_address,
            key: blob_hash,
            track_number,
            kind: TrackKind::Raw as u64,
            state: TrackState::Certified as u64,
            size,
            group: GroupIndex(3),
            value_hash: Hash::new_unique(),
        };
        let track_hash = track.get_hash();
        let mut track_tree = tape_crypto::merkle::MerkleTree::<TRACK_TREE_HEIGHT>::new();
        track_tree.add_leaf_hash(track_hash).unwrap();
        let proof: [Hash; TRACK_TREE_HEIGHT] =
            tape_crypto::merkle::create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(
                &[track_hash],
                track_number.0 as usize,
            )
            .expect("track proof is valid")
            .try_into()
            .expect("proof has correct length");

        // Instruction
        let instruction = build_add_to_blacklist_ix(fee_payer.into(), authority.into(),
            node_address,
            CompressedTrackProof { state: track, proof },
        );

        // Prepare node with initialized blacklist
        let mut node = Node::zeroed();
        node.authority = authority.into();
        node.blacklist = Blacklist::new();
        let tape = Tape {
            tracks: TrackArchive {
                tree: track_tree,
                next_number: TrackNumber(1),
                num_tracks: 1,
            },
            ..Tape::zeroed()
        };

        // Build accounts
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        // Expected node after blacklisting
        let mut expected_node = node.clone();
        expected_node
            .blacklist
            .add(blob_hash, size)
            .expect("blacklist add");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                // Verify node updated with blacklist containing the track
                Check::account(&Pubkey::from(node_address))
                    .data(expected_node.pack().as_ref())
                    .build(),
            ],
        );
    }
}
