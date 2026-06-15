use tape_api::program::prelude::*;

use crate::track::helpers::delete_track;

pub fn process_remove_from_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RemoveFromBlacklist::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        blacklist_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let node = node_info.as_account::<Node>(&tapedrive::ID)?;
    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let node_address = (*node_info.key).into();
    let (blacklist_address, _) = blacklist_pda(node_address);

    let tape = blacklist_info
        .is_writable()?
        .has_address(&blacklist_address.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if !tape.is_blacklist_tape(node.id) {
        return Err(ProgramError::InvalidAccountData);
    }

    let proof = args.track;
    if proof.state.tape != blacklist_address || !proof.state.is_inline() {
        return Err(ProgramError::InvalidInstructionData);
    }

    delete_track(tape, blacklist_address, proof)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::size_of;
    use tape_core::system::BlacklistEntry;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, CompressedTrackProof, TrackKind, TrackState};
    use tape_crypto::hash::hash;
    use tape_crypto::merkle::{MerkleTree, create_proof_from_leaf_hashes};
    use tape_test::*;

    #[test]
    fn remove_from_blacklist() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let node_address = Pubkey::new_unique();
        let (blacklist_address, _) = blacklist_pda(node_address.into());

        let entry = BlacklistEntry::tape(Address::new_unique());
        let entry_size = size_of::<BlacklistEntry>() as u64;
        let track_number = TrackNumber(0);
        let track = CompressedTrack {
            tape: blacklist_address,
            key: entry.key(),
            track_number,
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(entry_size),
            group: GroupIndex(7),
            value_hash: hash(bytemuck::bytes_of(&entry)),
        };
        let track_hash = track.get_hash();
        let mut track_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        track_tree.add_leaf_hash(track_hash).unwrap();
        let proof: [Hash; TRACK_TREE_HEIGHT] =
            create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(
                &[track_hash],
                track_number.0 as usize,
            )
            .expect("track proof is valid")
            .try_into()
            .expect("proof has correct length");
        let mut expected_tree = track_tree;
        expected_tree
            .remove_leaf_hash(track_number.0, &proof, track_hash)
            .unwrap();

        let node = Node {
            id: NodeId(9),
            authority: authority.into(),
            ..Node::zeroed()
        };
        let mut tape = Tape::blacklist(node.id, EpochNumber(0));
        tape.used = StorageUnits::from_bytes(entry_size);
        tape.tracks = TrackArchive {
            num_tracks: 1,
            next_number: TrackNumber(1),
            tree: track_tree,
        };

        let instruction = build_remove_from_blacklist_ix(
            fee_payer.into(),
            authority.into(),
            node_address.into(),
            CompressedTrackProof { state: track, proof },
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(blacklist_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(blacklist_address))
                    .data(
                        Tape {
                            used: StorageUnits::zero(),
                            tracks: TrackArchive {
                                num_tracks: 0,
                                next_number: TrackNumber(1),
                                tree: expected_tree,
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
