use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::state::Group;
use tape_api::event::TrackCertified;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::TrackState;
use tape_crypto::bls12254::min_sig::*;


pub fn process_certify_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CertifyTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        system_info,
        group_info,
        tape_info,
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

    let curr = system.current_epoch;

    let proof = args.track;
    let track = proof.state;

    let (expected_group_address, _) = group_pda(curr, track.group);
    if expected_group_address != (*group_info.key).into() {
        return Err(TapeError::EpochChanged.into());
    }

    let group = group_info
        .is_group(curr, track.group)?
        .as_account::<Group>(&tapedrive::ID)?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    let (tape_address, _) = tape_pda(tape.authority);
    let track_address = track_pda(track.tape, track.track_number).0;

    if tape.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    if tape_address != (*tape_info.key).into() || track.tape != (*tape_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let old_track_hash = tape.tracks
        .verify(&proof)
        .map_err(|_| TapeError::BadProof)?;

    if !track.is_blob() {
        return Err(TapeError::UnexpectedState.into());
    }

    if track.is_invalidated() {
        return Err(TapeError::AlreadyInvalidated.into());
    }

    if track.is_certified() {
        return Err(TapeError::AlreadyCertified.into());
    }

    if !track.is_registered() {
        return Err(TapeError::UnexpectedState.into());
    }

    let weight = args.bitmap.count_ones() as u64;
    if !is_supermajority(weight, GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let indices = args.bitmap.indices();
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for spool_index in &indices {
        let spool = group.spools.get(*spool_index).ok_or(TapeError::BadMember)?;
        pubkeys.push(spool.bls_pubkey.0);
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = TrackWriteMessage::new(curr, old_track_hash);
    let message_bytes = message.to_bytes();

    verify_aggregate(
        &message_bytes,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    let mut updated_track = track;
    updated_track.state = TrackState::Certified as u64;
    tape.update_track(&proof, &updated_track)
        .map_err(|_| TapeError::BadProof)?;

    let signer_count = indices.len() as u64;

    TrackCertified {
        track: track_address,
        epoch: curr,
        signer_count: signer_count.to_le_bytes(),
        signer_weight: weight.to_le_bytes(),
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::system::Spool;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, CompressedTrackProof, TrackKind};
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, MerkleTree};
    use tape_crypto::Hash;
    use tape_test::*;

    /// Build a Group whose 20 spools are owned by 20 distinct BLS keys.
    fn make_group(epoch: EpochNumber, group_id: GroupIndex) -> (Vec<BlsPrivateKey>, Group) {
        let mut group = Group::zeroed();
        group.epoch = epoch;
        group.id = group_id;
        group.size = StorageUnits::mb(50);

        let mut sks = Vec::with_capacity(GROUP_SIZE);
        for i in 0..GROUP_SIZE {
            let sk = BlsPrivateKey::from_random();
            let pk = sk.public_key().expect("pubkey");
            let mut bytes = [0u8; 32];
            bytes[0] = (i as u8) + 1;
            let addr = Address::new(bytes);

            group.spools[i] = Spool {
                node: addr,
                bls_pubkey: pk,
            };
            sks.push(sk);
        }
        (sks, group)
    }

    // happy-path BLS-aggregate certification of a track
    #[test]
    fn certify() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();
        let curr = EpochNumber(42);

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let group_id = GroupIndex(0);
        let (group_address, _) = group_pda(curr, group_id);

        const SIGNERS: usize = 14;

        let (sks, group) = make_group(curr, group_id);

        let system = System {
            current_epoch: curr,
            committee_size: 128,
            ..System::zeroed()
        };

        let track_number = TrackNumber(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Registered as u64,
            size: StorageUnits::mb(250),
            group: group_id,
            value_hash: Hash::new_unique(),
        };
        let old_track_hash = track.get_hash();

        let mut track_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        track_tree.add_leaf_hash(old_track_hash).unwrap();
        let proof: [Hash; TRACK_TREE_HEIGHT] = create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(
                &[old_track_hash],
                track_number.0 as usize,
            )
            .expect("track proof is valid")
            .try_into()
            .expect("proof has correct length");

        let mut expected_tree = track_tree;
        let mut updated_track = track;
        updated_track.state = TrackState::Certified as u64;
        let new_track_hash = updated_track.get_hash();
        expected_tree
            .update_leaf_hash(track_number.0, &proof, old_track_hash, new_track_hash)
            .unwrap();

        let tape = Tape {
            authority: authority.into(),
            tracks: TrackArchive {
                tree: track_tree,
                next_number: TrackNumber(1),
                num_tracks: 1,
            },
            ..Tape::zeroed()
        };

        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);

        let track_message = TrackWriteMessage::new(curr, old_track_hash);
        let message = track_message.to_bytes();

        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_certify_track_ix(
            fee_payer.into(),
            authority.into(),
            CompressedTrackProof { state: track, proof },
            curr,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
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
                        tracks: TrackArchive {
                            tree: expected_tree,
                            next_number: TrackNumber(1),
                            num_tracks: 1,
                        },
                        ..tape
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    // group PDA from a different epoch than system.current_epoch is rejected
    #[test]
    fn wrong_epoch_group() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();
        let curr = EpochNumber(42);
        let stale = EpochNumber(41);

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let group_id = GroupIndex(0);
        let (stale_group_address, _) = group_pda(stale, group_id);

        let (sks, group) = make_group(stale, group_id);

        let system = System {
            current_epoch: curr,
            committee_size: 128,
            ..System::zeroed()
        };

        let track_number = TrackNumber(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Registered as u64,
            size: StorageUnits::mb(250),
            group: group_id,
            value_hash: Hash::new_unique(),
        };
        let old_track_hash = track.get_hash();
        let mut track_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        track_tree.add_leaf_hash(old_track_hash).unwrap();
        let proof: [Hash; TRACK_TREE_HEIGHT] = create_proof_from_leaf_hashes::<TRACK_TREE_HEIGHT>(
                &[old_track_hash],
                track_number.0 as usize,
            )
            .expect("track proof is valid")
            .try_into()
            .expect("proof has correct length");

        let tape = Tape {
            authority: authority.into(),
            tracks: TrackArchive {
                tree: track_tree,
                next_number: TrackNumber(1),
                num_tracks: 1,
            },
            ..Tape::zeroed()
        };

        let signed_indices: Vec<usize> = (0..14).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message = TrackWriteMessage::new(stale, old_track_hash).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_certify_track_ix(
            fee_payer.into(),
            authority.into(),
            CompressedTrackProof { state: track, proof },
            stale,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(stale_group_address, group.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::EpochChanged.into())],
        );
    }
}
