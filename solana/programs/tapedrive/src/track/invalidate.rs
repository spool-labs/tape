use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TrackInvalidated;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::track::types::TrackState;
use tape_crypto::bls12254::min_sig::*;

use crate::error::*;

pub fn process_invalidate_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = parse_invalidate_track(data)?;
    let [
        fee_payer_info,

        system_info,
        epoch_info,
        tape_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(tape.authority);
    let proof = args.track;
    let track = proof.state;
    let track_address = track_pda(track.tape, track.track_number).0;

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

    if !track.is_registered() && !track.is_certified() {
        return Err(TapeError::UnexpectedState.into());
    }

    let cert_epoch = EpochNumber::unpack(args.epoch);
    let (committee, spools) = system
        .committee_at(cert_epoch, current_epoch(epoch))
        .ok_or(TapeError::BadEpochId)?;

    let weight = spools.group_weight(track.spool_group, &args.bitmap);

    if !is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let committee_size = committee.size();
    let indices = args.bitmap.indices(committee_size);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for member_index in &indices {
        if let Some(member) = committee.member_at(*member_index) {
            pubkeys.push(member.key.0);
        } else {
            return Err(TapeError::BadMember.into());
        }
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let invalidate_message = InvalidateMessage::new(
        cert_epoch,
        old_track_hash.0,
        args.computed_root.0,
    );
    let message = invalidate_message.to_bytes();

    verify_aggregate(
        &message,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    let mut updated_track = track;
    updated_track.state = TrackState::Invalidated as u64;
    tape.update_track(&proof, &updated_track)
        .map_err(|_| TapeError::BadProof)?;

    TrackInvalidated {
        track: track_address,
        epoch: cert_epoch,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::store::TrackStore;
    use tape_core::track::types::{CompressedTrack, CompressedTrackProof, TrackKind};
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, MerkleTree};
    use tape_crypto::Hash;
    use tape_test::*;
    use tape_spooler::dhondt_allocate;

    #[test]
    fn test_invalidate_track() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        const SIGNERS: usize = 75;

        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..MEMBER_COUNT)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        let mut system = System::zeroed();
        system.committee = Committee::from_members(
            &committee
                .iter()
                .enumerate()
                .map(|(i, (_, pk))| CommitteeMember {
                    id: NodeId::from(i as u64),
                    stake: TAPE(1_000 * (i * i) as u64),
                    key: *pk,
                    ..CommitteeMember::zeroed()
                })
                .collect::<Vec<_>>(),
        );

        let stakes = system.committee.active_stakes();
        let seat_counts = dhondt_allocate(&stakes, SPOOL_COUNT as u16).unwrap();
        system.spools = SpoolAssignment::try_from_counts(&seat_counts)
            .expect("spools from counts");

        let track_number = TrackNumber(0);
        let spool_group = SpoolGroup(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::mb(250),
            spool_group,
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
        updated_track.state = TrackState::Invalidated as u64;
        let new_track_hash = updated_track.get_hash();
        expected_tree
            .update_leaf_hash(track_number.0, &proof, old_track_hash, new_track_hash)
            .unwrap();

        let tape = Tape {
            authority: authority.into(),
            tracks: TrackStore {
                tree: track_tree,
                next_number: TrackNumber(1),
                live_count: 1,
            },
            ..Tape::zeroed()
        };

        let computed_root = Hash::new_unique();
        let epoch = Epoch {
            id: EpochNumber(42),
            nonce: Hash::default(),
            ..Epoch::zeroed()
        };

        let committee_size = system.committee.size();
        assert!(SIGNERS <= committee_size);

        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, committee_size);

        let invalidate_message = InvalidateMessage::new(
            epoch.id,
            old_track_hash.0,
            computed_root.0,
        );
        let message = invalidate_message.to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| {
                let member_pk = system.committee
                    .member_at(i)
                    .expect("member at index").key;
                let sk = committee
                    .iter()
                    .find(|(_, pk)| *pk == member_pk)
                    .expect("matching sk for pk").0
                    .clone();
                sk.sign(&message).unwrap()
            })
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_invalidate_track_ix(fee_payer.into(),
            system_address,
            epoch_address,
            CompressedTrackProof { state: track, proof },
            epoch.id,
            bitmap,
            agg_sig,
            computed_root,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
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
                        tracks: TrackStore {
                            tree: expected_tree,
                            next_number: TrackNumber(1),
                            live_count: 1,
                        },
                        ..tape
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }

    #[test]
    fn test_invalidate_rejects_already_invalidated() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..MEMBER_COUNT)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        let mut system = System::zeroed();
        system.committee = Committee::from_members(
            &committee
                .iter()
                .enumerate()
                .map(|(i, (_, pk))| CommitteeMember {
                    id: NodeId::from(i as u64),
                    stake: TAPE(1_000 * (i * i) as u64),
                    key: *pk,
                    ..CommitteeMember::zeroed()
                })
                .collect::<Vec<_>>(),
        );

        let stakes = system.committee.active_stakes();
        let seat_counts = dhondt_allocate(&stakes, SPOOL_COUNT as u16).unwrap();
        system.spools = SpoolAssignment::try_from_counts(&seat_counts)
            .expect("spools from counts");

        let track_number = TrackNumber(0);
        let spool_group = SpoolGroup(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Invalidated as u64,
            size: StorageUnits::mb(250),
            spool_group,
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
            tracks: TrackStore {
                tree: track_tree,
                next_number: TrackNumber(1),
                live_count: 1,
            },
            ..Tape::zeroed()
        };

        let epoch = Epoch {
            id: EpochNumber(42),
            nonce: Hash::default(),
            ..Epoch::zeroed()
        };

        let signed_indices: Vec<usize> = (0..75).collect();
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, system.committee.size());
        let computed_root = Hash::new_unique();

        let invalidate_message = InvalidateMessage::new(
            epoch.id,
            old_track_hash.0,
            computed_root.0,
        );
        let message = invalidate_message.to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| {
                let member_pk = system.committee.member_at(i).unwrap().key;
                let sk = committee.iter().find(|(_, pk)| *pk == member_pk).unwrap().0.clone();
                sk.sign(&message).unwrap()
            })
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_invalidate_track_ix(fee_payer.into(),
            system_address,
            epoch_address,
            CompressedTrackProof { state: track, proof },
            epoch.id,
            bitmap,
            agg_sig,
            computed_root,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::AlreadyInvalidated.into())],
        );
    }
}
