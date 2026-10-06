use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::state::Group;
use tape_api::event::TrackInvalidated;
use tape_crypto::bls12254::min_sig::*;

use crate::tape::helpers::verified_tape_address;

pub fn process_invalidate_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = InvalidateTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        group_info,
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

    let curr = system.current_epoch;

    let proof = args.track;
    let track = proof.state;

    // The members of the live group that carries the track sign for it.
    let live_group = system.live_group(track.group)?;

    let (expected_group_address, _) = group_pda(curr, live_group);
    if expected_group_address != (*group_info.key).into() {
        return Err(TapeError::EpochChanged.into());
    }

    let group = group_info
        .is_group(curr, live_group)?
        .as_account::<Group>(&tapedrive::ID)?;

    let tape = tape_info
        .is_writable()?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.is_system() {
        return Err(TapeError::UnexpectedState.into());
    }

    verified_tape_address(tape_info, tape)?;
    let track_address = track.address();

    if track.tape != (*tape_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let old_track_hash = tape.tracks
        .verify(&proof)
        .map_err(|_| TapeError::BadProof)?;

    if track.is_invalidated() {
        return Err(TapeError::AlreadyInvalidated.into());
    }

    if !track.is_registered() && !track.is_certified() {
        return Err(TapeError::UnexpectedState.into());
    }

    let indices = system.signers(&args.bitmap)?;
    let weight = indices.len() as u64;
    if weight < system.thresholds()?.invalidate {
        return Err(TapeError::NoQuorum.into());
    }

    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for spool_index in &indices {
        let spool = group.spools.get(*spool_index)
            .ok_or(TapeError::BadMember)?;
        pubkeys.push(spool.bls_pubkey.0);
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = TrackInvalidateMessage::new(
        curr,
        old_track_hash,
    );
    let message_bytes = message.to_bytes();

    verify_aggregate(
        message_bytes,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    tape.invalidate_track(&proof)
        .map_err(|_| TapeError::BadProof)?;

    TrackInvalidated {
        track: track_address,
        epoch: curr,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::RSParams;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{
        CompressedTrack,
        CompressedTrackProof,
        TrackFlags,
        TrackKind,
        TrackState,
    };
    use tape_crypto::merkle::{create_proof_from_leaf_hashes, MerkleTree};
    use tape_crypto::Hash;
    use tape_test::*;


    const NARROW: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

    // a seeded track and the group that signs for it
    struct Case {
        base: System,
        kind: TrackKind,
        state: TrackState,
        size: StorageUnits,
        virtual_group: VirtualGroup,
    }

    impl Case {
        fn permissionless() -> Self {
            Self {
                base: one_group(permissionless_system()),
                kind: TrackKind::Coded,
                state: TrackState::Certified,
                size: StorageUnits::mb(250),
                virtual_group: VirtualGroup(0),
            }
        }

        fn narrow() -> Self {
            Self { base: one_group(trusted_system(NARROW)), ..Self::permissionless() }
        }
    }

    // a network with one live group
    fn one_group(base: System) -> System {
        System {
            live_group_count: 1,
            ..base
        }
    }

    fn signers(count: usize) -> Vec<usize> {
        (0..count).collect()
    }

    // quorum invalidation of one seeded track, expecting its capacity back
    fn invalidate_returns_capacity(kind: TrackKind, state: TrackState, size: StorageUnits) {
        invalidate_case(Case { kind, state, size, ..Case::permissionless() }, &signers(14), None);
    }

    // invalidation of one seeded track by the given signer bits
    fn invalidate_case(case: Case, signed: &[usize], error: Option<ProgramError>) {
        let Case { base, kind, state, size, virtual_group } = case;
        let profile = base.code().unwrap();

        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();
        let curr = EpochNumber(42);

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let group_id = base.live_group(virtual_group).unwrap();
        let (group_address, _) = group_pda(curr, group_id);

        let (sks, group) = make_group_of(curr, group_id, profile);

        let system = System {
            current_epoch: curr,
            committee_size: 128,
            ..base
        };

        let track_number = TrackNumber(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: kind as u64,
            state: state as u64,
            flags: TrackFlags::NONE,
            size,
            group: virtual_group,
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
            capacity: StorageUnits::mb(1000),
            used: track.size,
            tracks: TrackArchive {
                tree: track_tree,
                next_number: TrackNumber(1),
                num_tracks: 1,
            },
            ..Tape::zeroed()
        };

        let bitmap = SpoolBitmap::from_indices(signed);

        let invalidate_message = TrackInvalidateMessage::new(
            curr,
            old_track_hash,
        );
        let message = invalidate_message.to_bytes();

        // A bit past the profile's n has no key behind it, so only the live ones sign.
        let partials: Vec<BlsSignature> = signed
            .iter()
            .filter(|&&i| i < sks.len())
            .map(|&i| sks[i].sign(message).unwrap())
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_invalidate_track_ix(
            fee_payer.into(),
            CompressedTrackProof { state: track, proof },
            curr,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

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
                        used: StorageUnits(0),
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

    // happy-path BLS-aggregate invalidation of a certified track
    #[test]
    fn invalidate() {
        invalidate_returns_capacity(
            TrackKind::Coded,
            TrackState::Certified,
            StorageUnits::mb(250),
        );
    }

    // inline registered tracks invalidate and return their capacity too
    #[test]
    fn invalidate_inline() {
        invalidate_returns_capacity(
            TrackKind::Inline,
            TrackState::Registered,
            StorageUnits::mb(1),
        );
    }

    // a track stamped past the live count is invalidated by the live group that carries it
    #[test]
    fn folded_group() {
        let base = System {
            live_group_count: 5,
            ..permissionless_system()
        };

        invalidate_case(
            Case { base, virtual_group: VirtualGroup(7), ..Case::permissionless() },
            &signers(14),
            None,
        );
    }

    // tapenet refuses thirteen signers, one short of its fourteen
    #[test]
    fn below_quorum() {
        invalidate_case(Case::permissionless(), &signers(13), Some(TapeError::NoQuorum.into()));
    }

    // a 3-of-4 cluster invalidates on three signers and refuses two
    #[test]
    fn trusted_quorum() {
        invalidate_case(Case::narrow(), &signers(3), None);
        invalidate_case(Case::narrow(), &signers(2), Some(TapeError::NoQuorum.into()));
    }

    // a signer bit past the network's width adds no weight
    #[test]
    fn padding_signer() {
        invalidate_case(Case::narrow(), &[0, 1, 4], Some(TapeError::BadMember.into()));
    }

    // already-invalidated tracks reject re-invalidation
    #[test]
    fn already_invalidated() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();
        let curr = EpochNumber(42);

        let (tape_address, _) = tape_pda(authority.into());
        let (system_address, _) = system_pda();
        let group_id = GroupIndex(0);
        let (group_address, _) = group_pda(curr, group_id);

        let (sks, group) = make_group(curr, group_id);

        let system = System {
            current_epoch: curr,
            committee_size: 128,
            ..one_group(permissionless_system())
        };

        let track_number = TrackNumber(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Invalidated as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::mb(250),
            group: VirtualGroup::from(group_id),
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

        let invalidate_message = TrackInvalidateMessage::new(
            curr,
            old_track_hash,
        );
        let message = invalidate_message.to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(message).unwrap())
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_invalidate_track_ix(
            fee_payer.into(),
            CompressedTrackProof { state: track, proof },
            curr,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::AlreadyInvalidated.into())],
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
            ..one_group(permissionless_system())
        };

        let track_number = TrackNumber(0);
        let track = CompressedTrack {
            tape: tape_address,
            key: bucket_hash,
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::mb(250),
            group: VirtualGroup::from(group_id),
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
        let message = TrackInvalidateMessage::new(stale, old_track_hash).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_invalidate_track_ix(
            fee_payer.into(),
            CompressedTrackProof { state: track, proof },
            stale,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
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
