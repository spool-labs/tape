use tape_solana::*;
use tape_api::program::prelude::*;
use tape_core::{
    erasure::SPOOL_GROUP_SIZE,
    snapshot::{chunk::snapshot_chunk_key, types::SnapshotState},
    track::data::TrackDataSlice,
};
use tape_crypto::bls12254::min_sig::*;

pub fn process_write_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = WriteSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        snapshot_info,
        snapshot_tape_info,
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

    let snapshot_epoch = prev_epoch(epoch);
    let snapshot_address = snapshot_pda(snapshot_epoch).0;
    let snapshot_tape = snapshot_tape_pda(snapshot_epoch).0;
    let snapshot_blob = BlobInfo::unpack(args.snapshot);
    let meta = TrackDataSlice::Blob(snapshot_blob)
        .meta()
        .ok_or(TapeError::InvalidCommitment)?;

    let snapshot = snapshot_info
        .has_address(&snapshot_address.into())?
        .as_account::<Snapshot>(&tapedrive::ID)?;

    if snapshot.state > SnapshotState::PartiallyCertified as u64 {
        return Err(TapeError::AlreadyCertified.into());
    }

    let spool_group = SpoolGroup::unpack(args.group);
    let chunk_index = ChunkNumber::unpack(args.chunk);

    // verify signature before mutating state

    let committee = &system.committee;
    let weight = args.bitmap.count_ones() as u64;

    if !is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let indices = args.bitmap.indices(SPOOL_GROUP_SIZE);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    let group_offset = spool_group.0 * SPOOL_GROUP_SIZE as u64;
    for member_index in &indices {
        let spool_index = member_index + group_offset as usize;
        let committee_idx = system.spools.0[spool_index] as usize;
        if let Some(member) = committee.member_at(committee_idx) {
            pubkeys.push(member.key.0);
        } else {
            return Err(TapeError::BadMember.into());
        }
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = SnapshotWriteMessage::new(
        snapshot_epoch,
        spool_group,
        chunk_index,
        meta.value_hash,
    );
    let message_bytes = message.to_bytes();

    verify_aggregate(
        &message_bytes,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    // assign track number and append to tree

    let tape = snapshot_tape_info
        .is_writable()?
        .has_address(&snapshot_tape.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    let key = snapshot_chunk_key(snapshot_epoch, spool_group, chunk_index);
    let track_number = tape.tracks.next_number();

    let track = CompressedTrack {
        tape: snapshot_tape,
        track_number,
        key,
        kind: meta.kind as u64,
        state: TrackState::Certified as u64,
        size: meta.size,
        spool_group,
        value_hash: meta.value_hash,
    };

    let track_address = track_pda(track.tape, track.track_number).0;
    let track_hash = track.get_hash();

    tape.write_track(&track)?;

    SnapshotWritten {
        epoch: snapshot_epoch,
        track: track_address,
        group: spool_group,
        track_number,
        track_hash,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::COMMITMENT_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{SpoolGroupBitmap, StripeCount};
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_crypto::Hash;
    use tape_test::*;

    const SIGNERS: usize = 14;

    fn make_blob() -> BlobInfo {
        let slices: Vec<Vec<u8>> = (0..SPOOL_GROUP_SIZE)
            .map(|i| vec![i as u8; 64])
            .collect();
        let leaves: [Hash; SPOOL_GROUP_SIZE] =
            core::array::from_fn(|i| hash_leaf(&slices[i]));
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

        BlobInfo {
            size: StorageUnits::from_bytes(64 * SPOOL_GROUP_SIZE as u64),
            commitment,
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(SPOOL_GROUP_SIZE as u64),
            leaves,
        }
    }

    // Stable, known committee: first SPOOL_GROUP_SIZE members at indices 0..19,
    // equal stakes + ascending NodeId => sort preserves insertion order,
    // so private_keys[i] signs for committee.member_at(i). Group 0's spool
    // assignment is an identity mapping so the spool→member indirection in
    // the verifier resolves bit i in the bitmap to member i.
    fn make_committee() -> (Vec<BlsPrivateKey>, System) {
        let keypairs: Vec<(BlsPrivateKey, BlsPubkey)> = (0..SPOOL_GROUP_SIZE)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        let members: Vec<CommitteeMember> = keypairs
            .iter()
            .enumerate()
            .map(|(i, (_, pk))| CommitteeMember {
                id: NodeId::from(i as u64),
                stake: TAPE(1),
                key: *pk,
                ..CommitteeMember::zeroed()
            })
            .collect();

        let mut system = System::zeroed();
        system.committee = Committee::from_members(&members);
        for i in 0..SPOOL_GROUP_SIZE {
            system.spools.0[i] = i as u8;
        }

        (keypairs.into_iter().map(|(sk, _)| sk).collect(), system)
    }

    #[test]
    fn test_write_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let current_epoch = EpochNumber(10);
        let snapshot_epoch = EpochNumber(9);
        let spool_group = SpoolGroup(0);
        let chunk_index = ChunkNumber(0);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_address, _) = snapshot_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let (private_keys, system) = make_committee();

        let epoch = Epoch {
            id: current_epoch,
            ..Epoch::zeroed()
        };

        let snapshot = Snapshot {
            epoch: snapshot_epoch,
            state: SnapshotState::Registered as u64,
            group_bitmap: GroupBitmap::zeroed(),
        };

        let snapshot_tape = Tape {
            id: TapeNumber(0),
            authority: SYSTEM_ADDRESS,
            capacity: StorageUnits(u64::MAX),
            used: StorageUnits::zero(),
            active_epoch: snapshot_epoch,
            expiry_epoch: EpochNumber(u64::MAX),
            ..Tape::zeroed()
        };

        let blob = make_blob();
        let key = snapshot_chunk_key(snapshot_epoch, spool_group, chunk_index);
        let value_hash = blob.get_hash();
        let expected_track = CompressedTrack {
            tape: snapshot_tape_address,
            track_number: TrackNumber(0),
            key,
            kind: TrackKind::Blob as u64,
            state: TrackState::Registered as u64,
            size: blob.size,
            spool_group,
            value_hash,
        };

        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolGroupBitmap::from_indices(&signed_indices, SPOOL_GROUP_SIZE);
        let message =
            SnapshotWriteMessage::new(snapshot_epoch, spool_group, chunk_index, value_hash).to_bytes();

        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| private_keys[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_write_snapshot_ix(
            fee_payer.into(),
            snapshot_epoch,
            spool_group,
            chunk_index,
            bitmap,
            agg_sig,
            &blob,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_address, snapshot.pack(), tapedrive::ID),
            pda(snapshot_tape_address, snapshot_tape.pack(), tapedrive::ID),
        ];

        let mut expected_tracks = TrackArchive::zeroed();
        expected_tracks.append(&expected_track).unwrap();

        let expected_tape = Tape {
            used: blob.size,
            tracks: expected_tracks,
            ..snapshot_tape
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(snapshot_address))
                    .data(snapshot.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(snapshot_tape_address))
                    .data(expected_tape.pack().as_ref())
                    .build(),
            ],
        );
    }
}
