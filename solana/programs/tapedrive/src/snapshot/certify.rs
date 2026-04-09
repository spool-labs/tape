use tape_api::program::prelude::*;
use tape_api::instruction::snapshot_blob_from_certification;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::snapshot::chunk::snapshot_chunk_key;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_crypto::bls12254::min_sig::{verify_aggregate, G1Point};

use crate::error::TapeError;

pub fn process_certify_snapshot_group(
    accounts: &[AccountInfo<'_>],
    data: &[u8],
) -> ProgramResult {
    let certification = CertifySnapshotGroup::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        snapshot_state_info,
        manifest_info,
        snapshot_tape_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info.is_signer()?.is_writable()?;

    let (system_address, _) = system_pda();
    let system = system_info
        .is_system()?
        .has_address(&system_address.into())?
        .as_account::<System>(&tapedrive::ID)?;
    let epoch = epoch_info.is_epoch()?.as_account::<Epoch>(&tapedrive::ID)?;
    let snapshot_state = snapshot_state_info
        .is_snapshot_state()?
        .as_account::<SnapshotState>(&tapedrive::ID)?;

    let snapshot_epoch = EpochNumber::unpack(certification.epoch);
    let current_epoch = current_epoch(epoch);
    let expected_epoch = required_snapshot_epoch(current_epoch)?;
    let expected_parent = snapshot_state
        .tail_epoch
        .checked_add(EpochNumber(1))
        .ok_or(ProgramError::ArithmeticOverflow)?;

    if snapshot_epoch != expected_epoch || snapshot_epoch != expected_parent {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
    let manifest = manifest_info
        .is_writable()?
        .has_address(&manifest_address.into())?
        .is_snapshot_manifest()?
        .as_account_mut::<SnapshotManifest>(&tapedrive::ID)?;

    if manifest.parent_epoch != snapshot_state.tail_epoch {
        return Err(TapeError::SnapshotParentMismatch.into());
    }

    let group = SpoolGroup::unpack(certification.group);
    let group_index = group.0 as usize;
    if group_index >= SPOOL_GROUP_COUNT {
        return Err(ProgramError::InvalidArgument);
    }
    if manifest.group_bitmap.is_set(group_index) {
        return Err(TapeError::SnapshotGroupSealed.into());
    }

    let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);
    let snapshot_tape = snapshot_tape_info
        .is_writable()?
        .has_address(&snapshot_tape_address.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if snapshot_tape.authority != system_address {
        return Err(ProgramError::InvalidAccountData);
    }

    let signing_epoch = EpochNumber::unpack(certification.signing_epoch);
    let (committee, spools) = system
        .committee_at(signing_epoch, current_epoch)
        .ok_or(TapeError::BadEpochId)?;

    let signer_weight = spools.group_weight(group, &certification.bitmap);
    if !is_supermajority(signer_weight, SPOOL_GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let committee_size = committee.size();
    let indices = certification.bitmap.indices(committee_size);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for member_index in &indices {
        let member = committee
            .member_at(*member_index)
            .ok_or(TapeError::BadMember)?;
        pubkeys.push(member.key.0);
    }

    let decompressed_sig =
        G1Point::try_from(&certification.signature.0).map_err(|_| TapeError::BadSignature)?;
    let blob = snapshot_blob_from_certification(certification)?;
    let blob_hash = blob.get_hash();
    let message = SnapshotMessage::new(
        snapshot_epoch,
        signing_epoch,
        group,
        blob_hash,
        manifest.parent_epoch,
    )
    .to_bytes();

    verify_aggregate(&message, &pubkeys, &decompressed_sig)
        .map_err(|_| TapeError::BadSignature)?;

    let track_number = snapshot_tape.tracks.next_number();
    let track = CompressedTrack {
        tape: snapshot_tape_address,
        key: snapshot_chunk_key(snapshot_epoch, group, manifest.parent_epoch),
        track_number,
        kind: TrackKind::Blob as u64,
        state: TrackState::Certified as u64,
        size: blob.size,
        spool_group: group,
        value_hash: blob_hash,
    };

    snapshot_tape.write_track(&track)?;

    manifest.group_bitmap.set(group_index);
    manifest.groups[group_index] = SnapshotChunkRecord {
        size: blob.size,
        value_hash: blob_hash,
        commitment: blob.commitment,
        track_number,
        profile: blob.profile,
        stripe_size: blob.stripe_size,
        stripe_count: blob.stripe_count,
    };

    let signer_count = indices.len() as u64;

    SnapshotCertified {
        epoch: snapshot_epoch,
        group,
        track: track_number,
        commitment: blob.commitment,
        signer_count: signer_count.to_le_bytes(),
        signer_weight: signer_weight.to_le_bytes(),
    }
    .log();

    Ok(())
}

fn required_snapshot_epoch(current_epoch: EpochNumber) -> Result<EpochNumber, ProgramError> {
    if current_epoch <= EpochNumber(1) {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    current_epoch
        .checked_sub(EpochNumber(1))
        .ok_or(TapeError::SnapshotEpochClosed.into())
}

#[cfg(test)]
mod tests {
    use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
    use tape_core::snapshot::chunk::snapshot_chunk_root;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::store::TrackStore;
    use tape_core::types::{CommitteeBitmap, SnapshotGroupBitmap};
    use tape_crypto::Hash;
    use tape_crypto::merkle::MerkleTree;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_spooler::dhondt_allocate;
    use tape_test::*;

    use super::*;

    #[test]
    fn test_certify_snapshot_group() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(42);
        let signing_epoch = EpochNumber(43);
        let group = SpoolGroup(0);
        const SIGNERS: usize = 75;

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..MEMBER_COUNT)
            .map(|_| {
                let private_key = BlsPrivateKey::from_random();
                let public_key = private_key.public_key().expect("public key");
                (private_key, public_key)
            })
            .collect();

        let mut system = System::zeroed();
        system.committee = Committee::from_members(
            &committee
                .iter()
                .enumerate()
                .map(|(member_index, (_, public_key))| CommitteeMember {
                    id: NodeId::from(member_index as u64),
                    stake: TAPE(1_000 * (member_index * member_index) as u64),
                    key: *public_key,
                    ..CommitteeMember::zeroed()
                })
                .collect::<Vec<_>>(),
        );

        let stakes = system.committee.active_stakes();
        let seat_counts = dhondt_allocate(&stakes, SPOOL_COUNT as u16).expect("seat counts");
        system.spools = SpoolAssignment::try_from_counts(&seat_counts).expect("spools");

        let epoch = Epoch {
            id: signing_epoch,
            ..Epoch::zeroed()
        };
        let snapshot_state = SnapshotState {
            tail_epoch: EpochNumber(41),
        };
        let manifest = SnapshotManifest {
            parent_epoch: EpochNumber(41),
            group_bitmap: SnapshotGroupBitmap::zeroed(),
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };
        let tape = Tape {
            id: TapeNumber(9),
            authority: system_address.into(),
            capacity: StorageUnits(u64::MAX),
            active_epoch: snapshot_epoch,
            expiry_epoch: EpochNumber(u64::MAX),
            tracks: TrackStore {
                tree: MerkleTree::new(),
                next_number: TrackNumber(0),
                live_count: 0,
            },
            ..Tape::zeroed()
        };

        let leaves = [Hash::from([0x11; 32]); SPOOL_GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);
        let chunk_size = StorageUnits::from_bytes(1_537);
        let root = snapshot_chunk_root(b"snapshot chunk");
        let blob = BlobInfo {
            size: chunk_size,
            root,
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, system.committee.size());
        let message = SnapshotMessage::new(
            snapshot_epoch,
            signing_epoch,
            group,
            blob.get_hash(),
            EpochNumber(41),
        )
        .to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|member_index| {
                let member_public_key = system
                    .committee
                    .member_at(*member_index)
                    .expect("member at index")
                    .key;
                let private_key = committee
                    .iter()
                    .find(|(_, public_key)| *public_key == member_public_key)
                    .expect("matching keypair")
                    .0
                    .clone();
                private_key.sign(&message).expect("signature")
            })
            .collect();
        let aggregate_signature = BlsSignature::aggregate(&partials).expect("aggregate");

        let instruction = build_certify_snapshot_group_ix(
            fee_payer.into(),
            snapshot_epoch,
            signing_epoch,
            group,
            &blob,
            bitmap,
            aggregate_signature,
        );

        let expected_track = CompressedTrack {
            tape: snapshot_tape_address,
            key: snapshot_chunk_key(snapshot_epoch, group, EpochNumber(41)),
            track_number: TrackNumber(0),
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: chunk_size,
            spool_group: group,
            value_hash: blob.get_hash(),
        };
        let mut expected_tree = MerkleTree::new();
        expected_tree
            .add_leaf_hash(expected_track.get_hash())
            .expect("append track");

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
            pda(manifest_address, manifest.pack(), tapedrive::ID),
            pda(snapshot_tape_address, tape.pack(), tapedrive::ID),
        ];

        let mut expected_group_bitmap = SnapshotGroupBitmap::zeroed();
        expected_group_bitmap.set(group.0 as usize);

        let mut expected_groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];
        expected_groups[group.0 as usize] = SnapshotChunkRecord {
            size: chunk_size,
            value_hash: blob.get_hash(),
            commitment,
            track_number: TrackNumber(0),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(manifest_address))
                    .data(
                        SnapshotManifest {
                            group_bitmap: expected_group_bitmap,
                            groups: expected_groups,
                            ..manifest
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
                Check::account(&Pubkey::from(snapshot_tape_address))
                    .data(
                        Tape {
                            used: chunk_size,
                            tracks: TrackStore {
                                tree: expected_tree,
                                next_number: TrackNumber(1),
                                live_count: 1,
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

    #[test]
    fn test_certify_snapshot_group_rejects_sealed_group() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(42);
        let signing_epoch = EpochNumber(43);
        let group = SpoolGroup(0);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_state_address, _) = snapshot_state_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
        let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

        let mut group_bitmap = SnapshotGroupBitmap::zeroed();
        group_bitmap.set(group.0 as usize);

        let blob = BlobInfo {
            size: StorageUnits::from_bytes(1_024),
            root: snapshot_chunk_root(b"sealed-group"),
            commitment: Hash::from([0x22; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves: [Hash::from([0x11; 32]); SPOOL_GROUP_SIZE],
        };

        let instruction = build_certify_snapshot_group_ix(
            fee_payer.into(),
            snapshot_epoch,
            signing_epoch,
            group,
            &blob,
            CommitteeBitmap::zeroed(),
            BlsSignature::zeroed(),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, System::zeroed().pack(), tapedrive::ID),
            pda(
                epoch_address,
                Epoch {
                    id: signing_epoch,
                    ..Epoch::zeroed()
                }
                .pack(),
                tapedrive::ID,
            ),
            pda(
                snapshot_state_address,
                SnapshotState {
                    tail_epoch: EpochNumber(41),
                }
                .pack(),
                tapedrive::ID,
            ),
            pda(
                manifest_address,
                SnapshotManifest {
                    parent_epoch: EpochNumber(41),
                    group_bitmap,
                    groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
                }
                .pack(),
                tapedrive::ID,
            ),
            pda(
                snapshot_tape_address,
                Tape {
                    authority: system_address.into(),
                    ..Tape::zeroed()
                }
                .pack(),
                tapedrive::ID,
            ),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::SnapshotGroupSealed.into())],
        );
    }
}
