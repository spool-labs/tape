use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TrackCertified;
use tape_core::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_crypto::bls12254::min_sig::*;
use crate::error::*;

pub fn process_certify_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CertifySnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        tape_info,
        track_info,
        snapshot_state_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let (system_address, _) = system_pda();

    let system = system_info
        .is_system()?
        .has_address(&system_address)?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(system_address);

    tape_info
        .has_address(&tape_address)?
        .as_account::<Tape>(&tapedrive::ID)?;

    let track = track_info
        .is_writable()?
        .as_account_mut::<Track>(&tapedrive::ID)?;

    // Verify track belongs to the snapshot tape
    if track.tape != tape_address {
        return Err(ProgramError::InvalidAccountData);
    }

    // Derive expected track PDA from the track's commitment and verify
    let epoch_number = EpochNumber::unpack(args.epoch);

    // Snapshot must be for a past epoch (epoch has already advanced)
    if epoch_number >= current_epoch(epoch) {
        return Err(ProgramError::InvalidArgument);
    }

    let commitment = track.data.commitment_hash;

    let (track_address, _) = snapshot_pda(epoch_number, commitment);
    if track_address != *track_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Already certified — idempotent success for race conditions
    if track.data.is_certified() {
        return Err(TapeError::AlreadyCertified.into());
    }

    if !track.data.is_registered() {
        return Err(ProgramError::InvalidAccountData);
    }

    let signing_epoch = EpochNumber::unpack(args.signing_epoch);
    let (committee, spools) = system
        .committee_at(signing_epoch, current_epoch(epoch))
        .ok_or(TapeError::BadEpochId)?;

    let group = track.data.spool_group();
    if (group.0 as usize) >= SPOOL_GROUP_COUNT {
        return Err(ProgramError::InvalidArgument);
    }

    let weight = spools.group_weight(group, &args.bitmap);

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

    // Build snapshot message with domain separation
    let snapshot_message = SnapshotMessage::new(
        epoch_number,
        commitment.0,
    );
    let message = snapshot_message.to_bytes();

    verify_aggregate(
        &message,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    let snapshot_state = snapshot_state_info
        .is_writable()?
        .is_snapshot_state()?
        .as_account_mut::<SnapshotState>(&tapedrive::ID)?;

    let signer_count = indices.len() as u64;

    track.data.set_certified(
        epoch_number,
    );

    // Track certification progress per epoch.
    // If this is a new epoch, reset the counter.
    if epoch_number != snapshot_state.certifying_epoch {
        snapshot_state.certifying_epoch = epoch_number;
        snapshot_state.certified_count = 0;
    }

    snapshot_state.certified_count += 1;

    // All chunks certified — mark epoch as fully snapshotted.
    if snapshot_state.certified_count == SPOOL_GROUP_COUNT as u64 {
        snapshot_state.latest_epoch = epoch_number;
    }

    TrackCertified {
        track: *track_info.key,
        epoch: epoch_number,
        signer_count: signer_count.to_le_bytes(),
        signer_weight: weight.to_le_bytes(),
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::Hash;
    use tape_test::*;
    use tape_spooler::dhondt_allocate;
    use tape_core::spooler::SpoolGroup;

    #[test]
    fn test_certify_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (tape_address, _) = tape_pda(system_address);

        let epoch_number = EpochNumber(42);
        let commitment_hash = Hash::new_unique();
        let spool_group = SpoolGroup(49); // Last chunk triggers latest_epoch update
        let (track_address, _) = snapshot_pda(epoch_number, commitment_hash);

        const SIGNERS: usize = 75;

        // Generate BLS keypairs for committee
        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..MEMBER_COUNT)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        // Build on-chain committee and spools
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
        let seat_counts = dhondt_allocate(
            &stakes,
            SPOOL_COUNT as u16,
        ).unwrap();
        system.spools = SpoolAssignment::try_from_counts(&seat_counts)
            .expect("spools from counts");

        let tape = Tape {
            authority: system_address,
            ..Tape::zeroed()
        };

        let track = Track {
            tape: tape_address,
            key: Hash::default(),
            data: TrackData {
                commitment_hash,
                spool_group,
                ..TrackData::zeroed()
            },
            ..Track::zeroed()
        };

        // Epoch account reflects the CURRENT epoch (already advanced past the snapshot epoch)
        let epoch = Epoch {
            id: EpochNumber(epoch_number.as_u64() + 1),
            nonce: Hash::default(),
            ..Epoch::zeroed()
        };

        // Build bitmap and aggregate BLS signature
        let committee_size = system.committee.size();
        assert!(SIGNERS <= committee_size);

        // Sign with highest-index members (they own the most spools, including group 49)
        let signed_indices: Vec<usize> = (MEMBER_COUNT - SIGNERS..MEMBER_COUNT).collect();
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, committee_size);

        let snapshot_message = SnapshotMessage::new(
            epoch_number,
            commitment_hash.0,
        );
        let message = snapshot_message.to_bytes();

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

        // signing_epoch = current on-chain epoch (epoch has advanced past snapshot epoch)
        let signing_epoch = epoch.id;
        let instruction = build_certify_snapshot_ix(
            fee_payer, epoch_number, signing_epoch, commitment_hash, bitmap, agg_sig,
        );

        let (snapshot_state_address, _) = snapshot_state_pda();
        let snapshot_state = SnapshotState::zeroed();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(track_address, track.pack(), tapedrive::ID),
            pda(snapshot_state_address, snapshot_state.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&track_address).data(
                    Track {
                        data: TrackData {
                            state: TrackState {
                                phase: TrackPhase::Certified.into(),
                                certified_epoch: epoch_number,
                            },
                            ..track.data
                        },
                        ..track
                    }
                    .pack()
                    .as_ref(),
                )
                .build(),
                Check::account(&snapshot_state_address).data(
                    SnapshotState {
                        certifying_epoch: epoch_number,
                        certified_count: 1,
                        ..snapshot_state
                    }
                    .pack()
                    .as_ref(),
                )
                .build(),
            ],
        );
    }
}
