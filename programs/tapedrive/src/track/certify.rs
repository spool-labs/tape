use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::TrackCertified;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::bls12254::min_sig::*;
use crate::error::*;

pub fn process_certify_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CertifyTrack::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        system_info,
        epoch_info,
        tape_info,
        track_info,
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

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let tape = tape_info
        .as_account::<Tape>(&tapedrive::ID)?;

    let track = track_info
        .is_writable()?
        .as_account_mut::<Track>(&tapedrive::ID)?;

    let (tape_address, _) = tape_pda(tape.authority);
    let (track_address, _) = track_pda(tape.authority, track.key);

    if tape_address != *tape_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if track_address != *track_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if track.tape != tape_address {
        return Err(ProgramError::InvalidAccountData);
    }

    if !track.data.is_registered() {
        return Err(ProgramError::InvalidAccountData);
    }

    let cert_epoch = EpochNumber::unpack(args.epoch);
    let (committee, spools) = system
        .committee_at(cert_epoch, current_epoch(epoch))
        .ok_or(TapeError::BadEpochId)?;

    let group = track.data.spool_group();
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

    // Build certification message with domain separation and epoch binding
    // Must match the format used by storage nodes when signing
    let certify_message = CertifyMessage::new(
        cert_epoch,
        track_address.to_bytes(),
        track.data.commitment_hash.0,
    );
    let message = certify_message.to_bytes();

    verify_aggregate(
        &message,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    let signer_count = indices.len() as u64;

    track.data.set_certified(
        current_epoch(epoch),
    );

    TrackCertified {
        track: *track_info.key,
        epoch: current_epoch(epoch),
        signer_count: signer_count.to_le_bytes(),
        signer_weight: weight.to_le_bytes(),
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;
    use tape_spooler::dhondt_allocate;

    #[test]
    fn test_certify_track() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(authority);
        let (track_address, _) = track_pda(authority, bucket_hash);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        const SIGNERS: usize = 75;

        // Generate keypairs
        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..MEMBER_COUNT)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        // Build on-chain committee and spools (this may reorder members)
        let mut system = System::zeroed();
        system.committee = Committee::from_members(
            // Will be reordered to stake order
            &committee
                .iter()
                .enumerate()
                .map(|(i, (_, pk))| CommitteeMember {
                    id: NodeId::from(i as u64),
                    stake: TAPE(1_000 * (i * i) as u64), // non-linear stake distribution
                    key: *pk,
                    ..CommitteeMember::zeroed()
                })
                .collect::<Vec<_>>(),
        );

        // Allocate spools based on stake
        let stakes = system.committee.active_stakes();
        let seat_counts = dhondt_allocate(
            &stakes,
            SPOOL_COUNT as u16
        ).unwrap();
        system.spools = SpoolAssignment::try_from_counts(&seat_counts)
            .expect("spools from counts");

        // Accounts/state
        let tape = Tape {
            authority: authority,
            ..Tape::zeroed()
        };

        let commitment_hash = Hash::new_unique();
        let track = Track {
            tape: tape_address,
            key: bucket_hash,
            data: TrackData {
                commitment_hash,
                spool_group: 0, // test uses group 0 (spools 0-19)
                ..TrackData::zeroed()
            },
            ..Track::zeroed()
        };

        let epoch = Epoch {
            id: EpochNumber(42),
            nonce: Hash::default(),
            ..Epoch::zeroed()
        };

        // Build bitmap and aggregate signature using the on-chain committee order
        let committee_size = system.committee.size();
        assert!(SIGNERS <= committee_size);

        // Choose the first SIGNERS in the on-chain order. If the committee is sorted by stake,
        // this picks the highest-stake members, which helps pass the seat-weighted supermajority.
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, committee_size);

        // Build certification message with domain separation and epoch binding
        let certify_message = CertifyMessage::new(epoch.id, track_address.to_bytes(), commitment_hash.0);
        let message = certify_message.to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| {
                // Find the SK whose PK matches the on-chain member at index i
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

        // Instruction and accounts
        let instruction = build_certify_track_ix(
            fee_payer, authority, bucket_hash, epoch.id, bitmap, agg_sig);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(tape_address, tape.pack(), tapedrive::ID),
            pda(track_address, track.pack(), tapedrive::ID),
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
                                certified_epoch: EpochNumber(42),
                            },
                            ..track.data
                        },
                        ..track
                    }
                    .pack()
                    .as_ref(),
                )
                .build(),
            ],
        );
    }
}
