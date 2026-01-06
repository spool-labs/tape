use tape_solana::*;
use tape_api::prelude::*;
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

    let mut weight : u64 = 0;
    for &i in system.spools.iter() {
        if args.bitmap.is_set(i as usize) {
            weight += 1;
        }
    }

    if !is_supermajority(weight, SLICE_COUNT as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let committee_size = system.committee.size();
    let indices = args.bitmap.indices(committee_size);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for member_index in indices {
        if let Some(member) = system.committee.member_at(member_index) {
            pubkeys.push(member.key.0);
        } else {
            return Err(TapeError::BadMember.into());
        }
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = track_address.as_ref();
    verify_aggregate(
        message,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    track.data.set_certified(
        current_epoch(epoch),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

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
            SLICE_COUNT as u16
        );
        system.spools = SpoolAssignment::try_from_counts(&seat_counts)
            .expect("spools from counts");

        // Accounts/state
        let tape = Tape {
            authority: authority,
            ..Tape::zeroed()
        };

        let track = Track {
            tape: tape_address,
            key: bucket_hash,
            ..Track::zeroed()
        };

        let epoch = Epoch {
            id: EpochNumber(42),
            ..Epoch::zeroed()
        };

        // Build bitmap and aggregate signature using the on-chain committee order
        let committee_size = system.committee.size();
        assert!(SIGNERS <= committee_size);

        // Choose the first SIGNERS in the on-chain order. If the committee is sorted by stake,
        // this picks the highest-stake members, which helps pass the seat-weighted supermajority.
        let mut signed_indices: Vec<usize> = (0..SIGNERS).collect();
        signed_indices[0] = MEMBER_COUNT - 1; // non-trivial ordering
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, committee_size);

        // Aggregate signature for the same post-order members
        let message = track_address.as_ref();
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
                sk.sign(message).unwrap()
            })
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        // Instruction and accounts
        let instruction = build_certify_track_ix(
            fee_payer, authority, bucket_hash, bitmap, agg_sig);

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
