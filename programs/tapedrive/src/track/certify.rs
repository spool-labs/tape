use tape_api::prelude::*;
use tape_crypto::bls12254::min_sig::*;
use steel::*;

pub fn process_certify_track(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CertifyTrack::try_from_bytes(data)?;
    let [
        signer_info,

        system_info,
        epoch_info,
        tape_info,
        track_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
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

    let committee_size = system.committee.size();
    if args.bitmap.count_ones() >= min_correct(committee_size as u64) as usize {
        return Err(ProgramError::InvalidAccountData);
    }

    let indices = args.bitmap.indices(committee_size);
    if indices.is_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for idx in indices {
        if idx >= committee_size {
            return Err(ProgramError::InvalidInstructionData);
        }
        let pk = system.committee.members[idx].key.0;
        pubkeys.push(pk);
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let message = track_address.as_ref();
    verify_aggregate(
        message,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| ProgramError::InvalidInstructionData)?;

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
        let signer = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(signer);
        let (track_address, _) = track_pda(signer, bucket_hash);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        const COMMITTEE_SIZE: usize = 128;
        const SIGNERS: usize = 85;

        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..COMMITTEE_SIZE)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        // Create bitmap for signers
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = CommitteeBitmap::from_indices(&signed_indices, COMMITTEE_SIZE);

        // Create aggregate signature
        let message = track_address.as_ref();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| committee[i].0.sign(message).unwrap())
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        // On-chain committee
        let mut system = System::zeroed();
        system.committee = Committee::from_members(
            &committee
                .iter()
                .enumerate()
                .map(|(i, (_, pk))| CommitteeMember {
                    id: NodeId::from(i as u64),
                    stake: TAPE(1_000 * i),
                    key: *pk,
                    blacklist: StorageUnits(0),
                })
                .collect::<Vec<_>>(),
        );

        let tape = Tape {
            authority: signer, 
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

        let instruction = build_certify_track_ix(
            signer, bucket_hash, bitmap, agg_sig);

        let accounts = vec![
            sol(signer, 1_000_000_000),

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
                        data: BlobData {
                            state: BlobState {
                                phase: BlobPhase::Certified.into(),
                                certified_epoch: EpochNumber(42),
                            },
                            ..track.data
                        },
                        ..track
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }
}
