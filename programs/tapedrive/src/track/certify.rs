use tape_api::prelude::*;
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

    // Verify the BLS signature over the track address using the committee members indicated in the
    // bitmap

    let committee = &system.committee;
    let member_count = committee.size();

    if member_count == 0 {
        return Err(ProgramError::InvalidAccountData);
    }

    // Must have exactly 16 bytes for 128-bit bitmap
    if args.bitmap.len() != 16 {
        return Err(ProgramError::InvalidInstructionData);
    }

    // Build signer pubkey list from bitmap
    let bitmap = &args.bitmap;
    let mut signer_pubkeys = Vec::with_capacity(member_count);

    for i in 0..committee.size() {
        if i >= 128 { break; }
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        if (bitmap[byte_idx] >> bit_idx) & 1 == 1 {
            if let Some(member) = committee.iter().nth(i) {
                signer_pubkeys.push(member.key);
            } else {
                return Err(ProgramError::InvalidAccountData);
            }
        }
    }

    if signer_pubkeys.is_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let message = track_address.as_ref();
    args.signature
        .verify_aggregate(message, &signer_pubkeys)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;
    use tape_crypto::bls12254::min_sig::aggregate::*;

    #[test]
    fn test_certify_track() {
        let signer = Pubkey::new_unique();
        let bucket_hash = Hash::new_unique();

        let (tape_address, _) = tape_pda(signer);
        let (track_address, _) = track_pda(signer, bucket_hash);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        const COMMITTEE_SIZE: usize = 128;
        const SIGNERS: usize = 32;

        let committee: Vec<(BlsPrivateKey, BlsPubkey)> = (0..COMMITTEE_SIZE)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        // Create bitmap for signers
        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap: [u8; 16] = {
            let vec = indices_to_bitmap(&signed_indices, COMMITTEE_SIZE);
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&vec);
            arr
        };

        // Create aggregate signature
        let message = track_address.as_ref();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| committee[i].0.sign(message).unwrap())
            .collect();

        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        // On-chain committee: full 128 members
        let mut system = System::zeroed();
        system.committee = Committee::from_members(
            &committee
                .iter()
                .enumerate()
                .map(|(i, (_, pk))| CommitteeMember {
                    id: NodeId::from(i as u64),
                    stake: TAPE(1_000),
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

        let epoch = Epoch::zeroed();

        let instruction = build_certify_track_ix(
            signer, bucket_hash, bitmap, agg_sig);

        // Execute
        let env = test_env();
        env.process_instruction(
            &instruction,
            &[
                sol(signer, 1_000_000_000),
                pda(system_address, system.pack(), tapedrive::ID),
                pda(epoch_address, epoch.pack(), tapedrive::ID),
                pda(tape_address, tape.pack(), tapedrive::ID),
                pda(track_address, track.pack(), tapedrive::ID),
            ],
            &[Check::success()],
        );

        //// Negative: wrong message fails
        //let wrong_sig = {
        //    let wrong_msg = b"wrong message";
        //    let wrong_partials: Vec<_> = signed_indices
        //        .iter()
        //        .map(|&i| committee[i].0.sign(wrong_msg).expect("sign"))
        //        .collect();
        //    BlsSignature::aggregate(&wrong_partials).unwrap()
        //};
        //
        //let bad_ix = build_certify_track_ix(signer, bitmap, wrong_sig);
        //env.process_instruction(
        //    &bad_ix,
        //    &[
        //        sol(signer, 1_000_000_000),
        //        pda(system_address, system.pack(), tapedrive::ID),
        //        pda(epoch_address, epoch.pack(), tapedrive::ID),
        //        pda(tape_address, tape.pack(), tapedrive::ID),
        //        pda(track_address, track.pack(), tapedrive::ID),
        //    ],
        //    &[Check::failure(ProgramError::InvalidInstructionData)], // or BLSVerificationError
        //);
    }
}
