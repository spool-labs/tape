use tape_api::event::VoteRecorded;
use tape_api::program::prelude::*;
use tape_core::cert::AssignmentVoteMessage;
use tape_crypto::bls12254::min_sig::*;

pub fn process_vote_assignment(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = parse_vote_assignment(data)?;
    let [
        fee_payer_info,
        system_info,
        voting_epoch_info,
        target_epoch_info,
        curr_group_info,
        vote_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let voting_epoch_id = system.current_epoch;
    let target_epoch_id = voting_epoch_id.saturating_add(EpochNumber(1));

    let voting_epoch = voting_epoch_info
        .is_epoch(voting_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if voting_epoch.state.phase != EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let target_epoch = target_epoch_info
        .is_writable()?
        .is_epoch(target_epoch_id)?
        .as_account_mut::<Epoch>(&tapedrive::ID)?;

    let group_id = SpoolGroup::unpack(args.group);
    if group_id.0 >= voting_epoch.total_groups {
        return Err(TapeError::BadMember.into());
    }

    let curr_group = curr_group_info
        .is_group(voting_epoch_id, group_id)?
        .as_account::<Group>(&tapedrive::ID)?;

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
        let spool = curr_group.spools.get(*spool_index).ok_or(TapeError::BadMember)?;
        pubkeys.push(spool.bls_pubkey.0);
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = AssignmentVoteMessage::new(target_epoch_id, target_epoch.nonce, args.hash)
        .to_bytes();

    verify_aggregate(&message, &pubkeys, &decompressed_sig)
        .map_err(|_| TapeError::BadSignature)?;

    let (vote_address, _) = assignment_vote_pda(voting_epoch_id, target_epoch_id, args.hash);
    vote_info
        .is_writable()?
        .has_address(&vote_address.into())?
        .is_type::<Vote>(&tapedrive::ID)?;

    let (vote, bitmap) = Vote::read_mut(vote_info, &tapedrive::ID)?;
    if vote.kind != VoteKind::Assignment as u64
        || vote.hash != args.hash
        || vote.voting_epoch != voting_epoch_id
        || vote.target_epoch != target_epoch_id
    {
        return Err(TapeError::UnexpectedState.into());
    }

    let bits = usize::try_from(voting_epoch.total_groups)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    if bitmap.len() < bytes_for_members(bits) {
        return Err(TapeError::UnexpectedState.into());
    }

    let group_index = usize::try_from(group_id.0)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let mut bitmap = BitmapMut::new(bitmap, bits);
    if bitmap.is_set(group_index) {
        return Err(TapeError::AlreadySigned.into());
    }

    bitmap.set(group_index);
    let signed_groups = bitmap.count_ones() as u64;
    let landed = signed_groups == voting_epoch.total_groups;

    if landed {
        if target_epoch.has_assignment_hash() && target_epoch.assignment_hash != args.hash {
            return Err(TapeError::UnexpectedState.into());
        }

        if !target_epoch.has_assignment_hash() {
            target_epoch.assignment_hash = args.hash;
        }
    }

    VoteRecorded {
        kind: VoteKind::Assignment as u64,
        vote: vote_address,
        voting_epoch: voting_epoch_id,
        target_epoch: target_epoch_id,
        hash: args.hash,
        group: group_id,
        signer_count: weight.to_le_bytes(),
        signed_groups: signed_groups.to_le_bytes(),
        total_groups: voting_epoch.total_groups.to_le_bytes(),
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::system::Spool;
    use tape_test::*;

    fn make_group(epoch: EpochNumber, group_id: SpoolGroup) -> (Vec<BlsPrivateKey>, Group) {
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

    #[test]
    fn vote_assignment() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let hash = Hash::new_unique();
        let nonce = Hash::new_unique();
        let group_id = SpoolGroup(0);
        let total_groups = 1;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (group_address, _) = group_pda(voting_epoch_id, group_id);
        let (vote_address, _) =
            assignment_vote_pda(voting_epoch_id, target_epoch_id, hash);

        let (sks, group) = make_group(voting_epoch_id, group_id);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };

        let voting_epoch = Epoch {
            id: voting_epoch_id,
            state: EpochState {
                phase: EpochPhase::Closing as u64,
                ..EpochState::zeroed()
            },
            total_groups,
            ..Epoch::zeroed()
        };

        let target_epoch = Epoch {
            id: target_epoch_id,
            nonce,
            ..Epoch::zeroed()
        };

        let vote = Vote {
            kind: VoteKind::Assignment as u64,
            hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        const SIGNERS: usize = 14;

        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolBitmap::from_indices(&signed_indices);
        let message =
            AssignmentVoteMessage::new(target_epoch_id, nonce, hash).to_bytes();
        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| sks[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_vote_assignment_ix(
            fee_payer.into(),
            voting_epoch_id,
            hash,
            group_id,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            pda(group_address, group.pack(), tapedrive::ID),
            pda(vote_address, vote.pack_with(&vec![0u8; bitmap_len]), tapedrive::ID),
        ];

        let expected_vote = vote.pack_with(&[1u8]);
        let expected_target_epoch = Epoch {
            assignment_hash: hash,
            ..target_epoch
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(target_epoch_address))
                    .data(expected_target_epoch.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(vote_address))
                    .data(expected_vote.as_ref())
                    .build(),
            ],
        );
    }
}
