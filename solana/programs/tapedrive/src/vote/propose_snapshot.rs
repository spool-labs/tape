use core::mem::size_of;

use tape_api::event::VoteProposed;
use tape_api::program::prelude::*;

pub fn process_propose_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = parse_propose_snapshot(data)?;
    let [
        fee_payer_info,
        system_info,
        voting_epoch_info,
        target_epoch_info,
        vote_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;

    if args.hash == Hash::zeroed() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let voting_epoch_id = system.current_epoch;
    let target_epoch_id = voting_epoch_id.saturating_sub(EpochNumber(1));

    let voting_epoch = voting_epoch_info
        .is_epoch(voting_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if voting_epoch.state.phase != EpochPhase::Snapshot as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let target_epoch = target_epoch_info
        .is_epoch(target_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    if target_epoch.has_snapshot_hash() && target_epoch.snapshot_hash != args.hash {
        return Err(TapeError::UnexpectedState.into());
    }

    let (vote_address, bump) = snapshot_vote_pda(voting_epoch_id, target_epoch_id, args.hash);
    vote_info
        .is_empty()?
        .is_writable()?
        .has_address(&vote_address.into())?;

    let total_groups = usize::try_from(voting_epoch.total_groups)
        .map_err(|_| ProgramError::InvalidInstructionData)?;
    if total_groups == 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    let bitmap_len = bytes_for_members(total_groups);
    let size = 8 + size_of::<Vote>() + bitmap_len;

    create_account_with_size::<Vote>(
        vote_info,
        system_program_info,
        fee_payer_info,
        size,
        &tapedrive::ID,
        &[
            VOTE,
            VOTE_SNAPSHOT,
            &voting_epoch_id.pack(),
            &target_epoch_id.pack(),
            args.hash.as_ref(),
        ],
        bump,
    )?;

    let vote = Vote::header_mut(vote_info, &tapedrive::ID)?;
    vote.kind = VoteKind::Snapshot as u64;
    vote.hash = args.hash;
    vote.voting_epoch = voting_epoch_id;
    vote.target_epoch = target_epoch_id;
    vote.registered_by = *fee_payer_info.key;
    vote.bitmap = Tail::new(bitmap_len as u64, bitmap_len as u64);

    let (_, bitmap) = Vote::read_mut(vote_info, &tapedrive::ID)?;
    bitmap.fill(0);

    VoteProposed {
        kind: VoteKind::Snapshot as u64,
        vote: vote_address,
        voting_epoch: voting_epoch_id,
        target_epoch: target_epoch_id,
        hash: args.hash,
        total_groups: voting_epoch.total_groups.to_le_bytes(),
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn propose_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(11);
        let hash = Hash::new_unique();
        let total_groups = 9;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (vote_address, _) =
            snapshot_vote_pda(voting_epoch_id, target_epoch_id, hash);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };

        let voting_epoch = Epoch {
            id: voting_epoch_id,
            state: EpochState {
                phase: EpochPhase::Snapshot as u64,
                ..EpochState::zeroed()
            },
            total_groups,
            ..Epoch::zeroed()
        };

        let target_epoch = Epoch {
            id: target_epoch_id,
            ..Epoch::zeroed()
        };

        let instruction =
            build_propose_snapshot_ix(fee_payer.into(), voting_epoch_id, hash);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            empty(vote_address),
            system_program(),
        ];

        let vote = Vote {
            kind: VoteKind::Snapshot as u64,
            hash,
            voting_epoch: voting_epoch_id,
            target_epoch: target_epoch_id,
            registered_by: fee_payer,
            bitmap: Tail::new(bitmap_len as u64, bitmap_len as u64),
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(vote_address))
                    .owner(&tapedrive::ID)
                    .data(vote.pack_with(&vec![0u8; bitmap_len]).as_ref())
                    .build(),
            ],
        );
    }
}
