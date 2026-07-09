use core::mem::size_of;

use tape_api::event::VoteProposed;
use tape_api::program::prelude::*;
use tape_crypto::{Address, Hash};

pub fn process_propose_eviction(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ProposeEviction::try_from_bytes(data)?;
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

    if args.node == Address::zeroed() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let voting_epoch_id = system.current_epoch;
    let target_epoch_id = voting_epoch_id.next();

    let voting_epoch = voting_epoch_info
        .is_epoch(voting_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    // Same window as join: the next committee must still be mutable when the
    // vote lands, so voting closes at Closing (unlike the assignment vote).
    if voting_epoch.state.phase >= EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    target_epoch_info
        .is_epoch(target_epoch_id)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node_hash = Hash(args.node.to_bytes());
    let (vote_address, bump) = eviction_vote_pda(voting_epoch_id, target_epoch_id, args.node);
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
            VOTE_EVICTION,
            &voting_epoch_id.pack(),
            &target_epoch_id.pack(),
            args.node.as_ref(),
        ],
        bump,
    )?;

    let vote = Vote::header_mut(vote_info, &tapedrive::ID)?;
    vote.kind = VoteKind::Eviction as u64;
    vote.hash = node_hash;
    vote.voting_epoch = voting_epoch_id;
    vote.target_epoch = target_epoch_id;
    vote.registered_by = *fee_payer_info.key;
    vote.bitmap = Tail::new(bitmap_len as u64, bitmap_len as u64);

    let (_, bitmap) = Vote::read_mut(vote_info, &tapedrive::ID)?;
    bitmap.fill(0);

    VoteProposed {
        kind: VoteKind::Eviction as u64,
        vote: vote_address,
        voting_epoch: voting_epoch_id,
        target_epoch: target_epoch_id,
        hash: node_hash,
        total_groups: voting_epoch.total_groups,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn propose_eviction() {
        let fee_payer = Pubkey::new_unique();
        let voting_epoch_id = EpochNumber(12);
        let target_epoch_id = EpochNumber(13);
        let node: Address = Pubkey::new_unique().into();
        let node_hash = Hash(node.to_bytes());
        let total_groups = 9;
        let bitmap_len = bytes_for_members(total_groups as usize);

        let (system_address, _) = system_pda();
        let (voting_epoch_address, _) = epoch_pda(voting_epoch_id);
        let (target_epoch_address, _) = epoch_pda(target_epoch_id);
        let (vote_address, _) = eviction_vote_pda(voting_epoch_id, target_epoch_id, node);

        let system = System {
            current_epoch: voting_epoch_id,
            ..System::zeroed()
        };

        let voting_epoch = Epoch {
            id: voting_epoch_id,
            total_groups,
            ..Epoch::zeroed()
        };

        let target_epoch = Epoch {
            id: target_epoch_id,
            ..Epoch::zeroed()
        };

        let instruction = build_propose_eviction_ix(fee_payer.into(), voting_epoch_id, node);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(voting_epoch_address, voting_epoch.pack(), tapedrive::ID),
            pda(target_epoch_address, target_epoch.pack(), tapedrive::ID),
            empty(vote_address),
            system_program(),
        ];

        let vote = Vote {
            kind: VoteKind::Eviction as u64,
            hash: node_hash,
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
