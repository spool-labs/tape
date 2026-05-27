use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;
use tape_api::dynamic::DynamicState;
use tape_api::event::CommitteeResized;
use tape_api::program::prelude::*;
use tape_api::state::Committee;

pub fn process_resize_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ResizeCommittee::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        curr_epoch_info,
        next_epoch_info,
        target_epoch_info,
        committee_info,
        system_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let next = curr
        .checked_next()
        .ok_or(ProgramError::ArithmeticOverflow)?;
    let target = next
        .checked_next()
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let curr_epoch = curr_epoch_info
        .is_epoch(curr)?
        .as_account::<Epoch>(&tapedrive::ID)?;
    if curr_epoch.id != curr {
        return Err(TapeError::BadEpochId.into());
    }
    if curr_epoch.state.phase != EpochPhase::Closing as u64 {
        return Err(TapeError::BadEpochState.into());
    }

    let next_epoch = next_epoch_info
        .is_epoch(next)?
        .as_account::<Epoch>(&tapedrive::ID)?;
    if next_epoch.id != next {
        return Err(TapeError::BadEpochId.into());
    }

    let target_epoch = target_epoch_info
        .is_epoch(target)?
        .as_account::<Epoch>(&tapedrive::ID)?;
    if target_epoch.id != target {
        return Err(TapeError::BadEpochId.into());
    }
    if target_epoch.state.phase != EpochPhase::Unknown as u64
        || target_epoch.assignment_hash != Hash::zeroed()
        || target_epoch.total_groups != 0
        || !target_epoch.total_assigned.is_zero()
    {
        return Err(TapeError::UnexpectedState.into());
    }

    let target_capacity = next_epoch.preferences.committee_size;

    if target_capacity < MIN_COMMITTEE_SIZE as u64 {
        return Err(TapeError::InsufficientCommittee.into());
    }

    committee_info
        .is_writable()?
        .is_committee(target)?;

    let target_size = Committee::size_for_capacity(target_capacity);
    let current_size = committee_info.data_len();

    let current = {
        let header = Committee::header(committee_info, &tapedrive::ID)?;
        header.members
    };

    if current.count != 0 {
        return Err(TapeError::UnexpectedState.into());
    }

    if current.would_orphan(target_capacity) {
        return Err(TapeError::ResizeWouldOrphan.into());
    }

    if target_size > current_size {
        let next_size = (current_size + MAX_PERMITTED_DATA_INCREASE).min(target_size);
        resize_account(committee_info, system_program_info, fee_payer_info, next_size)?;
    }

    let capacity = if committee_info.data_len() >= target_size {
        let header = Committee::header_mut(committee_info, &tapedrive::ID)?;
        header.epoch = target;
        header.members.capacity = target_capacity;
        target_capacity
    } else {
        Committee::header(committee_info, &tapedrive::ID)?.members.capacity
    };

    CommitteeResized {
        epoch: target,
        capacity,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn resize_committee_noop_when_full() {
        let fee_payer = Pubkey::new_unique();
        let committee_size: u64 = 128;

        let (system_address, _) = system_pda();

        let curr = EpochNumber(2);
        let next = EpochNumber(3);
        let target = EpochNumber(4);
        let (committee_address, _) = committee_pda(target);
        let system = System {
            current_epoch: curr,
            committee_size,
            ..System::zeroed()
        };

        let committee = Committee { epoch: target, members: Tail::empty(committee_size) }
            .pack_with(&[]);

        let curr_epoch = Epoch {
            id: curr,
            state: EpochState {
                phase: EpochPhase::Closing as u64,
                ..EpochState::zeroed()
            },
            ..Epoch::zeroed()
        };

        let next_epoch = Epoch {
            id: next,
            preferences: NodePreferences {
                committee_size,
                ..NodePreferences::zeroed()
            },
            ..Epoch::zeroed()
        };

        let target_epoch = Epoch {
            id: target,
            ..Epoch::zeroed()
        };

        let instruction = build_resize_committee_ix(fee_payer.into(), curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_pda(curr).0, curr_epoch.pack(), tapedrive::ID),
            pda(epoch_pda(next).0, next_epoch.pack(), tapedrive::ID),
            pda(epoch_pda(target).0, target_epoch.pack(), tapedrive::ID),
            pda(committee_address, committee, tapedrive::ID),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::success()],
        );
    }
}
