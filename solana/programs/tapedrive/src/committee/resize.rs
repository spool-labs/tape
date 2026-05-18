use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;
use solana_program::sysvar::rent::Rent;
use solana_program::sysvar::Sysvar;
use tape_solana::*;
use tape_api::dynamic::DynamicState;
use tape_api::event::CommitteeResized;
use tape_api::program::prelude::*;
use tape_api::state::Committee;

pub fn process_resize_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = ResizeCommittee::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
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
    system_info
        .is_system()?;

    let epoch = EpochNumber::unpack(args.epoch);
    committee_info
        .is_writable()?
        .is_committee(epoch)?;

    let target_capacity = system_info
        .as_account::<System>(&tapedrive::ID)?
        .committee_size;

    let target_size = Committee::size_for_capacity(target_capacity);
    let current_size = committee_info.data_len();
    if current_size >= target_size {
        log_committee_resized(committee_info, epoch)?;
        return Ok(());
    }

    let next_size = (current_size + MAX_PERMITTED_DATA_INCREASE).min(target_size);
    let needed = Rent::get()?
        .minimum_balance(next_size)
        .saturating_sub(committee_info.lamports());
    if needed > 0 {
        solana_program::program::invoke(
            &solana_program::system_instruction::transfer(
                fee_payer_info.key,
                committee_info.key,
                needed,
            ),
            &[
                fee_payer_info.clone(),
                committee_info.clone(),
                system_program_info.clone(),
            ],
        )?;
    }
    committee_info.resize(next_size)?;

    if next_size == target_size {
        let header = Committee::header_mut(committee_info, &tapedrive::ID)?;
        header.epoch = epoch;
        header.members.capacity = target_capacity;
        header.members.count = 0;
    }

    log_committee_resized(committee_info, epoch)?;

    Ok(())
}

fn log_committee_resized(committee_info: &AccountInfo<'_>, epoch: EpochNumber) -> ProgramResult {
    let capacity = Committee::header(committee_info, &tapedrive::ID)?
        .members
        .capacity;
    CommitteeResized {
        epoch,
        capacity: capacity.to_le_bytes(),
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
        let target = EpochNumber(3);
        let committee_size: u64 = 128;

        let (system_address, _) = system_pda();
        let (committee_address, _) = committee_pda(target);

        let system = System {
            current_epoch: EpochNumber(3),
            committee_size,
            ..System::zeroed()
        };

        let committee = Committee { epoch: target, members: Tail::empty(committee_size) }
            .pack_with(&[]);

        let instruction = build_resize_committee_ix(fee_payer.into(), target);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
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
