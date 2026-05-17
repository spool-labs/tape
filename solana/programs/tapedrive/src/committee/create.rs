use core::mem::size_of;
use solana_program::entrypoint::MAX_PERMITTED_DATA_INCREASE;
use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::state::Committee;
use tape_core::system::Member;

pub fn process_create_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateCommittee::try_from_bytes(data)?;
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
    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = EpochNumber::unpack(args.epoch);
    let (committee_address, bump) = committee_pda(epoch);

    committee_info
        .is_empty()?
        .is_writable()?
        .has_address(&committee_address.into())?;

    let genesis_capacity = if system.current_epoch == EpochNumber(0) && epoch == EpochNumber(1) {
        GROUP_SIZE as u64
    } else {
        0
    };

    let initial_size = if genesis_capacity > 0 {
        Committee::get_size()
            .saturating_add((genesis_capacity as usize).saturating_mul(size_of::<Member>()))
    } else {
        MAX_PERMITTED_DATA_INCREASE.min(Committee::get_size())
    };

    create_account_with_size::<Committee>(
        committee_info,
        system_program_info,
        fee_payer_info,
        initial_size,
        &tapedrive::ID,
        &[COMMITTEE, &epoch.pack()],
        bump,
    )?;

    if genesis_capacity > 0 {
        let committee = Committee::header_mut(committee_info, &tapedrive::ID)?;
        committee.epoch = epoch;
        committee.members = Tail::empty(genesis_capacity);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn create_committee() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(3);

        let (system_address, _) = system_pda();
        let (committee_address, _) = committee_pda(target);

        let system = System {
            current_epoch: EpochNumber(2),
            committee_size: 128,
            ..System::zeroed()
        };

        let instruction = build_create_committee_ix(fee_payer.into(), target);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            empty(committee_address),
            system_program(),
            rent_sysvar(),
        ];

        let initial_size = MAX_PERMITTED_DATA_INCREASE.min(Committee::get_size());

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(committee_address))
                    .space(initial_size)
                    .owner(&tapedrive::ID)
                    .data_slice(0, &[Committee::discriminator()])
                    .build(),
            ],
        );
    }
}
