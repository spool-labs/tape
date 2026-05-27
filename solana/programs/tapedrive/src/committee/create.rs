use tape_api::dynamic::DynamicState;
use tape_api::event::CommitteeCreated;
use tape_api::program::prelude::*;
use tape_api::state::Committee;

pub fn process_create_committee(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateCommittee::try_from_bytes(data)?;
    let [
        fee_payer_info,
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

    let epoch = args.epoch;
    let (committee_address, bump) = committee_pda(epoch);

    committee_info
        .is_empty()?
        .is_writable()?
        .has_address(&committee_address.into())?;

    create_account_with_size::<Committee>(
        committee_info,
        system_program_info,
        fee_payer_info,
        Committee::get_size(),
        &tapedrive::ID,
        &[COMMITTEE, &epoch.pack()],
        bump,
    )?;

    let committee = Committee::header_mut(committee_info, &tapedrive::ID)?;
    committee.epoch = epoch;
    committee.members = Tail::empty(0);

    CommitteeCreated {
        epoch,
        capacity: 0,
    }
    .log();

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

        let (committee_address, _) = committee_pda(target);

        let instruction = build_create_committee_ix(fee_payer.into(), target);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            empty(committee_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(committee_address))
                    .space(Committee::get_size())
                    .owner(&tapedrive::ID)
                    .data(
                        Committee {
                            epoch: target,
                            members: Tail::empty(0),
                        }
                        .pack_with(&[])
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }
}
