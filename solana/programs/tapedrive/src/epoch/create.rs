use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_create_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
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
    let (epoch_address, _) = epoch_pda(epoch);

    epoch_info
        .is_empty()?
        .is_writable()?
        .has_address(&epoch_address.into())?;

    create_program_account::<Epoch>(
        epoch_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[EPOCH, &epoch.pack()],
    )?;

    let epoch_acct = epoch_info.as_account_mut::<Epoch>(&tapedrive::ID)?;
    epoch_acct.id = epoch;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn create_epoch() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(7);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda(target);

        let system = System {
            current_epoch: EpochNumber(6),
            ..System::zeroed()
        };

        let instruction = build_create_epoch_ix(fee_payer.into(), target);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            empty(epoch_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(epoch_address))
                    .space(Epoch::get_size())
                    .owner(&tapedrive::ID)
                    .data_slice(0, &[Epoch::discriminator()])
                    .build(),
            ],
        );
    }
}
