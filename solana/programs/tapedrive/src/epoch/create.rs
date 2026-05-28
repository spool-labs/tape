use tape_solana::*;
use tape_api::event::EpochCreated;
use tape_api::program::prelude::*;

pub fn process_create_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = CreateEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
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

    let id = args.epoch;
    let (epoch_address, _) = epoch_pda(id);

    epoch_info
        .is_empty()?
        .is_writable()?
        .has_address(&epoch_address.into())?;

    create_program_account::<Epoch>(
        epoch_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[EPOCH, &id.pack()],
    )?;

    let epoch = epoch_info.as_account_mut::<Epoch>(&tapedrive::ID)?;

    epoch.id = id;
    if id == EpochNumber(0) {
        let clock = Clock::get()?;
        epoch.start_slot = SlotNumber(clock.slot);
        epoch.start_time = clock.unix_timestamp;
    }
    epoch.state.phase = EpochPhase::Unknown as u64;

    EpochCreated { epoch: id }.log();

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

        let (epoch_address, _) = epoch_pda(target);

        let instruction = build_create_epoch_ix(fee_payer.into(), target);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
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
                    .data(
                        Epoch {
                            id: target,
                            ..Epoch::zeroed()
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }

    #[test]
    fn create_bootstrap_epoch_sets_replay_boundary() {
        let fee_payer = Pubkey::new_unique();
        let target = EpochNumber(0);

        let (epoch_address, _) = epoch_pda(target);

        let instruction = build_create_epoch_ix(fee_payer.into(), target);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            empty(epoch_address),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        let slot = env.slot();
        let now = env.now();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(epoch_address))
                    .space(Epoch::get_size())
                    .owner(&tapedrive::ID)
                    .data(
                        Epoch {
                            id: target,
                            start_slot: SlotNumber(slot),
                            start_time: now,
                            state: EpochState {
                                phase: EpochPhase::Unknown as u64,
                                ..EpochState::zeroed()
                            },
                            ..Epoch::zeroed()
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }
}
