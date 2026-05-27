use tape_api::program::prelude::*;

pub fn process_set_epoch_duration(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetEpochDuration::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        system_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let epoch_duration = EpochDuration::unpack(args.epoch_duration);
    if epoch_duration < system.min_epoch_duration
        || epoch_duration > system.max_epoch_duration
    {
        return Err(ProgramError::InvalidArgument);
    }

    node.preferences.epoch_duration = epoch_duration;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn set_epoch_duration() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();

        let new_duration = EpochDuration(50);
        let instruction = build_set_epoch_duration_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            new_duration,
        );

        let system = System {
            min_epoch_duration: TEST_MIN_EPOCH_DURATION,
            max_epoch_duration: TEST_MAX_EPOCH_DURATION,
            ..System::zeroed()
        };

        let node = Node {
            authority: authority.into(),
            preferences: NodePreferences {
                epoch_duration: TEST_EPOCH_DURATION,
                ..NodePreferences::zeroed()
            },
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node_address))
                    .data(Node {
                        preferences: NodePreferences {
                            epoch_duration: new_duration,
                            ..NodePreferences::zeroed()
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn rejects_below_min() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();

        let instruction = build_set_epoch_duration_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            EpochDuration(5),
        );

        let system = System {
            min_epoch_duration: TEST_MIN_EPOCH_DURATION,
            max_epoch_duration: TEST_MAX_EPOCH_DURATION,
            ..System::zeroed()
        };

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidArgument)],
        );
    }

    #[test]
    fn rejects_above_max() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let (node_address, _) = node_pda(authority.into());
        let (system_address, _) = system_pda();

        let instruction = build_set_epoch_duration_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            EpochDuration(TEST_MAX_EPOCH_DURATION.0 + 1),
        );

        let system = System {
            min_epoch_duration: TEST_MIN_EPOCH_DURATION,
            max_epoch_duration: TEST_MAX_EPOCH_DURATION,
            ..System::zeroed()
        };

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(ProgramError::InvalidArgument)],
        );
    }
}
