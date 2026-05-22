use tape_api::program::prelude::*;

use crate::tape::helpers::destroy_expired;

pub fn process_destroy_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = DestroyBlacklist::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        blacklist_info,
        system_info,
        system_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;

    let node = node_info.as_account::<Node>(&tapedrive::ID)?;
    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let node_address = (*node_info.key).into();
    let (blacklist_address, _) = blacklist_pda(node_address);

    let tape = blacklist_info
        .is_writable()?
        .has_address(&blacklist_address.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if tape.authority != node_address {
        return Err(ProgramError::InvalidAccountData);
    }

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    destroy_expired(
        blacklist_info,
        fee_payer_info,
        system,
        blacklist_address,
        node_address,
        tape.expiry_epoch,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn destroy_blacklist() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let node_address = Pubkey::new_unique();
        let (blacklist_address, _) = blacklist_pda(node_address.into());
        let (system_address, _) = system_pda();

        let node = Node {
            authority: authority.into(),
            ..Node::zeroed()
        };
        let tape = Tape {
            authority: node_address.into(),
            capacity: StorageUnits::mb(1),
            used: StorageUnits::from_bytes(128),
            active_epoch: EpochNumber(0),
            expiry_epoch: EpochNumber(5),
            ..Tape::zeroed()
        };
        let system = System {
            current_epoch: EpochNumber(7),
            ..System::zeroed()
        };

        let instruction =
            build_destroy_blacklist_ix(fee_payer.into(), authority.into(), node_address.into());

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(blacklist_address, tape.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            system_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(fee_payer))
                    .lamports(1_000_000_000 + rent(Tape::get_size()))
                    .build(),
                Check::account(&Pubkey::from(blacklist_address))
                    .lamports(0)
                    .closed()
                    .build(),
            ],
        );
    }
}
