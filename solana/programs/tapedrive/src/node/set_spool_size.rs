use tape_api::program::prelude::*;

pub fn process_set_spool_size(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetSpoolSize::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    node.preferences.spool_size = args.spool_size;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn set_spool_size() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_size = StorageUnits::mb(5_000_000);
        let new_size = StorageUnits::mb(1_000_000);

        let (node_address, _) = node_pda(authority.into());

        let instruction = build_set_spool_size_ix(fee_payer.into(), authority.into(), node_address, new_size);

        let node = Node {
            authority: authority.into(),
            preferences: NodePreferences {
                spool_size: old_size,
                ..NodePreferences::zeroed()
            },
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
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
                            spool_size: new_size,
                            ..NodePreferences::zeroed()
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
