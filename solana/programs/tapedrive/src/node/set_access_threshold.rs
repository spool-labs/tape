use tape_api::program::prelude::*;

pub fn process_set_access_threshold(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetAccessThreshold::try_from_bytes(data)?;
    let [fee_payer_info, authority_info, node_info] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info.is_signer()?.is_writable()?;
    authority_info.is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    node.preferences.access_threshold = args.access_threshold;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_access_threshold() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_threshold = TAPE(500);
        let new_threshold = TAPE(1000);

        let (node_address, _) = node_pda(authority.into());

        let instruction =
            build_set_access_threshold_ix(
                fee_payer.into(),
                authority.into(),
                node_address,
                new_threshold,
            );

        let node = Node {
            authority: authority.into(),
            preferences: NodePreferences {
                access_threshold: old_threshold,
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
                    .data(
                        Node {
                            preferences: NodePreferences {
                                access_threshold: new_threshold,
                                ..NodePreferences::zeroed()
                            },
                            ..node
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }
}
