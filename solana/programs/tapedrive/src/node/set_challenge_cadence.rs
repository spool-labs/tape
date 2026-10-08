use tape_api::program::prelude::*;

pub fn process_set_challenge_cadence(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetChallengeCadence::try_from_bytes(data)?;
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

    node.preferences.challenge_cadence = args.challenge_cadence;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    // the node's authority changes its vote
    #[test]
    fn set_challenge_cadence() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_cadence = 5;
        let new_cadence = 10;

        let (node_address, _) = node_pda(authority.into());

        let instruction = build_set_challenge_cadence_ix(fee_payer.into(), authority.into(), node_address, new_cadence);

        let node = Node {
            authority: authority.into(),
            preferences: NodePreferences {
                challenge_cadence: old_cadence,
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
                            challenge_cadence: new_cadence,
                            ..NodePreferences::zeroed()
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }

    // a signer other than the node's authority can't change its vote
    #[test]
    fn wrong_authority() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let new_cadence = 10;

        let (node_address, _) = node_pda(authority.into());

        let instruction = build_set_challenge_cadence_ix(fee_payer.into(), authority.into(), node_address, new_cadence);

        let node = Node {
            authority: Pubkey::new_unique().into(),
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
            &[Check::err(ProgramError::InvalidAccountData)],
        );
    }
}
