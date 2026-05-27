//! `SetMinVersion` handler.
//!
//! Updates a node's preferred minimum protocol version. The aggregated
//! network value is recomputed at the next epoch boundary.

use tape_api::program::prelude::*;

pub fn process_set_min_version(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetMinVersion::try_from_bytes(data)?;
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

    node.preferences.min_version = args.min_version;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn set_min_version() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_version = VersionId(1);
        let new_version = VersionId(3);

        let (node_address, _) = node_pda(authority.into());

        let instruction = build_set_min_version_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            new_version,
        );

        let node = Node {
            authority: authority.into(),
            preferences: NodePreferences {
                min_version: old_version,
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
                            min_version: new_version,
                            ..NodePreferences::zeroed()
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
