use tape_api::prelude::*;
use steel::*;

pub fn process_set_authority(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = SetAuthority::try_from_bytes(data)?;
    let [
        signer_info,
        new_authority_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    node.authority = *new_authority_info.key;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_authority() {
        let signer = Pubkey::new_unique();
        let new_authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_authority_ix(signer, node_address, new_authority);

        let node = Node {
            authority: signer,
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
            sol(new_authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&node_address)
                    .data(Node {
                        authority: new_authority,
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
