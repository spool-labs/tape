use tape_api::prelude::*;
use steel::*;

pub fn process_set_name(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetName::try_from_bytes(data)?;
    let [
        signer_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    node.metadata.name = args.name;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_network_tls() {
        let signer = Pubkey::new_unique();
        let old_name = "hello, world";
        let new_name = "tapedrive";

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_name_ix(signer, node_address, new_name);

        let node = Node {
            authority: signer,
            metadata: NodeMetadata {
                name: to_name(old_name),
                ..NodeMetadata::zeroed()
            },
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
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
                        metadata: NodeMetadata {
                            name: to_name(new_name),
                            ..node.metadata
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
