use tape_api::prelude::*;
use steel::*;

pub fn process_set_network_tls(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetNetworkTls::try_from_bytes(data)?;
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

    node.metadata.network_tls = args.network_tls;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_network_tls() {
        let signer = Pubkey::new_unique();
        let old_tls = Pubkey::new_unique();
        let new_tls = Pubkey::new_unique();

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_network_tls_ix(signer, node_address, new_tls);

        let node = Node {
            authority: signer,
            metadata: NodeMetadata {
                network_tls: old_tls,
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
                            network_tls: new_tls,
                            ..node.metadata
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
