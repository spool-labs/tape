use tape_api::prelude::*;
use steel::*;

pub fn process_set_network_address(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetNetworkAddress::try_from_bytes(data)?;
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

    node.metadata.network_address = args.network_address;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_network_address() {
        let signer = Pubkey::new_unique();
        let old_address = NetworkAddress::new_ipv4([1, 2, 3, 4], 1234);
        let new_address = NetworkAddress::new_ipv4([5, 6, 7, 8], 5678);

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_network_address_ix(signer, node_address, new_address);

        let node = Node {
            authority: signer,
            metadata: NodeMetadata {
                network_address: old_address,
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
                            network_address: new_address,
                            ..node.metadata
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
