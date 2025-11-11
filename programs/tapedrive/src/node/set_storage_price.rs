use tape_api::prelude::*;
use steel::*;

pub fn process_set_storage_price(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetStoragePrice::try_from_bytes(data)?;
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

    let storage_price = TAPE::unpack(args.price);
    node.preferences.storage_price = storage_price;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_storage_price() {
        let signer = Pubkey::new_unique();
        let old_price = TAPE(500);
        let new_price = TAPE(1000);

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_storage_price_ix(signer, node_address, new_price);

        let node = Node {
            authority: signer,
            preferences: NodePreferences {
                storage_price: old_price,
                ..NodePreferences::zeroed()
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
                        preferences: NodePreferences {
                            storage_price: new_price,
                            ..NodePreferences::zeroed()
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
