use tape_api::prelude::*;
use steel::*;

pub fn process_set_storage_capacity(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetStorageCapacity::try_from_bytes(data)?;
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

    let storage_capacity = StorageUnits::unpack(args.size);
    node.preferences.storage_capacity = storage_capacity;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_storage_capacity() {
        let signer = Pubkey::new_unique();
        let old_capacity = StorageUnits(5_000_000);
        let new_capacity = StorageUnits(1_000_000);

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_storage_capacity_ix(signer, node_address, new_capacity);

        let node = Node {
            authority: signer,
            preferences: NodePreferences {
                storage_capacity: old_capacity,
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
                            storage_capacity: new_capacity,
                            ..NodePreferences::zeroed()
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
