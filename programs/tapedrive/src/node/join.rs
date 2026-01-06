use steel::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_join_network(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = JoinNetwork::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        system_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .as_account::<Node>(&tapedrive::ID)?;

    if node.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Find the stake balance at activation epoch (1 epoch from now)
    let activation_epoch = next_epoch(epoch);
    let balance = node.pool
        .calculate_stake_at(activation_epoch);

    let member = CommitteeMember {
        id: node.id,
        stake: balance,
        key: node.metadata.bls_pubkey,
        blacklist: node.blacklist.total_size(),
        preferences: node.preferences.clone(),
        weight: 0,
    };

    system.committee_next
        .try_join(&member)
        .map_err(|_| TapeError::UnexpectedState)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember::new(NodeId(id), TAPE(stake))
    }

    #[test]
    fn test_join_network() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (node_address, _) = node_pda(authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();

        let instruction = build_join_network_ix(fee_payer, authority, node_address);

        // Setup existing accounts
        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        system.committee_next = Committee::from_members(&[
            member(3, 3_500),
            member(4, 2_100),
        ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(5);
        node.authority = authority;

        // Minimal pool setup to produce a non-zero activation balance
        node.pool.stake = TAPE(1_000);
        node.pool.shares = ShareAmount(1_000);
        node.preferences = NodePreferences {
            storage_price: TAPE(10),
            storage_capacity: StorageUnits(1_000_000),
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        // Expected state after instruction
        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);

        let balance = node.pool.calculate_stake_at(e1);

        let member = CommitteeMember {
            id: node.id,
            stake: balance,
            key: node.metadata.bls_pubkey,
            blacklist: node.blacklist.total_size(),
            preferences: node.preferences.clone(),
            ..CommitteeMember::zeroed()
        };

        system
            .committee_next
            .try_join(&member)
            .expect("join committee");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address)
                    .data(system.pack().as_ref())
                    .build(),
                Check::account(&epoch_address) // unchanged
                    .data(epoch.pack().as_ref())
                    .build(),
                Check::account(&node_address) // unchanged
                    .data(node.pack().as_ref())
                    .build(),
            ],
        );
    }
}
