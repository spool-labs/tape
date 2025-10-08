use tape_api::prelude::*;
use steel::*;

pub fn process_stake_with_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeWithNode::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        stake_info,
        stake_ata_info,
        system_info,
        epoch_info,
        node_info,
        mint_info,
        token_program_info,
        associated_token_program_info,
        system_program_info, 
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    let (stake_address, _)     = staked_tape_pda(*signer_info.key, *node_info.key);
    let (stake_ata_address, _) = staked_tape_ata(stake_address);

    stake_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_address)?;

    stake_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_ata_address)?;

    let _system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tape_api::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<StorageNode>(&tape_api::ID)?;

    mint_info
        .is_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let amount = u64::from_le_bytes(args.amount);

    let staked_tape = node.pool.stake_with_pool(
        current_epoch(epoch),
        amount.into()
    ).map_err(|_| TapeError::StakingFailed)?;

    create_program_account::<StakedTape>(
        stake_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[STAKE, signer_info.key.as_ref(), node_info.key.as_ref()],
    )?;

    let stake = stake_info.as_account_mut::<StakedTape>(&tape_api::ID)?;

    stake.authority       = *signer_info.key;
    stake.node            = *node_info.key;
    stake.inner           = staked_tape;

    create_associated_token_account(
        signer_info,
        stake_info,
        stake_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    transfer(
        signer_info,
        signer_ata_info,
        stake_ata_info,
        token_program_info,
        amount,
    )?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_stake_with_node() {
        let signer = Pubkey::new_unique();
        let node_key = Pubkey::new_unique();
        let amount: u64 = 1000;
        let commission_rate = BasisPoints(100); // 1%

        let instruction = build_stake_ix(signer, node_key, amount.into());

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = staked_tape_pda(signer, node_key);
        let stake_ata = ata_address(&stake_address);
        let signer_ata = ata_address(&signer);

        // Setup existing accounts

        let system_data = System {
            total_nodes: 1,
        }.pack();

        let current_epoch_number = EpochNumber(42);
        let epoch_data = Epoch {
            id: current_epoch_number,
            state: EpochState::zeroed(),
            last_epoch_ms: 0,
            leaders: CandidateSet::zeroed(),
        }.pack();

        let mut node_pool = StakingPool::new(commission_rate);

        let initial_node = StorageNode {
            id: NodeId::new(0),
            authority: Pubkey::new_unique(),
            pool: node_pool,
            metadata: NodeMetadata::zeroed(),
            registered_epoch: EpochNumber(1),
        };

        // Simulate the stake_with_pool effect for expected state
        let activation_epoch = current_epoch_number + EpochNumber(2);
        let expected_stake_inner = Stake::new(amount.into(), activation_epoch);

        // Update pool: schedule pending stake
        node_pool.pending_stake.insert_or_add(activation_epoch, amount).unwrap();

        let expected_node = StorageNode {
            pool: node_pool,
            ..initial_node
        };

        let initial_signer_balance: u64 = 2000; // More than amount

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_signer_balance),
            empty(stake_address),
            empty(stake_ata),

            pda(system_address, system_data),
            pda(epoch_address, epoch_data),
            pda(node_key, initial_node.pack()),
            mint(0),

            token_program(),
            ata_program(),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env("tape".to_string());
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&stake_address).data(
                    StakedTape {
                        authority: signer,
                        node: node_key,
                        inner: expected_stake_inner,
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_key).data(
                    expected_node.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_signer_balance - amount).1.data.as_ref()
                ).build(),
                Check::account(&stake_ata).data(
                    token(stake_ata, stake_address, amount).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
