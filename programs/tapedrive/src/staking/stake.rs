use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_stake_with_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeWithPool::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        stake_info,
        vault_info,
        epoch_info,
        node_info,

        mint_info,
        token_program_info,
        system_program_info, 
        stakeing_program_info,
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

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    mint_info
        .is_mint()?;

    token_program_info
        .is_program(&spl_token::ID)?;
    system_program_info
        .is_program(&system_program::ID)?;
    stakeing_program_info
        .is_program(&staking::ID)?;
    rent_info
        .is_sysvar(&sysvar::rent::ID)?;

    let amount = TAPE::unpack(args.amount);
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    let (stake_address, _) = stake_pda(*signer_info.key, *node_info.key);
    let (vault_address, _) = vault_pda(stake_address);

    // We require a new stake account for each stake action to simplify logic. 
    // A user can merge stakes, if needed.

    stake_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_address)?;

    vault_info
        .is_empty()?
        .is_writable()?
        .has_address(&vault_address)?;

    let staked_tape = node.pool
        .stake(current_epoch(epoch), amount.into())
        .map_err(|_| TapeError::StakingFailed)?;

    // Create the state account
    create_program_account::<Stake>(
        stake_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[STAKE, signer_info.key.as_ref(), node_info.key.as_ref()],
    )?;

    let stake = stake_info
        .is_type::<Stake>(&tapedrive::ID)?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    stake.authority  = *signer_info.key;
    stake.pool       = *node_info.key;
    stake.inner      = staked_tape;

    // Create the vault for the stake and transfer tokens into it
    // (in an isolated program to remove custody issues)
    solana_program::program::invoke(
        &build_stake_ix(
            *signer_info.key,
            *node_info.key,
            amount,
        ),
        &[
            signer_info.clone(),
            signer_ata_info.clone(),
            node_info.clone(),
            vault_info.clone(),
            mint_info.clone(),
        ],
    )?;

    // TODO: update/advance the node's state?

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_stake_with_node() {
        let signer = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;

        let instruction = build_stake_with_pool_ix(signer, pool_address, amount.into());

        let signer_ata = ata_address(&signer);
        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(signer, pool_address);
        let (vault_address, _) = vault_pda(stake_address);

        // Setup existing accounts

        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        epoch.id = EpochNumber(42);

        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);

        node.id = NodeId(5);
        node.pool.stake = TAPE(5000);
        node.pool.schedule.incoming_tokens = EpochValues::try_from(
            &[e1, e2],
            &[1000, 200],
        ).expect("schedule incoming");

        node.pool.schedule.outgoing_tokens = EpochValues::try_from(
            &[e1, e2],
            &[100, 50],
        ).expect("schedule outgoing");

        assert_eq!(node.pool.stake_at(e0), TAPE(5000));
        assert_eq!(node.pool.stake_at(e1), TAPE(5900));
        assert_eq!(node.pool.stake_at(e2), TAPE(6050));

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),

            empty(stake_address),
            empty(vault_address),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            mint(0),

            token_program(),
            system_program(),
            staking_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction, 
            &accounts,
            &[
                Check::success(),
                Check::account(&stake_address).data(
                    Stake {
                        authority: signer,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: amount.into(),
                            activation_epoch: e2,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
                Check::account(&pool_address).data(
                    Node {
                        pool: StakingPool {
                            schedule: PoolSchedule {
                                incoming_tokens: EpochValues::try_from(
                                    &[e1, e2],
                                    &[1000, 200 + amount],
                                ).expect("schedule incoming"),
                                ..node.pool.schedule
                            },
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(
                        signer_ata, 
                        signer, 
                        initial_token_balance - amount
                    ).1.data.as_ref()
                ).build(),
                Check::account(&vault_address).data(
                    token(
                        vault_address, 
                        vault_address, 
                        amount
                    ).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
