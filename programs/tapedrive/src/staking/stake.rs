use tape_solana::*;
use tape_api::prelude::*;
use tape_api::event::StakeDeposited;
use crate::error::*;

pub fn process_stake_with_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeWithPool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        system_info,
        epoch_info,
        node_info,
        stake_info,
        vault_info,

        mint_info,
        token_program_info,
        system_program_info,
        stakeing_program_info,
        rent_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

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

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

    if node.latest_advance_epoch < prev_epoch(epoch) {
        return Err(TapeError::NodeStale.into());
    }

    let (stake_address, _) = stake_pda(*authority_info.key);
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

    let amount = TAPE::unpack(args.amount);
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    // Determine activation epoch based on system state
    let current = current_epoch(epoch);
    let activation_epoch = if system.will_be_low_quorum() {
        // Low-quorum: activate immediately so node can join
        current
    } else {
        // Normal mode: standard E+2 delay
        current + EpochNumber(2)
    };

    let staked_tape = node.pool
        .stake_with_pool_at(current, activation_epoch, amount.into())
        .map_err(|_| TapeError::StakingFailed)?;

    // Create the state account
    create_program_account::<Stake>(
        stake_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[STAKE, authority_info.key.as_ref()],
    )?;

    let stake = stake_info
        .is_type::<Stake>(&tapedrive::ID)?
        .as_account_mut::<Stake>(&tapedrive::ID)?;

    stake.authority  = *authority_info.key;
    stake.pool       = *node_info.key;
    stake.inner      = staked_tape;

    // Create the vault for the stake and transfer tokens into it
    // (in an isolated program to remove custody issues)
    solana_program::program::invoke(
        &build_stake_ix(
            *fee_payer_info.key,
            *authority_info.key,
            *node_info.key,
            amount,
        ),
        &[
            fee_payer_info.clone(),
            authority_info.clone(),
            authority_ata_info.clone(),
            node_info.clone(),
            vault_info.clone(),

            mint_info.clone(),
            token_program_info.clone(),
            system_program_info.clone(),
        ],
    )?;

    StakeDeposited {
        stake: stake_address,
        authority: *authority_info.key,
        pool: *node_info.key,
        amount: amount.as_u64().to_le_bytes(),
        activation_epoch: staked_tape.activation_epoch,
    }.log();

    // TODO: update/advance the node's state?

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }
    fn shares(v: u64) -> ShareAmount { ShareAmount(v) }

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember::new(NodeId(id), TAPE(stake))
    }

    #[test]
    fn test_stake_with_node() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;

        let instruction = build_stake_with_pool_ix(fee_payer, authority, pool_address, amount.into());

        let authority_ata = ata_address(&authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(authority);
        let (vault_address, _) = vault_pda(stake_address);

        // Setup existing accounts

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Normal mode: committee_next has 20 nodes (>= MIN_COMMITTEE_SIZE)
        let members: Vec<CommitteeMember> = (1..=20)
            .map(|i| member(i, 1_000))
            .collect();
        system.committee_next = Committee::from_members(&members);

        epoch.id = EpochNumber(42);

        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);

        node.id = NodeId(4);
        node.latest_advance_epoch = epoch.id;
        node.pool.stake = tape(5000);
        node.pool.shares = shares(5000);

        node.pool.schedule.stake(e1, tape(1000)).expect("stake");
        node.pool.schedule.stake(e2, tape(200)).expect("stake");
        node.pool.schedule.unstake(e1, shares(100)).expect("stake");
        node.pool.schedule.unstake(e2, shares(50)).expect("stake");

        assert_eq!(node.pool.calculate_stake_at(e0), tape(5000));
        assert_eq!(node.pool.calculate_stake_at(e1), tape(5900));
        assert_eq!(node.pool.calculate_stake_at(e2), tape(6050));

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            empty(stake_address),
            empty(vault_address),
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
                        authority: authority,
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
                Check::account(&authority_ata).data(
                    token(
                        authority_ata,
                        authority,
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

    #[test]
    fn test_stake_with_node_low_quorum_immediate() {
        // Test that in low-quorum mode, stake activates immediately (E+0)
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;

        let instruction = build_stake_with_pool_ix(fee_payer, authority, pool_address, amount.into());

        let authority_ata = ata_address(&authority);
        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(authority);
        let (vault_address, _) = vault_pda(stake_address);

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        // Low-quorum mode: committee_next has < MIN_COMMITTEE_SIZE (20) nodes
        let members: Vec<CommitteeMember> = (1..=10)
            .map(|i| member(i, 1_000))
            .collect();
        system.committee_next = Committee::from_members(&members);

        epoch.id = EpochNumber(42);

        let e0: EpochNumber = epoch.id;

        node.id = NodeId(4);
        node.latest_advance_epoch = epoch.id;
        node.pool.stake = tape(5000);
        node.pool.shares = shares(5000);

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
            empty(stake_address),
            empty(vault_address),
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
                // Activation epoch is E+0 (immediate) in low-quorum mode
                Check::account(&stake_address).data(
                    Stake {
                        authority: authority,
                        pool: pool_address,
                        inner: StakedTape {
                            amount: amount.into(),
                            activation_epoch: e0,  // Immediate activation
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
