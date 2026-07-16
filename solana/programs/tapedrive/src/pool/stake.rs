use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::StakeDeposited;

pub fn process_stake_with_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeWithPool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        system_info,
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

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    let prev = system.current_epoch.prev();
    if node.latest_advance_epoch < prev {
        return Err(TapeError::NodeStale.into());
    }

    let (stake_address, _) = stake_pda((*authority_info.key).into());
    let (vault_address, _) = vault_pda(stake_address);

    // We require a new stake account for each stake action to simplify logic.
    // A user can merge stakes, if needed.
    stake_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_address.into())?;

    vault_info
        .is_empty()?
        .is_writable()?
        .has_address(&vault_address.into())?;

    let amount = args.amount;
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    let current = system.current_epoch;
    let activation_epoch = if current == EpochNumber(0) {
        current
    } else {
        current + EpochNumber(2)
    };

    let staked_tape = node.pool
        .stake_with_pool_at(current, activation_epoch, amount.into())
        .map_err(|_| TapeError::StakingFailed)?;

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

    stake.authority = (*authority_info.key).into();
    stake.pool = (*node_info.key).into();
    stake.inner = staked_tape;

    // Create the vault for the stake and transfer tokens into it
    // (in an isolated program to remove custody issues)
    solana_program::program::invoke(
        &build_stake_ix(
            (*fee_payer_info.key).into(),
            (*authority_info.key).into(),
            (*node_info.key).into(),
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
        authority: (*authority_info.key).into(),
        pool: (*node_info.key).into(),
        amount,
        activation_epoch: staked_tape.activation_epoch,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }
    fn shares(v: u64) -> ShareAmount { ShareAmount(v) }

    #[test]
    fn stake_with_node() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;
        let current = EpochNumber(42);

        let instruction = build_stake_with_pool_ix(
            fee_payer.into(),
            authority.into(),
            pool_address.into(),
            amount.into(),
        );

        let authority_ata = ata_address(&authority);
        let (system_address, _) = system_pda();
        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);

        let system = System {
            current_epoch: current,
            committee_size: 128,
            ..System::zeroed()
        };

        let e0: EpochNumber = current;
        let e1: EpochNumber = e0.next();
        let e2: EpochNumber = e0.saturating_add(EpochNumber(2));

        let mut node = Node::zeroed();
        node.id = NodeId(4);
        node.latest_advance_epoch = current;
        node.pool.stake = tape(5000);
        node.pool.shares = shares(5000);

        node.pool.schedule.stake(e1, tape(1000)).expect("stake");
        node.pool.schedule.stake(e2, tape(200)).expect("stake");
        node.pool.schedule.unstake(e1, shares(100)).expect("stake");
        node.pool.schedule.unstake(e2, shares(50)).expect("stake");

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            pda(system_address, system.pack(), tapedrive::ID),
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
                Check::account(&Pubkey::from(stake_address)).data(
                    Stake {
                        authority: authority.into(),
                        pool: pool_address.into(),
                        inner: StakedTape {
                            amount: amount.into(),
                            activation_epoch: e2,
                            unlock_shares: ShareAmount::zero(),
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(authority_ata)).data(
                    token(
                        authority_ata,
                        authority,
                        initial_token_balance - amount,
                    ).1.data.as_ref()
                ).build(),
                Check::account(&Pubkey::from(vault_address)).data(
                    token(
                        vault_address,
                        vault_address,
                        amount,
                    ).1.data.as_ref()
                ).build(),
            ]
        );
    }

    #[test]
    fn stake_with_node_bootstrap_immediate() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;
        let current = EpochNumber(0);

        let instruction = build_stake_with_pool_ix(
            fee_payer.into(),
            authority.into(),
            pool_address.into(),
            amount.into(),
        );

        let authority_ata = ata_address(&authority);
        let (system_address, _) = system_pda();
        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);

        let system = System {
            current_epoch: current,
            committee_size: 128,
            ..System::zeroed()
        };

        let mut node = Node::zeroed();
        node.id = NodeId(4);
        node.latest_advance_epoch = current;
        node.pool.stake = tape(5000);
        node.pool.shares = shares(5000);

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, initial_token_balance),

            pda(system_address, system.pack(), tapedrive::ID),
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
                Check::account(&Pubkey::from(stake_address)).data(
                    Stake {
                        authority: authority.into(),
                        pool: pool_address.into(),
                        inner: StakedTape {
                            amount: amount.into(),
                            activation_epoch: current,
                            unlock_shares: ShareAmount::zero(),
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
