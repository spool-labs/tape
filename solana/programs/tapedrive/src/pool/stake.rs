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
        committee_next_info,
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

    let next_epoch = system.current_epoch.saturating_add(EpochNumber(1));

    committee_next_info.is_committee(next_epoch)?;

    let (committee_next, _) = Committee::read(committee_next_info, &tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    let prev = system.current_epoch.saturating_sub(EpochNumber(1));
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

    let amount = TAPE::unpack(args.amount);
    if amount.is_zero() {
        return Err(ProgramError::InvalidArgument);
    }

    // Determine activation epoch based on next-committee size.
    let current = system.current_epoch;
    let will_be_low_quorum = (committee_next.members.count as usize) < MIN_COMMITTEE_SIZE;
    let activation_epoch = if will_be_low_quorum {
        // Low-quorum: activate immediately so node can join
        current
    } else {
        // Normal mode: standard E+2 delay
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
        amount: amount.as_u64().to_le_bytes(),
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

    fn member(address_byte: u8, stake: u64) -> Member {
        let mut bytes = [0u8; 32];
        bytes[0] = address_byte;
        Member::new(Address::new(bytes), TAPE(stake))
    }

    fn committee(epoch: EpochNumber, members: &[Member]) -> Vec<u8> {
        Committee { epoch, members: Tail::new(128, members.len() as u64) }
            .pack_with(members)
    }

    #[test]
    fn stake_with_node() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;
        let current = EpochNumber(42);
        let next = EpochNumber(43);

        let instruction = build_stake_with_pool_ix(
            fee_payer.into(),
            authority.into(),
            pool_address.into(),
            amount.into(),
            current,
        );

        let authority_ata = ata_address(&authority);
        let (system_address, _) = system_pda();
        let (committee_next_addr, _) = committee_pda(next);
        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);

        let system = System {
            current_epoch: current,
            committee_size: 128,
            ..System::zeroed()
        };

        // Normal mode: committee_next has 20 nodes (>= MIN_COMMITTEE_SIZE)
        let members: Vec<Member> = (1..=20)
            .map(|i| member(i as u8, 1_000))
            .collect();
        let committee_next = committee(next, &members);

        let e0: EpochNumber = current;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);

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
            pda(committee_next_addr, committee_next, tapedrive::ID),
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
    fn stake_with_node_low_quorum_immediate() {
        // In low-quorum mode (committee_next < MIN_COMMITTEE_SIZE), stake activates at E+0.
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        let amount: u64 = 1000;
        let current = EpochNumber(42);
        let next = EpochNumber(43);

        let instruction = build_stake_with_pool_ix(
            fee_payer.into(),
            authority.into(),
            pool_address.into(),
            amount.into(),
            current,
        );

        let authority_ata = ata_address(&authority);
        let (system_address, _) = system_pda();
        let (committee_next_addr, _) = committee_pda(next);
        let (stake_address, _) = stake_pda(authority.into());
        let (vault_address, _) = vault_pda(stake_address);

        let system = System {
            current_epoch: current,
            committee_size: 128,
            ..System::zeroed()
        };

        // Low-quorum: committee_next has < MIN_COMMITTEE_SIZE (20) nodes
        let members: Vec<Member> = (1..=10)
            .map(|i| member(i as u8, 1_000))
            .collect();
        let committee_next = committee(next, &members);

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
            pda(committee_next_addr, committee_next, tapedrive::ID),
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
                Check::account(&Pubkey::from(stake_address)).data(
                    Stake {
                        authority: authority.into(),
                        pool: pool_address.into(),
                        inner: StakedTape {
                            amount: amount.into(),
                            activation_epoch: current,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
