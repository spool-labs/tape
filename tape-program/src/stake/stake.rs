use tape_api::prelude::*;
use steel::*;

pub fn process_stake_with_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = StakeWithNode::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,
        stake_info,
        stake_ata_info,
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

    let (stake_address, _)     = stake_pda(*signer_info.key, *node_info.key);
    let (stake_ata_address, _) = stake_ata(stake_address);

    stake_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_address)?;

    stake_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&stake_ata_address)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tape_api::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tape_api::ID)?;

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

    // Stake the tokens with the node's staking pool
    let staked_tape = node.pool
        .stake_with_pool(current_epoch(epoch), amount.into())
        .map_err(|_| TapeError::StakingFailed)?;
    
    // TODO: If the node is part of the epoch leader set, we *might* need to update the stake
    // (perhaps not needed as the candidate set is for E+1, and the stake activates in E+2)
    // if epoch.leaders.contains(&node.id) {
    //     ...
    //     epoch.leaders.update_stake(jkhh, new_stake)
    // }

    create_program_account::<Stake>(
        stake_info,
        system_program_info,
        signer_info,
        &tape_api::ID,
        &[STAKE, signer_info.key.as_ref(), node_info.key.as_ref()],
    )?;

    let stake = stake_info.as_account_mut::<Stake>(&tape_api::ID)?;

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
        let commission_rate = BasisPoints(100); // 1%
        let amount: u64 = 1000;

        let instruction = build_stake_ix(signer, node_key, amount.into());

        let (epoch_address, _) = epoch_pda();
        let (stake_address, _) = stake_pda(signer, node_key);
        let stake_ata = ata_address(&stake_address);
        let signer_ata = ata_address(&signer);

        // Setup existing accounts

        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            last_epoch_ms: 0,
            leaders: CandidateSet::zeroed(),
        };

        let mut node = Node {
            id: NodeId::new(37),
            authority: signer,
            pool: StakingPool::new(commission_rate),
            metadata: NodeMetadata::zeroed(),
            registered_epoch: EpochNumber(1),
        };

        // Setup initial state
        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);

        node.pool.tape_balance = TAPE(1000);
        node.pool.pending_stake = PendingValues(
            FixedMap {
                length: 2,
                keys: [e1, e2],
                values: [200, 30],
            }
        );

        // Check initial state
        assert_eq!(node.pool.tape_balance_at_epoch(e0), TAPE(1000));
        assert_eq!(node.pool.tape_balance_at_epoch(e1), TAPE(1200));
        assert_eq!(node.pool.tape_balance_at_epoch(e2), TAPE(1230));

        let initial_token_balance: u64 = 1_000_000_000;

        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, initial_token_balance),
            empty(stake_address),
            empty(stake_ata),

            pda(epoch_address, epoch.pack()),
            pda(node_key, node.pack()),
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
                    Stake {
                        authority: signer,
                        node: node_key,
                        inner: StakedTape {
                            amount: amount.into(),
                            activation_epoch: e2,
                            state: *StakeState::new().set_staked(),
                        },
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_key).data(
                    Node {
                        pool: StakingPool {
                            pending_stake: PendingValues(FixedMap {
                                length: 2,
                                keys: [e1, e2],
                                values: [200, 30 + amount],
                            }),
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, initial_token_balance - amount).1.data.as_ref()
                ).build(),
                Check::account(&stake_ata).data(
                    token(stake_ata, stake_address, amount).1.data.as_ref()
                ).build(),
            ]
        );
    }
}
