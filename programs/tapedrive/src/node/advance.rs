use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_advance_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvancePool::try_from_bytes(data)?;
    let [
        signer_info,

        system_info,
        archive_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let system = system_info
        .is_writable()?
        .as_account::<System>(&tapedrive::ID)?;

    let archive = archive_info
        .is_writable()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.latest_epoch >= epoch.id {
        return Err(ProgramError::Custom(0));
        //.map_err(|_| TapeError::PoolAlreadyAdvanced);
    }

    // Get the rewards earned by this pool in the last epoch
    // rewards[n] = (weight[n] * (used_capacity - blocklist_size[n])) / sum(stored[n]) * total_rewards)

    let capacity_used = archive.recent_usage;
    let total_rewards = archive.recent_fees;

    let weight = system.committee_prev
        .index_of(&node.id)
        .map_or(0, |idx| system.seats.weight(idx));


    //// If this node is part of the next committee, update its stake there too
    //if system.committee_next.contains(&node.id) {
    //
    //    let next_stake = node.pool
    //        .stake_at(next_epoch(epoch));
    //
    //    system.committee_next
    //        .update_stake(&node.id, next_stake)
    //        .map_err(|_| ProgramError::Custom(1))?;
    //        //.map_err(|_| TapeError::CommitteeUpdateFailed)?;
    //}


    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    //fn member(id: u64, stake: u64) -> CommitteeMember {
    //    CommitteeMember {
    //        id: NodeId(id),
    //        stake: TAPE(stake),
    //        key: BlsPubkey::zeroed(),
    //    }
    //}
    //
    //#[test]
    //fn test_stake_with_node() {
    //    let signer = Pubkey::new_unique();
    //    let pool_address = Pubkey::new_unique();
    //    let amount: u64 = 1000;
    //
    //    let instruction = build_stake_with_pool_ix(signer, pool_address, amount.into());
    //
    //    let signer_ata = ata_address(&signer);
    //    let (system_address, _) = system_pda();
    //    let (epoch_address, _) = epoch_pda();
    //    let (stake_address, _) = stake_pda(signer, pool_address);
    //    let (vault_address, _) = vault_pda(stake_address);
    //
    //    // Setup existing accounts
    //
    //    let mut system = System::zeroed();
    //    let mut epoch = Epoch::zeroed();
    //    let mut node = Node::zeroed();
    //
    //    system.committee_prev = Committee::from_members(&[ member(2, 2_000), member(1, 1_000), ]);
    //    system.committee      = Committee::from_members(&[ member(3, 3_000), member(2, 2_000), member(1, 1_000), ]);
    //    system.committee_next = Committee::from_members(&[ member(3, 3_500), member(4, 2_100), member(2, 2_000), member(1, 1_000), ]);
    //
    //    epoch.id = EpochNumber(42);
    //
    //    let e0: EpochNumber = epoch.id;
    //    let e1: EpochNumber = e0 + EpochNumber(1);
    //    let e2: EpochNumber = e1 + EpochNumber(1);
    //
    //    node.id = NodeId(4);
    //    node.pool.stake = TAPE(5000);
    //    node.pool.schedule.incoming_tokens = EpochValues::try_from(
    //        &[e1, e2],
    //        &[1000, 200],
    //    ).expect("schedule incoming");
    //
    //    node.pool.schedule.outgoing_tokens = EpochValues::try_from(
    //        &[e1, e2],
    //        &[100, 50],
    //    ).expect("schedule outgoing");
    //
    //    assert_eq!(node.pool.stake_at(e0), TAPE(5000));
    //    assert_eq!(node.pool.stake_at(e1), TAPE(5900));
    //    assert_eq!(node.pool.stake_at(e2), TAPE(6050));
    //
    //    let initial_token_balance: u64 = 1_000_000_000;
    //
    //    let accounts = vec![
    //        sol(signer, 1_000_000_000),
    //        token(signer_ata, signer, initial_token_balance),
    //
    //        pda(system_address, system.pack(), tapedrive::ID),
    //        pda(epoch_address, epoch.pack(), tapedrive::ID),
    //        pda(pool_address, node.pack(), tapedrive::ID),
    //        empty(stake_address),
    //        empty(vault_address),
    //        mint(0),
    //
    //        token_program(),
    //        system_program(),
    //        staking_program(),
    //        rent_sysvar(),
    //    ];
    //
    //    let env = test_env();
    //    env.process_instruction(
    //        &instruction, 
    //        &accounts,
    //        &[
    //            Check::success(),
    //            Check::account(&system_address).data(
    //                System { 
    //                    committee_next: {
    //                        let mut committee = system.committee_next;
    //                        committee.update_stake(&node.id, TAPE(5900)).expect("update stake");
    //                        committee
    //                    },
    //                    ..system
    //                }.pack().as_ref()
    //            ).build(),
    //            Check::account(&stake_address).data(
    //                Stake {
    //                    authority: signer,
    //                    pool: pool_address,
    //                    inner: StakedTape {
    //                        amount: amount.into(),
    //                        activation_epoch: e2,
    //                        state: *StakeState::new().set_staked(),
    //                    },
    //                }.pack().as_ref()
    //            ).build(),
    //            Check::account(&pool_address).data(
    //                Node {
    //                    pool: StakingPool {
    //                        schedule: PoolSchedule {
    //                            incoming_tokens: EpochValues::try_from(
    //                                &[e1, e2],
    //                                &[1000, 200 + amount],
    //                            ).expect("schedule incoming"),
    //                            ..node.pool.schedule
    //                        },
    //                        ..node.pool
    //                    },
    //                    ..node
    //                }.pack().as_ref()
    //            ).build(),
    //            Check::account(&signer_ata).data(
    //                token(
    //                    signer_ata, 
    //                    signer, 
    //                    initial_token_balance - amount
    //                ).1.data.as_ref()
    //            ).build(),
    //            Check::account(&vault_address).data(
    //                token(
    //                    vault_address, 
    //                    vault_address, 
    //                    amount
    //                ).1.data.as_ref()
    //            ).build(),
    //        ]
    //    );
    //}
}
