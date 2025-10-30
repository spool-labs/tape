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

    // Signer does not need to be the pool authority

    signer_info
        .is_signer()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let archive = archive_info
        .is_archive()?
        .as_account::<Archive>(&tapedrive::ID)?;

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
    // rewards(e, n) = (weight[n] * (capacity_reserved[e] - blacklist_size[n])) / sum((capacity_reserved[e] - blacklist_size[n])) * fees_paid[e]
    // rewards(e, n) = (weight[n] * (capacity_reserved[e] - blacklist_size[n])) / sum(stored[n]) * fees_paid[e]

    let weight : u128 = system.committee_prev
        .index_of(&node.id)
        .map_or(0, |idx| system.seats.weight(idx))
        .into();

    let capacity_reserved = archive.recent_reserved;
    let fees_paid         = archive.recent_fees;
    let sum_stored        = archive.recent_stored;
    let blacklist_size    = node.blacklist.size.min(capacity_reserved);
    let node_stored       = capacity_reserved.saturating_sub(blacklist_size);

    let prev_rewards = weight
        .saturating_mul(node_stored.as_u128())
        .saturating_mul(fees_paid.as_u128())
        .checked_div(sum_stored.as_u128())
        .unwrap_or(0) as u64;

    // TODO: what if there is no prev rate? (new node)
    let prev_rate = node.history
        .rate_at(current_epoch(epoch))
        .ok_or(ProgramError::Custom(0))?;
    //    .ok_or(TapeError::MissingExchangeRate)?;

    let new_rate = node.pool.advance_epoch(
        current_epoch(epoch), 
        prev_rewards.into(), 
        prev_rate)
        .map_err(|_| ProgramError::Custom(1))?;

    node.history.push(current_epoch(epoch), new_rate);

    solana_program::msg!("total rewards {:?}", fees_paid);
    solana_program::msg!("prev rate rate: {:?}", prev_rate);
    solana_program::msg!("Advanced pool {}, earned: {}, new rate: {:?}", node.id.0, TAPE(prev_rewards), new_rate);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember {
            id: NodeId(id),
            stake: TAPE(stake),
            key: BlsPubkey::zeroed(),
            blacklist: StorageUnits(0),
        }
    }

    #[test]
    fn test_advance_pool() {

        let signer = Pubkey::new_unique();
        let pool_owner = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (pool_address, _)  = node_pda(pool_owner);

        let instruction = build_advance_pool_ix(signer, pool_address);

        let mut system = System::zeroed();
        let mut archive = Archive::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        system.committee_prev = Committee::from_members(&[ member(2, 2_000), member(1, 1_000), ]);
        system.committee      = Committee::from_members(&[ member(3, 3_000), member(2, 2_000), member(1, 1_000), ]);
        system.committee_next = Committee::from_members(&[ member(3, 3_500), member(4, 2_100), member(2, 2_000), member(1, 1_000), ]);

        archive.recent_reserved = StorageUnits(1_000);
        archive.recent_stored = StorageUnits((1_000 - 50) * 3);
        archive.recent_fees = TAPE(10_000);

        epoch.id = EpochNumber(42);

        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);

        let rate = ExchangeRate { tape: 1000, other: 9000 };
        node.id = NodeId(2);
        node.authority = pool_owner;
        node.history.push(EpochNumber(30), rate);
        node.pool.rewards = TAPE(500);
        node.pool.stake = TAPE(5000);
        node.pool.commission_rate = BasisPoints(500); // 5%
        node.pool.shares = rate.convert_to_other_amount(node.pool.stake.into());
        node.blacklist.size = StorageUnits(50);

        node.pool.schedule.incoming_tokens = EpochValues::try_from(
            &[e0, e2],
            &[1000, 200],
        ).expect("schedule incoming");

        node.pool.schedule.outgoing_tokens = EpochValues::try_from(
            &[e0, e2],
            &[100, 50],
        ).expect("schedule outgoing");

        assert_eq!(node.pool.stake_at(e0), TAPE(5900));
        assert_eq!(node.pool.stake_at(e1), TAPE(5900));
        assert_eq!(node.pool.stake_at(e2), TAPE(6050));

        let accounts = vec![
            sol(signer, 1_000_000_000),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(pool_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                //Check::account(&signer)
                //    .lamports(1_000_000_000 + rent_token() + rent(Stake::get_size()))
                //    .build(),
                //Check::account(&stake_address)
                //    .lamports(0)
                //    .closed()
                //    .build(),
                //Check::account(&vault_address)
                //    .lamports(0)
                //    .closed()
                //    .build(),
                //
                //// Signer gets principal tokens and vault gets closed, rent refunded
                //Check::account(&signer_ata).data(
                //    token(
                //        signer_ata,
                //        signer,
                //        principal + reward
                //    ).1.data.as_ref()
                //).build(),
                //
                //// Pool rewards reduced by owed_rewards (cap was exact)
                //Check::account(&pool_address).data(
                //    Node {
                //        pool: StakingPool {
                //            rewards: node.pool.rewards - TAPE(reward),
                //            ..node.pool
                //        },
                //        ..node
                //    }.pack().as_ref()
                //).build(),

            ],
        );
    }
}
