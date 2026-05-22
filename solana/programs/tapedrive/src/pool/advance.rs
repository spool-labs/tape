use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::PoolAdvanced;

pub fn process_advance_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvancePool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        archive_info,
        prev_epoch_info,
        prev_committee_info,
        pool_info,
        history_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let curr = system.current_epoch;
    let prev = curr.saturating_sub(EpochNumber(1));

    let archive = archive_info
        .is_writable()?
        .is_archive()?
        .as_account_mut::<Archive>(&tapedrive::ID)?;

    let prev_epoch = prev_epoch_info
        .is_epoch(prev)?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let pool_address: Address = (*pool_info.key).into();

    let node = pool_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    // Never drain a given prev epoch twice for the same node.
    if node.latest_advance_epoch >= prev {
        return Err(TapeError::AlreadyAdvanced.into());
    }

    prev_committee_info.is_committee(prev)?;
    let (prev_committee, prev_members) = 
        Committee::read(prev_committee_info, &tapedrive::ID)?;
    if prev_committee.epoch != prev {
        return Err(TapeError::BadEpochId.into());
    }

    let claim = prev_members
        .iter()
        .find(|m| m.node == pool_address)
        .map(|m| compute_member_share(m, prev_epoch.total_assigned, archive.rewards_pool))
        .transpose()?
        .unwrap_or_else(TAPE::zero);

    let next_paid = archive
        .rewards_paid
        .checked_add(claim)
        .ok_or(TapeError::RewardsOverflow)?;
    if next_paid > archive.rewards_pool {
        return Err(TapeError::RewardsOverflow.into());
    }

    archive.rewards_paid = next_paid;

    node.pool.advance_epoch(curr, claim)
        .map_err(|_| TapeError::PoolAccountingFailed)?;

    let new_rate = node.pool.get_current_rate();

    // Update the nodes history with the new exchange rate for this epoch.
    let (history_address, _) = history_pda(pool_address);
    history_info
        .is_writable()?
        .has_address(&history_address.into())?;

    let history = history_info.as_account_mut::<History>(&tapedrive::ID)?;
    history.inner.push(prev, new_rate);
    history.latest_epoch = prev;

    node.latest_advance_epoch = prev;

    PoolAdvanced {
        node: pool_address,
        epoch: prev,
    }.log();

    Ok(())
}

fn compute_member_share(
    member: &Member,
    total_assigned: StorageUnits,
    rewards_pool: Coin<TAPE>,
) -> Result<Coin<TAPE>, TapeError> {
    if total_assigned.is_zero() {
        return Ok(TAPE::zero());
    }

    let weight = member
        .assigned
        .checked_sub(member.blacklisted)
        .ok_or(TapeError::UnexpectedState)?;

    let raw = rewards_pool
        .as_u128()
        .checked_mul(weight.as_u128())
        .ok_or(TapeError::RewardsOverflow)?
        / total_assigned.as_u128();

    if raw > u64::MAX as u128 {
        return Err(TapeError::RewardsOverflow);
    }

    Ok(TAPE(raw as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::Committee;
    use tape_core::staking::StakingPool;
    use tape_test::*;

    const COMMITTEE_SIZE: u64 = 128;
    const POOL_SCHEDULE_SIZE: usize = 4;

    fn make_test_pool(stake: u64) -> StakingPool<POOL_SCHEDULE_SIZE> {
        let mut pool = StakingPool::<POOL_SCHEDULE_SIZE>::new(BasisPoints(1000)); // 10% commission
        pool.stake = TAPE(stake);
        pool.shares = ShareAmount(stake);
        pool
    }

    fn pack_committee(
        epoch: EpochNumber,
        node: Address,
        assigned: StorageUnits,
        blacklisted: StorageUnits,
    ) -> Vec<u8> {
        let members = [Member {
            node,
            stake: TAPE(1_000),
            assigned,
            blacklisted,
            spools: 0,
        }];
        Committee { epoch, members: Tail::new(COMMITTEE_SIZE, members.len() as u64) }
            .pack_with(&members)
    }

    #[test]
    fn advance() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (prev_epoch_address, _) = epoch_pda(prev);
        let (prev_committee_address, _) = committee_pda(prev);
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        let archive = Archive {
            rewards_pool: TAPE(1_000),
            rewards_paid: TAPE::zero(),
            ..Archive::zeroed()
        };
        let prev_epoch_data = Epoch {
            id: prev,
            total_assigned: StorageUnits::mb(1_000),
            ..Epoch::zeroed()
        };

        let node = Node {
            authority: authority.into(),
            latest_advance_epoch: prev.saturating_sub(EpochNumber(1)), // at-the-edge
            pool: make_test_pool(1_000),
            ..Node::zeroed()
        };

        let mut history = History::zeroed();
        history.node = node_address;

        let instruction = build_advance_pool_ix(fee_payer.into(), node_address, curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(prev_epoch_address, prev_epoch_data.pack(), tapedrive::ID),
            pda(
                prev_committee_address,
                pack_committee(
                    prev,
                    node_address,
                    StorageUnits::mb(300),
                    StorageUnits::zero(),
                ),
                tapedrive::ID,
            ),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // 300 / 1000 * 1000 = 300, with 10% commission.
        let mut expected_node = node;
        expected_node.latest_advance_epoch = prev;
        expected_node.pool.stake = TAPE(1_270);
        expected_node.pool.rewards = TAPE(270);
        expected_node.pool.commission = TAPE(30);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(archive_address))
                    .data(Archive {
                        rewards_paid: TAPE(300),
                        ..archive
                    }.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(node_address))
                    .data(expected_node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn blacklisted_weight_is_unpaid() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (prev_epoch_address, _) = epoch_pda(prev);
        let (prev_committee_address, _) = committee_pda(prev);
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        let archive = Archive {
            rewards_pool: TAPE(1_000),
            rewards_paid: TAPE::zero(),
            ..Archive::zeroed()
        };
        let prev_epoch_data = Epoch {
            id: prev,
            total_assigned: StorageUnits::mb(1_000),
            ..Epoch::zeroed()
        };
        let node = Node {
            authority: authority.into(),
            latest_advance_epoch: prev.saturating_sub(EpochNumber(1)),
            pool: make_test_pool(1_000),
            ..Node::zeroed()
        };

        let mut history = History::zeroed();
        history.node = node_address;

        let instruction = build_advance_pool_ix(fee_payer.into(), node_address, curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(prev_epoch_address, prev_epoch_data.pack(), tapedrive::ID),
            pda(
                prev_committee_address,
                pack_committee(
                    prev,
                    node_address,
                    StorageUnits::mb(300),
                    StorageUnits::mb(100),
                ),
                tapedrive::ID,
            ),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let mut expected_node = node;
        expected_node.latest_advance_epoch = prev;
        expected_node.pool.stake = TAPE(1_180);
        expected_node.pool.rewards = TAPE(180);
        expected_node.pool.commission = TAPE(20);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(archive_address))
                    .data(Archive {
                        rewards_paid: TAPE(200),
                        ..archive
                    }.pack().as_ref())
                    .build(),
                Check::account(&Pubkey::from(node_address))
                    .data(expected_node.pack().as_ref())
                    .build(),
            ],
        );
    }

    #[test]
    fn accepts_partial_when_behind() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (prev_epoch_address, _) = epoch_pda(prev);
        let (prev_committee_address, _) = committee_pda(prev);
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        let archive = Archive {
            rewards_pool: TAPE(1_000),
            rewards_paid: TAPE::zero(),
            ..Archive::zeroed()
        };
        let prev_epoch_data = Epoch {
            id: prev,
            total_assigned: StorageUnits::mb(1_000),
            ..Epoch::zeroed()
        };
        let node = Node {
            authority: authority.into(),
            latest_advance_epoch: EpochNumber(5),
            pool: make_test_pool(1_000),
            ..Node::zeroed()
        };

        let mut history = History::zeroed();
        history.node = node_address;

        let instruction = build_advance_pool_ix(fee_payer.into(), node_address, curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(prev_epoch_address, prev_epoch_data.pack(), tapedrive::ID),
            pda(
                prev_committee_address,
                pack_committee(
                    prev,
                    node_address,
                    StorageUnits::mb(300),
                    StorageUnits::zero(),
                ),
                tapedrive::ID,
            ),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let mut expected_node = node;
        expected_node.latest_advance_epoch = prev;
        expected_node.pool.stake = TAPE(1_270);
        expected_node.pool.rewards = TAPE(270);
        expected_node.pool.commission = TAPE(30);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node_address))
                    .data(expected_node.pack().as_ref())
                    .build(),
            ],
        );
    }
}
