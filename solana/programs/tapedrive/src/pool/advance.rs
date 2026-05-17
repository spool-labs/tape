//! `AdvancePool` handler.
//!
//! Drains the rewards accumulated by `SettleSpool` into the pool's commission
//! + stake, snapshots the new exchange rate to the node's history, and
//! resets the pending accumulator. Permissionless: anyone can pay the fee to
//! advance a node's pool.
//!
//! Grief gate: AdvancePool can be called for any prev epoch the node hasn't
//! drained yet. We split that into two cases:
//!
//! - **At-the-edge** (`latest_advance_epoch + 1 == prev`): one epoch behind
//!   the last successful drain. Settles for `prev` may still land any time
//!   during the current epoch, so we require all of this node's prev-epoch
//!   spools to be settled (`pending_settled == k`) before draining.
//!   Otherwise an early drain could split a single epoch's rewards across
//!   two exchange-rate snapshots and corrupt staker accounting.
//!
//! - **Catching up** (`latest_advance_epoch + 1 < prev`): more than one epoch
//!   has passed since the last drain. The prev shifts forward each epoch
//!   boundary, so settles for stale prev epochs are no longer addressable —
//!   any partial pending state is permanent loss. Drain whatever's there
//!   and bump `latest_advance_epoch` so the node isn't stuck.

use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::PoolAdvanced;

pub fn process_advance_pool(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvancePool::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
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

    let pool_address: Address = (*pool_info.key).into();

    let node = pool_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    // Never drain a given prev epoch twice for the same node.
    if node.latest_advance_epoch >= prev {
        return Err(TapeError::AlreadyAdvanced.into());
    }

    // K = the node's prev-epoch spool count. Linear scan of Committee(prev);
    // a node absent from prev's committee has K = 0.
    prev_committee_info.is_committee(prev)?;
    let (_, prev_members) = Committee::read(prev_committee_info, &tapedrive::ID)?;
    let k = prev_members
        .iter()
        .find(|m| m.node == pool_address)
        .map(|m| m.spools)
        .unwrap_or(0);

    // If we're at-the-edge, require all K spools to be settled before draining. Otherwise, allow
    // partial settles to be drained when the node is behind multiple epochs, since those unsettled
    // rewards are effectively lost.

    let at_edge = node.latest_advance_epoch
        .saturating_add(EpochNumber(1)) == prev;
    if at_edge && node.pool.pending_settled != k {
        return Err(TapeError::SpoolsNotSettled.into());
    }

    // Drain the pool's pending rewards into the stake and commission, and reset the pending state
    node.pool.advance_epoch(curr)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::Committee;
    use tape_core::staking::StakingPool;
    use tape_test::*;

    const COMMITTEE_SIZE: u64 = 128;
    const POOL_SCHEDULE_SIZE: usize = 4;

    fn make_test_pool(stake: u64, pending: u64, settled: u64) -> StakingPool<POOL_SCHEDULE_SIZE> {
        let mut pool = StakingPool::<POOL_SCHEDULE_SIZE>::new(BasisPoints(1000)); // 10% commission
        pool.stake = TAPE(stake);
        pool.shares = ShareAmount(stake);
        pool.pending_rewards = TAPE(pending);
        pool.pending_settled = settled;
        pool
    }

    fn pack_committee(epoch: EpochNumber, node: Address, spools: u64) -> Vec<u8> {
        let members = [Member {
            node,
            stake: TAPE(1_000),
            blacklist: StorageUnits::zero(),
            spools,
        }];
        Committee { epoch, members: Tail::new(COMMITTEE_SIZE, members.len() as u64) }
            .pack_with(&members)
    }

    // Happy path: at-the-edge with all spools settled → drain succeeds.
    #[test]
    fn advance() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);

        let (system_address, _) = system_pda();
        let (prev_committee_address, _) = committee_pda(prev);
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        // Node held 3 spools in prev, all 3 settled.
        let k = 3u64;
        let node = Node {
            authority: authority.into(),
            latest_advance_epoch: prev.saturating_sub(EpochNumber(1)), // at-the-edge
            pool: make_test_pool(1_000, 300, k),
            ..Node::zeroed()
        };

        let mut history = History::zeroed();
        history.node = node_address;

        let instruction = build_advance_pool_ix(fee_payer.into(), node_address, curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(prev_committee_address, pack_committee(prev, node_address, k), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // 10% commission on 300 → 30 commission, 270 to stake.
        let mut expected_node = node;
        expected_node.latest_advance_epoch = prev;
        expected_node.pool.stake = TAPE(1_270);
        expected_node.pool.rewards = TAPE(270);
        expected_node.pool.commission = TAPE(30);
        expected_node.pool.pending_rewards = TAPE::zero();
        expected_node.pool.pending_settled = 0;

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

    // Grief prevention: at-the-edge with partial settles -> reject.
    #[test]
    fn rejects_partial_at_edge() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);

        let (system_address, _) = system_pda();
        let (prev_committee_address, _) = committee_pda(prev);
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        // Node holds 5 spools, only 3 settled so far. At-the-edge.
        let k = 5u64;
        let node = Node {
            authority: authority.into(),
            latest_advance_epoch: prev.saturating_sub(EpochNumber(1)),
            pool: make_test_pool(1_000, 300, 3),
            ..Node::zeroed()
        };

        let mut history = History::zeroed();
        history.node = node_address;

        let instruction = build_advance_pool_ix(fee_payer.into(), node_address, curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(prev_committee_address, pack_committee(prev, node_address, k), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::SpoolsNotSettled.into())],
        );
    }

    #[test]
    fn accepts_partial_when_behind() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let curr = EpochNumber(10);
        let prev = EpochNumber(9);

        let (system_address, _) = system_pda();
        let (prev_committee_address, _) = committee_pda(prev);
        let (node_address, _) = node_pda(authority.into());
        let (history_address, _) = history_pda(node_address);

        let system = System {
            current_epoch: curr,
            committee_size: COMMITTEE_SIZE,
            ..System::zeroed()
        };

        // Node holds 5 spools in prev, only 3 settled. But latest_advance_epoch
        // is far behind (epoch 5) grief gate skipped.
        let k = 5u64;
        let node = Node {
            authority: authority.into(),
            latest_advance_epoch: EpochNumber(5),
            pool: make_test_pool(1_000, 300, 3),
            ..Node::zeroed()
        };

        let mut history = History::zeroed();
        history.node = node_address;

        let instruction = build_advance_pool_ix(fee_payer.into(), node_address, curr);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(prev_committee_address, pack_committee(prev, node_address, k), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(history_address, history.pack(), tapedrive::ID),
        ];

        // Drain proceeds with the partial 300 TAPE pending.
        let mut expected_node = node;
        expected_node.latest_advance_epoch = prev;
        expected_node.pool.stake = TAPE(1_270);
        expected_node.pool.rewards = TAPE(270);
        expected_node.pool.commission = TAPE(30);
        expected_node.pool.pending_rewards = TAPE::zero();
        expected_node.pool.pending_settled = 0;

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
