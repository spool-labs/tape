use crate::types::*;
use bytemuck::{Pod, Zeroable};

use super::schedule::*;
use super::state::*;
use crate::system::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    ScheduleFailed,
    PoolInactive,
    BalanceExceeded,
    EpochInvalid,
    StakeInvalid,
    StakeActive,
    StakeNotActive,
    ZeroShares,
    ZeroStake,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StakingPool<const N: usize> {
    /// The total number of shares issued by this pool.
    pub shares: ShareAmount,

    /// The total stake held by this pool (excluding commission).
    pub stake: Coin<TAPE>,

    /// The rewards this pool has earned from being active and available to distribute to stakers
    pub rewards: Coin<TAPE>,

    /// The commission earned by the pool operator, available for withdrawal.
    pub commission: Coin<TAPE>,

    /// The current commission rate.
    pub commission_rate: BasisPoints,

    /// All scheduled/pending changes tied to this pool.
    pub schedule: PoolSchedule<N>,
}

unsafe impl<const N: usize> Zeroable for StakingPool<N> {}
unsafe impl<const N: usize> Pod for StakingPool<N> {}

impl<const N: usize> StakingPool<N> {
    pub fn new(commission_rate: BasisPoints) -> Self {
        Self {
            shares: ShareAmount::zero(),
            stake: Coin::<TAPE>::zero(),
            rewards: Coin::<TAPE>::zero(),
            commission: Coin::<TAPE>::zero(),
            commission_rate,
            schedule: PoolSchedule::new(),
        }
    }

    /// Get the current exchange rate (stake per share).
    pub fn get_current_rate(&self) -> ExchangeRate {
        // Ensure both stake and shares are zero or non-zero, any other state is invalid.
        debug_assert!(self.shares.is_zero() == self.stake.is_zero());

        if self.shares.is_zero() {
            ExchangeRate::flat()
        } else {
            ExchangeRate::new(
                self.stake.into(),
                self.shares.into()
            )
        }
    }

    /// Project the stake at a future epoch, valid for Next N epochs 
    /// (uses current exchange rate snapshot).
    pub fn calculate_stake_at(&self, epoch: EpochNumber) -> Coin<TAPE> {

        // Calculate current exchange rate (stake per share)
        let exchange_rate = self.get_current_rate();

        // Calculate net token additions (incoming - outgoing)
        let incoming = self.schedule.stake_sum(epoch);
        let outgoing = self.schedule.cancel_sum(epoch);
        let net_additions = incoming.saturating_sub(outgoing);

        // Convert outgoing shares to token amount
        let outgoing_shares = self.schedule.unstake_sum(epoch);
        let outgoing_tokens = exchange_rate
            .convert_to_tape_amount(outgoing_shares.into())
            .into();

        // Compute final stake: current stake + net additions - outgoing tokens
        self.stake
            .saturating_add(net_additions)
            .saturating_sub(outgoing_tokens)
    }

    /// Add rewards for previous epoch, apply commission, and process pending I/O.
    /// Caller passes the previous exchange rate and receives the new rate for this epoch.
    pub fn advance_epoch(
        &mut self,
        current_epoch: EpochNumber,
        rewards_earned: Coin<TAPE>,
    ) -> Result<(), PoolError> {

        // Apply scheduled commission changes
        if let Some(commission_rate) = self.schedule.take_commission_change(current_epoch) {
            self.commission_rate = commission_rate;
        }

        // Split rewards into commission and net, then add to pool stake
        if rewards_earned > TAPE::zero() {
            if self.stake.is_zero() {
                return Err(PoolError::StakeInvalid);
            }

            let commission_cut = (
                rewards_earned.as_u128() * self.commission_rate.as_u128() / BasisPoints::MAX as u128
            ) as u64;

            let rewards_net = rewards_earned.saturating_sub(commission_cut.into());

            self.commission = self.commission.saturating_add(commission_cut.into());
            self.rewards = self.rewards.saturating_add(rewards_net);
            self.stake = self.stake.saturating_add(rewards_net);
        }

        let shapshot = self.get_current_rate();

        // Handle stake increases (due to pending stake additions)
        self.process_scheduled_additions(current_epoch, shapshot)?;

        // Handle stake reductions (due to pending share withdrawals)
        self.process_scheduled_reductions(current_epoch, shapshot)?;

        Ok(())
    }

    /// Stake tokens with this pool (schedules activation at E+2).
    pub fn stake_with_pool(
        &mut self,
        current_epoch: EpochNumber,
        stake_amount: Coin<TAPE>,
    ) -> Result<StakedTape, PoolError> {
        if stake_amount.is_zero() {
            return Err(PoolError::ZeroStake);
        }

        let activation_epoch = current_epoch + EpochNumber(2);
        self.schedule
            .stake(activation_epoch, stake_amount)
            .map_err(|_| PoolError::ScheduleFailed)?;

        Ok(StakedTape {
            activation_epoch,
            amount: stake_amount,
            state: StakeState::new(),
        })
    }

    /// Request a stake cancel from this pool.
    /// Caller provides the activation exchange rate for this stake.
    pub fn request_cancel(
        &mut self,
        stake: &mut StakedTape,
        current_epoch: EpochNumber,
    ) -> Result<EpochNumber, PoolError> {

        if !stake.is_staked() {
            return Err(PoolError::StakeInvalid);
        }

        if stake.amount.is_zero() {
            return Err(PoolError::StakeInvalid);
        }

        // If the stake is already active, cannot cancel
        if stake.activation_epoch <= current_epoch {
            return Err(PoolError::StakeActive);
        }

        let withdraw_epoch = stake.activation_epoch;

        // Schedule the stake principal to be canceled at activation_epoch.
        // The net result is 0 stake change at that epoch.
        self.schedule
            .cancel(withdraw_epoch, stake.amount)
            .map_err(|_| PoolError::ScheduleFailed)?;

        stake.set_withdrawing(withdraw_epoch);

        Ok(withdraw_epoch)
    }

    /// Request a withdrawal of stake from this pool. 
    /// (Caller provides the activation exchange rate for this stake)
    pub fn request_withdraw(
        &mut self,
        stake: &mut StakedTape,
        current_epoch: EpochNumber,
        stake_activation_rate: ExchangeRate,
    ) -> Result<EpochNumber, PoolError> {
        if !stake.is_staked() {
            return Err(PoolError::StakeInvalid);
        }

        if stake.amount.is_zero() {
            return Err(PoolError::StakeInvalid);
        }

        // If the stake is not yet active, the stake cannot be withdrawn, it must be canceled.
        if stake.activation_epoch > current_epoch {
            return Err(PoolError::StakeNotActive);
        }

        // This is an active stake withdraw, so we need to schedule a share removal
        // which would calculate rewards at withdraw time.

        let withdraw_epoch = current_epoch + EpochNumber(2);

        stake.set_withdrawing(withdraw_epoch);

        // Calculate the shares corresponding to this stake at activation rate
        let shares : ShareAmount = stake_activation_rate
            .convert_to_other_amount(stake.amount.into())
            .into();

        if shares.is_zero() {
            return Err(PoolError::ZeroShares);
        }

        self.schedule
            .unstake(withdraw_epoch, shares)
            .map_err(|_| PoolError::ScheduleFailed)?;

        Ok(withdraw_epoch)
    }

    /// Withdrawn stake (capped by pool available rewards).
    /// Caller computes owed amount externally (e.g., via history or snapshot math).
    pub fn unstake_from_pool(
        &mut self,
        stake: &mut StakedTape,
        current_epoch: EpochNumber,
        owed_rewards: Coin<TAPE>,
    ) -> Result<Coin<TAPE>, PoolError> {
        if !stake.is_withdrawing() {
            return Err(PoolError::StakeInvalid);
        }

        let stake_withdraw_epoch = stake
            .state
            .withdraw_epoch()
            .ok_or(PoolError::StakeInvalid)?;

        if stake_withdraw_epoch > current_epoch {
            return Err(PoolError::EpochInvalid);
        }

        if stake.amount.is_zero() {
            return Err(PoolError::StakeInvalid);
        }

        // If the withdraw epoch is before or at activation, then no rewards are due.
        if stake_withdraw_epoch <= stake.activation_epoch {
            return Ok(TAPE::zero());
        }

        // Cap by available rewards
        let pay = if owed_rewards > self.rewards {
            self.rewards
        } else {
            owed_rewards
        };

        self.rewards = self.rewards
            .saturating_sub(pay);

        stake.set_withdrawn();

        Ok(pay)
    }

    /// Process pending stake additions and pre-active cancellations for the current_epoch.
    fn process_scheduled_additions(
        &mut self, current_epoch: EpochNumber,
        shapshot_rate: ExchangeRate,
        ) -> Result<(), PoolError> {

        // Sum all pending stake before or at current_epoch
        let incoming = self.schedule
            .take_incoming(current_epoch);

        // Sum all pre-active cancellations before or at current_epoch
        let outgoing = self.schedule
            .take_outgoing(current_epoch);

        // Net pending stake must be non-negative
        if outgoing > incoming {
            return Err(PoolError::BalanceExceeded);
        }

        // Increase stake by net added stake
        let net_added = incoming - outgoing;
        if net_added > TAPE::zero() {

            let incoming_shares = shapshot_rate
                .convert_to_other_amount(net_added.into())
                .into();
            
            self.shares = self.shares.saturating_add(incoming_shares);
            self.stake = self.stake.saturating_add(net_added.into());
        }

        Ok(())
    }

    /// Process pending share withdrawals for the current_epoch.
    fn process_scheduled_reductions(
        &mut self,
        current_epoch: EpochNumber,
        shapshot_rate: ExchangeRate,
    ) -> Result<(), PoolError> {

        // Sum all pending shares withdrawing before or at current_epoch
        let outgoing_shares = self.schedule
            .take_outgoing_shares(current_epoch);

        if outgoing_shares > self.shares {
            return Err(PoolError::BalanceExceeded);
        }

        // Convert shares to tape at the current price
        let net_removed = shapshot_rate
            .convert_to_tape_amount(outgoing_shares.into())
            .into();

        if self.stake < net_removed {
            return Err(PoolError::BalanceExceeded);
        }

        if outgoing_shares > ShareAmount::zero() {
            self.shares = self.shares
                .saturating_sub(outgoing_shares);

            self.stake = self.stake
                .saturating_sub(net_removed);
        }

        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    type TestPool = StakingPool<4>;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }
    fn shares(v: u64) -> ShareAmount { ShareAmount(v) }

    #[test]
    fn new_ok() {
        let p = TestPool::new(BasisPoints(1000));

        assert_eq!(p.stake, TAPE::zero());
        assert_eq!(p.shares, shares(0));
        assert_eq!(p.commission_rate, BasisPoints(1000));
    }

    #[test]
    fn stake_sched() {
        let mut p = TestPool::new(BasisPoints(0));
        let s = p.stake_with_pool(epoch(5), tape(700)).unwrap();

        assert_eq!(s.activation_epoch, epoch(7));
        assert_eq!(p.schedule.stake_sum(epoch(6)), tape(0));
        assert_eq!(p.schedule.stake_sum(epoch(7)), tape(700));
    }

    #[test]
    fn advance_commission() {
        let mut p = TestPool::new(BasisPoints(1000)); // 10%

        // Activate 1_000 at E1 (raw insert for immediate activation)
        p.schedule.stake(epoch(1), tape(1_000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        assert_eq!(p.stake, tape(1_000));

        // Add 500 gross at E2 → 10% commission, 450 net
        p.advance_epoch(epoch(2), tape(500)).unwrap();
        assert_eq!(p.commission, tape(50));
        assert_eq!(p.rewards, tape(450));
        assert_eq!(p.stake, tape(1_450));
    }

    #[test]
    fn adv_no_stake_err() {
        let mut p = TestPool::new(BasisPoints(0));

        let err = p.advance_epoch(epoch(1), tape(10)).unwrap_err();
        assert!(matches!(err, PoolError::StakeInvalid));
    }

    #[test]
    fn set_comm_next() {
        let mut p = TestPool::new(BasisPoints(1000));

        p.schedule.stake(epoch(1), tape(100)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();

        p.schedule.set_commission(epoch(4), BasisPoints(2000)).unwrap(); // applies at E4

        p.advance_epoch(epoch(3), tape(0)).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(1000));

        p.advance_epoch(epoch(4), tape(0)).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(2000));
    }

    #[test]
    fn process_pending() {
        let mut p = TestPool::new(BasisPoints(0));

        p.schedule.stake(epoch(1), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();

        let r1 = p.get_current_rate();

        assert_eq!(p.stake, tape(1000));
        assert_eq!(p.shares, r1.convert_to_other_amount(p.stake.into()).into());
    }

    #[test]
    fn balance_calc() {
        let mut p = TestPool::new(BasisPoints(0));

        p.schedule.stake(epoch(1), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // balance=1000

        // Schedule more stake for E5 and a withdraw at E6
        p.schedule.stake(epoch(5), tape(600)).unwrap();
        p.schedule.unstake(epoch(6), shares(200)).unwrap();

        // Projection uses current rate (flat 1:1 here)
        assert_eq!(p.calculate_stake_at(epoch(4)), tape(1000));
        assert_eq!(p.calculate_stake_at(epoch(5)), tape(1600));
        assert_eq!(p.calculate_stake_at(epoch(6)), tape(1400));
    }

    #[test]
    fn exceed_pre_active_err() {
        let mut p = TestPool::new(BasisPoints(0));

        p.schedule.stake(epoch(3), tape(100)).unwrap();
        p.schedule.cancel(epoch(3), tape(200)).unwrap();

        let err = p.advance_epoch(epoch(3), tape(0)).unwrap_err();
        assert!(matches!(err, PoolError::BalanceExceeded));
    }

    #[test]
    fn exceed_shares_err() {
        let mut p = TestPool::new(BasisPoints(0));

        p.schedule.stake(epoch(1), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();

        p.schedule.unstake(epoch(2), shares(1500)).unwrap();
        let err = p.advance_epoch(epoch(2), tape(0)).unwrap_err();

        assert!(matches!(err, PoolError::BalanceExceeded));
    }

    #[test]
    fn withdraw_pre_active() {
        let mut p = TestPool::new(BasisPoints(0));

        // Create a pre-active stake at current=5 → activation=7
        let mut s = p.stake_with_pool(epoch(5), tape(500)).unwrap();

        // Since activation is in the future, we should get an error trying to withdraw
        let err = p.request_withdraw(&mut s, epoch(5), ExchangeRate::flat()).unwrap_err();
        assert!(matches!(err, PoolError::StakeNotActive));

        // Instead, request a cancel
        let we = p.request_cancel(&mut s, epoch(5)).unwrap();

        assert_eq!(we, epoch(7)); // current(5)+2
        assert_eq!(p.schedule.cancel_sum(epoch(7)), tape(500));
        assert_eq!(p.schedule.unstake_sum(epoch(7)), shares(0));
    }

    #[test]
    fn withdraw_active() {
        let mut p = TestPool::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Stake at E1 → activation E3
        let mut s = p.stake_with_pool(epoch(1), tape(1000)).unwrap();

        // Advance epochs so activation snapshot exists and stake is active
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();

        // Request at E3 → withdraw at E5; shares computed from rate at activation (E3)
        let activation_rate = flat;
        let we = p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        assert_eq!(we, epoch(5));
        assert_eq!(p.schedule.unstake_sum(epoch(5)), shares(1000));
    }

    #[test]
    fn withdraw_pre_no_rewards() {
        let mut p = TestPool::new(BasisPoints(0));

        // Pre-active: stake at E5 → activation E7
        let mut s = p.stake_with_pool(epoch(5), tape(500)).unwrap();
        p.request_cancel(&mut s, epoch(5)).unwrap(); // withdraw E7

        // Walk epochs
        p.advance_epoch(epoch(6), tape(0)).unwrap();
        p.advance_epoch(epoch(7), tape(0)).unwrap();

        // No rewards owed for pre-active cancel
        let paid = p.unstake_from_pool(&mut s, epoch(8), tape(0)).unwrap();

        assert_eq!(paid, tape(0));
    }

    #[test]
    fn rate_change_and_withdraw() {
        let mut p = TestPool::new(BasisPoints(0));

        // Seed the pool so rewards can be earned.
        p.schedule.stake(epoch(0), tape(100)).unwrap();

        // Create a user stake at current=E1 → activation=E3 (E+2)
        let mut s = p.stake_with_pool(epoch(1), tape(100)).unwrap();

        // E1: activate seed
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        let r1 = p.get_current_rate();
        assert_eq!(r1, ExchangeRate { tape: 100, other: 100 });

        // E2: rewards 10
        p.advance_epoch(epoch(2), tape(10)).unwrap();
        let r2 = p.get_current_rate();
        assert_eq!(r2, ExchangeRate { tape: 110, other: 100 });

        // E3: rewards 10, user's 100 activates → shares mint at snapshot (120/100)
        p.advance_epoch(epoch(3), tape(10)).unwrap();
        let r3 = p.get_current_rate();
        assert_eq!(r3, ExchangeRate { tape: 220, other: 183 });

        // Request at E3 → withdraw epoch = E5 (E+2)
        let activation_rate = r3; // rate at activation E3
        p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        // E4: rewards 30 (user accrues E4 rewards)
        p.advance_epoch(epoch(4), tape(30)).unwrap();
        let r4 = p.get_current_rate();
        assert_eq!(r4, ExchangeRate { tape: 250, other: 183 });

        // E5: rewards 30, then process share withdrawal (burn)
        p.advance_epoch(epoch(5), tape(30)).unwrap();
        let r5 = p.get_current_rate();

        // After burning 83 shares at the E5 snapshot, stake and shares should be:
        // stake: 154, shares: 100
        assert_eq!(r5, ExchangeRate { tape: 154, other: 100 });
        assert_eq!(p.stake, tape(154));
        assert_eq!(p.shares, shares(100));

        // Compute owed rewards for the user (only E4 rewards):
        // shares_user = floor(100 * 183 / 220) = 83
        // tape_at_r4 = floor(83 * 250 / 183) = 113
        // owed = 113 - 100 = 13

        let user_shares: ShareAmount = r3
            .convert_to_other_amount(s.amount.into())
            .into();
        let tape_at_r4: Coin<TAPE> = r4
            .convert_to_tape_amount(user_shares.into())
            .into();
        let owed = tape_at_r4.saturating_sub(s.amount);

        let paid = p.unstake_from_pool(&mut s, epoch(5), owed).unwrap();
        assert_eq!(paid, owed);

        // Rewards pool started at 0 and got +10 (E2) +10 (E3) +30 (E4) +30 (E5) = 80,
        // then we paid 13 to the user.
        assert_eq!(p.rewards, tape(67));
    }

    #[test]
    fn rewards_only() {
        let mut p = TestPool::new(BasisPoints(0));

        // Seed and activate 100 at E1
        p.schedule.stake(epoch(0), tape(100)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        assert_eq!(p.stake, tape(100));
        assert_eq!(p.shares, shares(100));

        // E2 +50 rewards; shares unchanged
        p.advance_epoch(epoch(2), tape(50)).unwrap();
        assert_eq!(p.stake, tape(150));
        assert_eq!(p.shares, shares(100));
        assert_eq!(p.rewards, tape(50));

        // E3 +25 rewards; shares unchanged
        p.advance_epoch(epoch(3), tape(25)).unwrap();
        assert_eq!(p.stake, tape(175));
        assert_eq!(p.shares, shares(100));
        assert_eq!(p.rewards, tape(75));

        // Rate follows stake/shares
        let r3 = p.get_current_rate();
        assert_eq!(r3, ExchangeRate { tape: 175, other: 100 });
    }

     #[test]
    fn pre_active_cancel_no_shares() {
        let mut p = TestPool::new(BasisPoints(0));

        // User stakes at E1 -> activation E3
        let mut s = p.stake_with_pool(epoch(1), tape(200)).unwrap();

        // Before activation, cancel the stake (withdraw_epoch=E3)
        let we = p.request_cancel(&mut s, epoch(1)).unwrap();
        assert_eq!(we, epoch(3));

        // Walk epochs; since it was pre-active, no shares should be minted ever
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();

        assert_eq!(p.shares, shares(0));
        assert_eq!(p.stake, tape(0));
        assert_eq!(p.rewards, tape(0));

        // No rewards owed for pre-active cancel path
        let paid = p.unstake_from_pool(&mut s, epoch(3), tape(0)).unwrap();
        assert_eq!(paid, tape(0));
    }

    #[test]
    fn two_users_fair_split() {
        let mut p = TestPool::new(BasisPoints(0));

        // Seed pool so rewards can be earned
        p.schedule.stake(epoch(0), tape(100)).unwrap();

        // Alice stakes at E1 -> activation E3
        let mut a = p.stake_with_pool(epoch(1), tape(100)).unwrap();

        p.advance_epoch(epoch(1), tape(0)).unwrap();   // E1
        p.advance_epoch(epoch(2), tape(60)).unwrap();  // E2: rewards 60, A not yet active
        p.advance_epoch(epoch(3), tape(40)).unwrap();  // E3: rewards 40, A activates this epoch

        let r3 = p.get_current_rate();

        // Bob stakes at E3 -> activation E5
        let mut b = p.stake_with_pool(epoch(3), tape(100)).unwrap();

        // Alice withdraws at E3 → E5
        p.request_withdraw(&mut a, epoch(3), r3).unwrap();

        // E4: rewards 50 (A accrues this epoch; Bob not yet active)
        p.advance_epoch(epoch(4), tape(50)).unwrap();
        let r4 = p.get_current_rate();

        // E5: rewards 0; process Alices withdrawal
        p.advance_epoch(epoch(5), tape(0)).unwrap();
        let r5 = p.get_current_rate();

        // Compute A owed from activation rate r3 to r4 only
        let a_shares: ShareAmount = r3.convert_to_other_amount(a.amount.into()).into();
        let a_tape_at_r4: Coin<TAPE> = r4.convert_to_tape_amount(a_shares.into()).into();
        let a_owed = a_tape_at_r4.saturating_sub(a.amount);
        let paid_a = p.unstake_from_pool(&mut a, epoch(5), a_owed).unwrap();
        assert_eq!(paid_a, a_owed);

        // Now Bob activates at E5; schedule Bob withdraw at E5 → E7
        let r5_activation = r5;
        p.request_withdraw(&mut b, epoch(5), r5_activation).unwrap();

        // E6: add rewards 30 (Bob accrues this epoch)
        p.advance_epoch(epoch(6), tape(30)).unwrap();
        let r6 = p.get_current_rate();

        // E7: process Bob withdrawal
        p.advance_epoch(epoch(7), tape(0)).unwrap();

        // Compute Bob owed from r5 to r6 only
        let b_shares: ShareAmount = r5_activation.convert_to_other_amount(b.amount.into()).into();
        let b_tape_at_r6: Coin<TAPE> = r6.convert_to_tape_amount(b_shares.into()).into();
        let b_owed = b_tape_at_r6.saturating_sub(b.amount);
        let paid_b = p.unstake_from_pool(&mut b, epoch(7), b_owed).unwrap();

        assert_eq!(paid_b, b_owed);

        // No overpayment: paid <= total pool rewards accumulated
        // Total rewards collected by pool were 60 + 40 + 50 + 30 = 180; ensure paid_a + paid_b <= 180
        assert!(paid_a.as_u64() + paid_b.as_u64() <= 180);
    }

    #[test]
    fn withdraw_and_cap() {
        let mut p = TestPool::new(BasisPoints(0));

        // Seed the pool so rewards can be earned.
        p.schedule.stake(epoch(0), tape(100)).unwrap();

        // Create a user stake at current=E1 → activation=E3 (E+2)
        let mut s = p.stake_with_pool(epoch(1), tape(100)).unwrap();

        p.advance_epoch(epoch(1), tape(0)).unwrap();
        let r1 = p.get_current_rate();
        assert_eq!(r1, ExchangeRate { tape: 100, other: 100 });

        p.advance_epoch(epoch(2), tape(10)).unwrap();
        let r2 = p.get_current_rate();
        assert_eq!(r2, ExchangeRate { tape: 110, other: 100 });

        p.advance_epoch(epoch(3), tape(10)).unwrap();
        let r3 = p.get_current_rate();
        assert_eq!(r3, ExchangeRate { tape: 220, other: 183 });

        // Request at E3 → withdraw epoch = E5 (E+2)
        let activation_rate = r3; // rate at activation E3
        p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        // Add rewards AFTER activation, s accrues rewards (E4 only).
        p.advance_epoch(epoch(4), tape(30)).unwrap();
        let r4 = p.get_current_rate();
        assert_eq!(r4, ExchangeRate { tape: 250, other: 183 });

        p.advance_epoch(epoch(5), tape(30)).unwrap();

        // Cap rewards pool to 10
        p.rewards = tape(10);

        // Compute owed using the last pre-withdraw snapshot (r4)
        let shares = activation_rate.convert_to_other_amount(s.amount.into());
        let owed = tape(r4.convert_to_tape_amount(shares).saturating_sub(s.amount.into()));

        let paid = p.unstake_from_pool(&mut s, epoch(5), owed).unwrap();
        assert_eq!(paid, tape(10));
        assert_eq!(p.rewards, tape(0));
    }

    #[test]
    fn withdraw_early_err() {
        let mut p = TestPool::new(BasisPoints(0));

        // Stake at E1 → activate E3
        let mut s = p.stake_with_pool(epoch(1), tape(100)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();

        // Request at E3 → withdraw at E5
        let activation_rate = p.get_current_rate();
        p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        // Try to claim at E4 < withdraw → error
        p.advance_epoch(epoch(4), tape(0)).unwrap(); // advance to E4
 
        let r4 = p.get_current_rate();
        let shares = activation_rate.convert_to_other_amount(s.amount.into());
        let owed = tape(r4.convert_to_tape_amount(shares).saturating_sub(s.amount.into()));
        let err = p.unstake_from_pool(&mut s, epoch(4), owed).unwrap_err();
        assert!(matches!(err, PoolError::EpochInvalid));
    }

    #[test]
    fn alice_single() {
        let mut p = TestPool::new(BasisPoints(0));

        // Alice stakes at E0 → activation E2 (E+2)
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();

        // E1: no rewards, still pre-active
        p.advance_epoch(epoch(1), tape(0)).unwrap();

        // E2: activation (no rewards)
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        let r2 = p.get_current_rate();
        assert_eq!(r2, ExchangeRate { tape: 1000, other: 1000 });

        // E3: pool earns 1000
        p.advance_epoch(epoch(3), tape(1000)).unwrap();

        // Request withdraw at E3 → withdraw at E5
        p.request_withdraw(&mut alice, epoch(3), r2).unwrap();

        // E4: no rewards; compute owed at E4 (pre-withdraw snapshot)
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        let r4 = p.get_current_rate();

        // Compute owed using activation shares and E4 rate
        let shares = r2.convert_to_other_amount(alice.amount.into());
        let tape_at_r4 = r4.convert_to_tape_amount(shares);
        let owed = tape(tape_at_r4.saturating_sub(alice.amount.into()));

        // E5: process withdraw; then pay rewards
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        let paid = p.unstake_from_pool(&mut alice, epoch(5), owed).unwrap();
        assert_eq!(paid, owed);
        assert!(paid > TAPE(0));
    }

    #[test]
    fn alice_bob_split() {
        let mut p = TestPool::new(BasisPoints(0));

        // E0: both stake → activate E2
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();
        let mut bob   = p.stake_with_pool(epoch(0), tape(1000)).unwrap();

        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap(); // E2: both active
        let r2 = p.get_current_rate();

        // E3: rewards 1000
        p.advance_epoch(epoch(3), tape(1000)).unwrap();

        // Both request at E3 → withdraw E5
        let activation_rate = r2;
        let wa = p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();
        let wb = p.request_withdraw(&mut bob,   epoch(3), activation_rate).unwrap();
        assert_eq!(wa, epoch(5));
        assert_eq!(wb, epoch(5));

        // E4: settle, no rewards
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        let r4 = p.get_current_rate();

        // E5: claim
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        let shares = activation_rate.convert_to_other_amount(tape(1000).into());
        let owed_each = tape(r4.convert_to_tape_amount(shares).saturating_sub(1000));

        let ra = p.unstake_from_pool(&mut alice, epoch(5), owed_each).unwrap();
        let rb = p.unstake_from_pool(&mut bob,   epoch(5), owed_each).unwrap();

        assert_ne!(ra, 0.into());
        assert_eq!(ra, rb);
    }

    #[test]
    fn commission_round() {
        let mut p = TestPool::new(BasisPoints(1000)); // 10%

        // E0 stake → activate E2
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();

        p.advance_epoch(epoch(1), tape(0)).unwrap();   // E1
        p.advance_epoch(epoch(2), tape(0)).unwrap();   // E2 active
        let r2 = p.get_current_rate();

        p.advance_epoch(epoch(3), tape(202)).unwrap(); // E3 rewards gross=202 → commission=20, net=182

        assert_eq!(p.commission, tape(20));
        assert_eq!(p.rewards, tape(182));

        // Request at E3 → withdraw E5; no more rewards
        let activation_rate = r2;
        p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();
        p.advance_epoch(epoch(4), tape(0)).unwrap();

        let r4 = p.get_current_rate();
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        let shares = activation_rate.convert_to_other_amount(alice.amount.into());
        let owed = tape(r4.convert_to_tape_amount(shares).saturating_sub(alice.amount.into()));
        let paid = p.unstake_from_pool(&mut alice, epoch(5), owed).unwrap();

        assert_eq!(paid, tape(182));
        assert_eq!(p.commission, tape(20));
    }

    #[test]
    fn early_blocked() {
        let mut p = TestPool::new(BasisPoints(0));

        // Stake at E1 → activate E3
        let mut alice = p.stake_with_pool(epoch(1), tape(500)).unwrap();

        // Make sure activation snapshot exists
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();
        let r3 = p.get_current_rate();

        // Request at E3 → withdraw E5
        let activation_rate = r3;
        p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();

        // Trying to claim at E4 (< E5) must error
        p.advance_epoch(epoch(4), tape(0)).unwrap();

        let shares = activation_rate.convert_to_other_amount(alice.amount.into());
        let owed = tape(r3.convert_to_tape_amount(shares).saturating_sub(alice.amount.into()));
        let err = p.unstake_from_pool(&mut alice, epoch(4), owed).unwrap_err();

        assert!(matches!(err, PoolError::EpochInvalid));
    }

    #[test]
    fn maintain_ratio() {
        let mut p = TestPool::new(BasisPoints(0));

        // Alice stakes 1000 at E0 (E2 active)
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        let r2 = p.get_current_rate();

        // Bob stakes 2000 at E1 (E3 active)
        let mut bob = p.stake_with_pool(epoch(1), tape(2000)).unwrap();
        p.advance_epoch(epoch(3), tape(1000)).unwrap(); // Rewards when both are active
        let r3 = p.get_current_rate();

        // Alice requests at E3 → E5
        let activation_rate_e2 = r2;
        p.request_withdraw(&mut alice, epoch(3), activation_rate_e2).unwrap();

        // Bob requests at E4 → E6
        p.advance_epoch(epoch(4), tape(1000)).unwrap();
        let r4 = p.get_current_rate();

        let activation_rate_e3 = r3;
        p.request_withdraw(&mut bob, epoch(4), activation_rate_e3).unwrap();

        // Walk to E5 and let Alice claim
        p.advance_epoch(epoch(5), tape(0)).unwrap();
        let r5 = p.get_current_rate();

        let alice_shares = activation_rate_e2.convert_to_other_amount(alice.amount.into());
        let ra_owed = tape(r4.convert_to_tape_amount(alice_shares).saturating_sub(alice.amount.into()));
        let ra = p.unstake_from_pool(&mut alice, epoch(5), ra_owed).unwrap();

        // Walk to E6 and let Bob claim
        p.advance_epoch(epoch(6), tape(0)).unwrap();
        let bob_shares = activation_rate_e3.convert_to_other_amount(bob.amount.into());
        let rb_owed = tape(r5.convert_to_tape_amount(bob_shares).saturating_sub(bob.amount.into()));
        let rb = p.unstake_from_pool(&mut bob, epoch(6), rb_owed).unwrap();

        // Basic sanity: both > 0, reflect different active windows
        assert!(ra > TAPE(0));
        assert!(rb > TAPE(0));
    }
}
