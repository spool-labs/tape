use crate::coin::*;
use crate::types::{EpochNumber, BasisPoints};

use bytemuck::{Pod, Zeroable};

use super::{
    exchange::*,
    value::*
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    FailedToScheduleStake,
    FailedToScheduleCommission,
    FailedToScheduleWithdraw,
    PoolIsNotActive,
    PendingStakeExceeded,
    TapeBalanceExceeded,
    EpochAlreadyProcessed,
    MustHaveStakedTape,
    WithdrawEpochNotReached,
    NoSuchRate,
    ZeroShares,
    ZeroStake
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StakingPool<const N: usize, const M: usize> {
    /// The latest epoch for this pool was updated.
    pub latest_epoch: EpochNumber,


    /// The total TAPE held by this pool (excluding commission).
    pub tape_balance: Coin<TAPE>,

    /// The rewards this pool has earned from being active and available to distribute to stakers
    pub rewards_pool: Coin<TAPE>,

    /// The totlal number of shares issued by this pool.
    pub count_shares: u64,

    /// The commission (in TAPE) earned by the pool operator, available for withdrawal.
    pub commission: Coin<TAPE>,

    /// The commission rate (in basis points, 1/100 of a percent) taken from rewards earned by this pool.
    pub commission_rate: BasisPoints,


    /// The pending commission rate changes, scheduled for future epochs.
    /// epoch -> u64(bps)
    pub pending_commission_rate: PendingValues<M>,  

    /// The pending stake additions and share withdrawals, scheduled for future epochs.
    /// activation_epoch -> principal
    pub pending_stake: PendingValues<M>,            

    /// The pending share withdrawals, scheduled for future epochs.
    /// withdraw_epoch -> shares
    pub pending_shares_withdraw: PendingValues<M>,  

    /// The pending pre-active stake cancellations, scheduled for future epochs.
    /// activation_epoch -> principal canceled pre-active
    pub pre_active_withdrawals: PendingValues<M>,   

    /// Exchange rates (to shares) for epochs this pool was active.
    /// The most recent N rates are kept. 
    pub history: PreviousRates<N>,
}

unsafe impl<const N: usize, const M: usize> Zeroable for StakingPool<N, M> {}
unsafe impl<const N: usize, const M: usize> Pod for StakingPool<N, M> {}

impl<const N: usize, const M: usize> StakingPool<N, M> {
    pub fn new(commission_rate: BasisPoints) -> Self {
        let latest_epoch = EpochNumber::zero();
        Self {
            latest_epoch,
            count_shares: 0,
            tape_balance: Coin::<TAPE>::zero(),
            rewards_pool: Coin::<TAPE>::zero(),
            commission: Coin::<TAPE>::zero(),
            commission_rate,
            pending_stake: PendingValues::new(),
            pending_shares_withdraw: PendingValues::new(),
            pre_active_withdrawals: PendingValues::new(),
            pending_commission_rate: PendingValues::new(),
            history: PreviousRates::new(),
        }
    }

    /// Get the most recent rate at or before the given epoch, 
    /// returning None if no such rate exists.
    pub fn exchange_rate_at_epoch(&self, epoch: EpochNumber) -> Option<ExchangeRate> {
        // TODO: add a merkle tree lookup path for older rates. 
        // Shapshots should add to a history root value.
        // (the current desing will work for *years*)

        self.history.on_or_before(epoch)
    }


    /// Add rewards from previous epoch to this pool, split commission vs net rewards.
    /// rewards_gross is the total earned by this pool in the previous epoch.
    pub fn advance_epoch(
        &mut self, 
        current_epoch: EpochNumber, 
        rewards_gross: Coin<TAPE>
    ) -> Result<(), PoolError> {

        if current_epoch <= self.latest_epoch {
            return Err(PoolError::EpochAlreadyProcessed);
        }

        self.apply_pending_commission_rate(current_epoch);

        if rewards_gross > TAPE::zero() {
            if self.tape_balance == TAPE::zero() {
                return Err(PoolError::MustHaveStakedTape);
            }

            let commission_cut = (
                rewards_gross.as_u128() * self.commission_rate.as_u128() / BasisPoints::MAX as u128
            ) as u64;

            let rewards_net = rewards_gross
                .saturating_sub(commission_cut.into());

            self.commission = self.commission
                .saturating_add(commission_cut.into());

            self.rewards_pool = self.rewards_pool
                .saturating_add(rewards_net);

            self.tape_balance = self.tape_balance
                .saturating_add(rewards_net);
        }

        self.process_pending_stake(current_epoch)?;
        self.latest_epoch = current_epoch;

        Ok(())
    }

    /// Schedule a commission rate change for E+2.
    pub fn set_next_commission(
        &mut self, 
        current_epoch: EpochNumber, 
        new_rate: BasisPoints
    ) -> Result<(), PoolError> {

        let effective_epoch = current_epoch + EpochNumber(2);
        self.pending_commission_rate
            .insert_or_replace(effective_epoch, new_rate.into())
            .map_err(|_| PoolError::FailedToScheduleCommission)?;

        Ok(())
    }

    /// Apply commission rate update if one is scheduled for current_epoch.
    pub fn apply_pending_commission_rate(&mut self, current_epoch: EpochNumber) {
        if let Some(&new_rate) = self.pending_commission_rate.get(&current_epoch) {
            self.commission_rate = BasisPoints(new_rate);

            // Clear all <= current
            self.pending_commission_rate.flush(current_epoch);
        }
    }

    /// Project the tape_balance at a future epoch
    pub fn tape_balance_at_epoch(&self, epoch: EpochNumber) -> u64 {
        let current_rate = ExchangeRate::new(
            self.tape_balance.into(),
            self.count_shares
        );

        let stake_additions = self.pending_stake.value_at(epoch);
        let canceled_pre_active = self.pre_active_withdrawals.value_at(epoch);
        let net_additions = stake_additions.saturating_sub(canceled_pre_active);

        let shares_withdraw = self.pending_shares_withdraw.value_at(epoch);
        let withdrawals_tape = current_rate.convert_to_tape_amount(shares_withdraw);

        self.tape_balance
            .as_u64()
            .saturating_add(net_additions)
            .saturating_sub(withdrawals_tape)
    }

    /// Process pending stake/withdrawals for the current_epoch
    pub fn process_pending_stake(&mut self, current_epoch: EpochNumber) -> Result<(), PoolError> {
        let current_rate = ExchangeRate::new(
            self.tape_balance.into(),
            self.count_shares
        );

        self.history.push(current_epoch, current_rate);

        // Handle tape_balance increases (due to pending stake additions)
        self.process_pending_additions(current_epoch)?;

        // Handle tape_balance reductions (due to pending share withdrawals)
        self.process_pending_reductions(current_epoch, current_rate)?;

        // Correct the current number of shares using the newly updated tape_balance
        self.count_shares = current_rate
            .convert_to_other_amount(self.tape_balance.into());

        Ok(())
    }

    /// Process pending stake additions and pre-active cancellations for the current_epoch.
    fn process_pending_additions(&mut self, current_epoch: EpochNumber) -> Result<(), PoolError> {
        // Sum all pending stake before or at current_epoch
        let total_pending = self.pending_stake.flush(current_epoch);

        // Sum all pre-active cancellations before or at current_epoch
        let canceled_pre_active = self.pre_active_withdrawals.flush(current_epoch);

        // Net pending stake must be non-negative 
        // (this should be guaranteed by scheduling logic)
        if canceled_pre_active > total_pending {
            return Err(PoolError::PendingStakeExceeded);
        }

        // Increase tape_balance by net added stake
        let net_added = total_pending - canceled_pre_active;

        if net_added > 0 {
            self.tape_balance = self.tape_balance
                .saturating_add(net_added.into());
        }

        Ok(())
    }

    /// Process pending share withdrawals for the current_epoch.
    fn process_pending_reductions(
        &mut self,
        current_epoch: EpochNumber,
        current_rate: ExchangeRate,
    ) -> Result<(), PoolError> {

        // Sum all pending shares withdrawing before or at current_epoch
        let total_shares_withdrawing = self.pending_shares_withdraw.flush(current_epoch);

        // Convert shares to tape at current rate and remove from tape_balance
        let net_removed = current_rate
            .convert_to_tape_amount(total_shares_withdrawing);

        // The net balance to remove must not exceed current balance
        // (this should be guaranteed by scheduling logic)
        if self.tape_balance < net_removed.into() {
            return Err(PoolError::TapeBalanceExceeded);
        }

        if net_removed > 0 {
            self.tape_balance = self.tape_balance
                .saturating_sub(net_removed.into());
        }

        Ok(())
    }

    /// Stake tokens with this pool.
    pub fn stake_with_pool(
        &mut self, 
        current_epoch: EpochNumber, 
        stake_amount: Coin<TAPE>
    ) -> Result<(), PoolError> {
        if stake_amount == TAPE::zero() {
            return Err(PoolError::ZeroStake);
        }

        // Activation is always E+2 for simplicity 
        // (may be changed later)
        let activation_epoch = current_epoch + EpochNumber(2);

        self.pending_stake
            .insert_or_add(activation_epoch, stake_amount.into())
            .map_err(|_| PoolError::FailedToScheduleStake)?;

        Ok(())
    }

    /// Request a withdrawal of stake from this pool.
    pub fn unstake_from_pool(
        &mut self,
        stake_activation_epoch: EpochNumber,
        stake_principal: Coin<TAPE>,
        current_epoch: EpochNumber,
    ) -> Result<EpochNumber, PoolError> {

        if stake_principal == TAPE::zero() {
            return Err(PoolError::MustHaveStakedTape);
        }

        // Withdrawals are always E+2 for simplicity
        // (may be changed later)
        let withdraw_epoch = current_epoch + EpochNumber(2);

        // If the stake activation was in the future, this is a pre-active cancel.

        if stake_activation_epoch > current_epoch {
            // Schedule the stake principal to be canceled at activation_epoch. 
            // The net result is 0 change to tape_balance at that epoch for this stake.
            self.pre_active_withdrawals
                .insert_or_add(stake_activation_epoch, stake_principal.into())
                .map_err(|_| PoolError::FailedToScheduleStake)?;

            return Ok(withdraw_epoch);
        }

        // Otherwise, this is an active stake withdraw, so we need to schedule a share removal
        // which would calculate rewards at withdraw time.

        let stake_activation_rate = self
            .exchange_rate_at_epoch(stake_activation_epoch)
            .ok_or(PoolError::NoSuchRate)?;

        let count_shares = stake_activation_rate
            .convert_to_other_amount(stake_principal.into());

        if count_shares == 0 {
            return Err(PoolError::ZeroShares);
        }

        self.pending_shares_withdraw
            .insert_or_add(withdraw_epoch, count_shares)
            .map_err(|_| PoolError::FailedToScheduleWithdraw)?;

        Ok(withdraw_epoch)
    }


    /// Claim rewards earned by a stake from activation to withdraw epoch.
    pub fn claim_stake_rewards(
        &mut self,
        stake_activation_epoch: EpochNumber,
        stake_withdraw_epoch: EpochNumber,
        stake_principal: Coin<TAPE>,
        current_epoch: EpochNumber,
    ) -> Result<Coin<TAPE>, PoolError> {
        if stake_principal == TAPE::zero() {
            return Err(PoolError::MustHaveStakedTape);
        }

        if current_epoch < stake_withdraw_epoch {
            return Err(PoolError::WithdrawEpochNotReached);
        }

        // If the withdraw epoch is before or at activation, then no rewards are due.
        if stake_withdraw_epoch <= stake_activation_epoch {
            return Ok(TAPE::zero());
        }

        // Otherwise, calculate rewards from the activation to the withdraw epoch.
        let mut rewards = self.calculate_rewards(
            stake_principal, 
            stake_activation_epoch, 
            stake_withdraw_epoch
        )?;

        if rewards > self.rewards_pool {
            rewards = self.rewards_pool;
        }

        self.rewards_pool = self.rewards_pool
            .saturating_sub(rewards);

        Ok(rewards)
    }

    /// Compute rewards from activation_epoch to withdraw_epoch via exchange rates
    pub fn calculate_rewards(
        &self,
        staked_principal: Coin<TAPE>,
        activation_epoch: EpochNumber,
        withdraw_epoch: EpochNumber,
    ) -> Result<Coin<TAPE>, PoolError> {

        let at_activation = self.exchange_rate_at_epoch(activation_epoch)
            .ok_or(PoolError::NoSuchRate)?;

        let at_withdraw = self.exchange_rate_at_epoch(withdraw_epoch)
            .ok_or(PoolError::NoSuchRate)?;

        let shares = at_activation
            .convert_to_other_amount(staked_principal.into());

        let net_rewards = at_withdraw
            .convert_to_tape_amount(shares)
            .saturating_sub(staked_principal.into());

        Ok(net_rewards.into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // Helpers
    type PoolN<const N: usize, const M: usize> = StakingPool<N, M>;
    type P = PoolN<100, 2>;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }

    #[test]
    fn new_ok() {
        let p = P::new(BasisPoints(1000));
        assert_eq!(p.latest_epoch, epoch(0));
        assert_eq!(p.tape_balance, TAPE::zero());
        assert_eq!(p.count_shares, 0);
        assert_eq!(p.commission_rate, BasisPoints(1000));
    }

    #[test]
    fn stake_sched() {
        let mut p = P::new(BasisPoints(0));
        p.stake_with_pool(epoch(5), tape(700)).unwrap();
        // E+2 scheduling
        assert_eq!(p.pending_stake.value_at(epoch(6)), 0);
        assert_eq!(p.pending_stake.value_at(epoch(7)), 700);
    }

    #[test]
    fn rate_none_flat() {
        let p = P::new(BasisPoints(0));
        // No rate at/before < activation → expect flat() from calculate path
        let r = p.exchange_rate_at_epoch(epoch(4));
        assert!(r.is_none());
    }

    #[test]
    fn adv_commission() {
        let mut p = P::new(BasisPoints(1000)); // 10%
        // Activate 1_000 at E1
        p.pending_stake.insert_or_add(epoch(1), 1_000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        assert_eq!(p.tape_balance, tape(1_000));

        // Add 500 gross at E2 → 10% commission, 450 net
        p.advance_epoch(epoch(2), tape(500)).unwrap();
        assert_eq!(p.commission, tape(50));
        assert_eq!(p.rewards_pool, tape(450));
        assert_eq!(p.tape_balance, tape(1_450));
    }

    #[test]
    fn adv_no_stake_err() {
        let mut p = P::new(BasisPoints(0));
        // Rewards with zero stake should error
        let err = p.advance_epoch(epoch(1), tape(10)).unwrap_err();
        assert!(matches!(err, PoolError::MustHaveStakedTape));
    }

    #[test]
    fn epoch_dupe_err() {
        let mut p = P::new(BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        let err = p.advance_epoch(epoch(1), tape(0)).unwrap_err();
        assert!(matches!(err, PoolError::EpochAlreadyProcessed));
    }

    #[test]
    fn set_comm_next() {
        let mut p = P::new(BasisPoints(1000));
        p.pending_stake.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.set_next_commission(epoch(2), BasisPoints(2000)).unwrap(); // applies at E4
        p.advance_epoch(epoch(3), tape(0)).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(1000)); // still old
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(2000)); // now new
    }

    #[test]
    fn process_pend() {
        let mut p = P::new(BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        assert_eq!(p.tape_balance, tape(1000));
        assert_eq!(p.count_shares, 1000); // flat rate at first snapshot
    }

    #[test]
    fn balance_proj() {
        let mut p = P::new(BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // balance=1000, shares=1000, flat
        // Schedule more stake for E5 and a giant withdraw at E6
        p.pending_stake.insert_or_add(epoch(5), 600).unwrap();
        p.pending_shares_withdraw.insert_or_add(epoch(6), 200).unwrap();
        // Projection uses CURRENT rate (flat 1:1 here)
        assert_eq!(p.tape_balance_at_epoch(epoch(4)), 1000);
        assert_eq!(p.tape_balance_at_epoch(epoch(5)), 1600); // +600
        assert_eq!(p.tape_balance_at_epoch(epoch(6)), 1400); // -200 (flat)
    }

    #[test]
    fn pend_over_cancel_err() {
        let mut p = P::new(BasisPoints(0));
        // Cancel more than added at same epoch → error
        p.pre_active_withdrawals.insert_or_add(epoch(3), 200).unwrap();
        p.pending_stake.insert_or_add(epoch(3), 100).unwrap();
        let err = p.process_pending_stake(epoch(3)).unwrap_err();
        assert!(matches!(err, PoolError::PendingStakeExceeded));
    }

    #[test]
    fn tape_exceed_err() {
        let mut p = P::new(BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // current_rate = 1000 tape / 1000 shares; withdrawing >1000 shares attempts >balance
        p.pending_shares_withdraw.insert_or_add(epoch(2), 1500).unwrap();
        let err = p.process_pending_stake(epoch(2)).unwrap_err();
        assert!(matches!(err, PoolError::TapeBalanceExceeded));
    }

    #[test]
    fn withdraw_sched_pre() {
        let mut p = P::new(BasisPoints(0));
        // pre-active stake (activation > current), should be canceled and no shares scheduled
        let we = p.unstake_from_pool(
            epoch(7),
            tape(500),
            epoch(5),
        ).unwrap();
        assert_eq!(we, epoch(7)); // current(5)+2
        assert_eq!(p.pre_active_withdrawals.value_at(epoch(7)), 500);
        assert_eq!(p.pending_shares_withdraw.value_at(epoch(7)), 0);
    }

    #[test]
    fn withdraw_sched_act() {
        let mut p = P::new(BasisPoints(0));
        // Activate 1000 at E1
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // Stake became active at E1 under flat rate
        let we = p.unstake_from_pool(
            epoch(1),
            tape(1000),
            epoch(2),
        ).unwrap();
        assert_eq!(we, epoch(4)); // E2+2
        assert_eq!(p.pending_shares_withdraw.value_at(epoch(4)), 1000);
    }


    #[test]
    fn withdraw_pre_no_rewards() {
        let mut p = P::new(BasisPoints(0));
        // Pre-active withdrawal: rewards should be zero
        let r = p.claim_stake_rewards(
            epoch(7),  // activation
            epoch(6),  // withdraw <= activation
            tape(500),
            epoch(8),
        ).unwrap();
        assert_eq!(r, tape(0));
    }

    #[test]
    fn withdraw_pay_cap() {
        let mut p = P::new(BasisPoints(0));
        // Activate 100 at E1
        p.pending_stake.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // Add rewards at E2 (pool grows to 120)
        p.advance_epoch(epoch(2), tape(20)).unwrap();
        // Another +30 at E3 (pool grows to 150)
        p.advance_epoch(epoch(3), tape(30)).unwrap();
        // Snapshot at E3 set; request withdraw at E3 → withdraw_epoch=E5
        let we = p.unstake_from_pool(epoch(1), tape(100), epoch(3)).unwrap();
        assert_eq!(we, epoch(5));

        // Make rewards_pool small to test cap
        p.rewards_pool = tape(10);

        // Push epochs to ensure rate history includes E5
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        // Rewards owed (from rate growth) may exceed pool; only 10 can be paid
        let paid = p.claim_stake_rewards(epoch(1), we, tape(100), epoch(5)).unwrap();
        assert_eq!(paid, tape(10));
        assert_eq!(p.rewards_pool, tape(0));
    }

    #[test]
    fn withdraw_early_err() {
        let mut p = P::new(BasisPoints(0));
        // Withdrawing before withdraw_epoch should error
        let err = p.claim_stake_rewards(
            epoch(1),
            epoch(6), // withdraw at 6
            tape(100),
            epoch(5), // current 5 < withdraw
        ).unwrap_err();
        assert!(matches!(err, PoolError::WithdrawEpochNotReached));
    }

    #[test]
    fn calc_simple() {
        let mut p = P::new(BasisPoints(0));
        // E1: add 100, flat -> shares 100
        p.pending_stake.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // E2: +20 rewards; now rate = 120/100
        p.advance_epoch(epoch(2), tape(20)).unwrap();

        // Rewards from E1 -> E2 = 20
        let r = p.calculate_rewards(tape(100), epoch(1), epoch(2)).unwrap();
        assert_eq!(r, tape(20));
    }

    #[test]
    fn rate_missing_err() {
        // Use small rate window so old epochs fall off
        type PS = PoolN<2, 2>; // only keep 2 rates
        let mut p = PS::new(BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // snapshot E1
        p.advance_epoch(epoch(2), tape(0)).unwrap(); // snapshot E2
        p.advance_epoch(epoch(3), tape(0)).unwrap(); // snapshot E3 (E1 likely evicted)
        // Ask for withdraw at E1 (older than kept history) → Err(NoSuchRate)
        let err = p.calculate_rewards(tape(100), epoch(1), epoch(1)).unwrap_err();
        assert!(matches!(err, PoolError::NoSuchRate));
    }
}
