use crate::coin::*;
use crate::types::{EpochNumber, BasisPoints};

use bytemuck::{Pod, Zeroable};

use super::{
    exchange::*,
    value::*,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    FailedToScheduleStake,
    FailedToScheduleCommission,
    FailedToScheduleWithdraw,
    PendingStakeExceeded,
    TapeBalanceExceeded,
    EpochAlreadyProcessed,
    MustHaveStakedTape,
    WithdrawEpochNotReached,
    NoSuchRate,
    ZeroShares,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StakingPool<const N: usize, const M: usize> {
    pub activation_epoch: EpochNumber,
    pub latest_epoch: EpochNumber,

    // Any stake withdraw that needs a rate older than the oldest is rejected as too old, the value
    // of N will be chosen to be large enough to provide a reasonable history, e.g. several years.
    pub exchange_rates: PreviousRates<N>,

    // core balances
    pub count_shares: u64,
    pub tape_balance: Coin<TAPE>,    // net, excluding commission; grows by net rewards
    pub rewards_pool: Coin<TAPE>,    // net rewards available to distribute to stakers
    pub commission: Coin<TAPE>,      // accumulated commission
    pub commission_rate: BasisPoints,

    // queued values
    pub pending_commission_rate: PendingValues<M>,  // epoch -> u64(bps)
    pub pending_stake: PendingValues<M>,            // activation_epoch -> principal
    pub pending_shares_withdraw: PendingValues<M>,  // withdraw_epoch -> shares
    pub pre_active_withdrawals: PendingValues<M>,   // activation_epoch -> principal canceled pre-active
}

unsafe impl<const N: usize, const M: usize> Zeroable for StakingPool<N, M> {}
unsafe impl<const N: usize, const M: usize> Pod for StakingPool<N, M> {}

impl<const N: usize, const M: usize> StakingPool<N, M> {
    pub fn new(
        activation_epoch: EpochNumber, 
        commission_rate: BasisPoints
        ) -> Self {

        // Set latest_epoch to (activation_epoch - 1), saturating at 0.
        let latest_epoch = activation_epoch
            .saturating_sub(EpochNumber::one());

        Self {
            activation_epoch,
            latest_epoch,
            exchange_rates: PreviousRates::new(),
            count_shares: 0,
            tape_balance: Coin::<TAPE>::zero(),
            rewards_pool: Coin::<TAPE>::zero(),
            commission: Coin::<TAPE>::zero(),
            commission_rate,
            pending_commission_rate: PendingValues::new(),
            pending_stake: PendingValues::new(),
            pending_shares_withdraw: PendingValues::new(),
            pre_active_withdrawals: PendingValues::new(),
        }
    }

    /// Schedule stake to activate at current + 2.
    pub fn stake(
        &mut self, 
        current_epoch: EpochNumber, 
        amount_tape: Coin<TAPE>
    ) -> Result<(), PoolError> {

        let activation_epoch = current_epoch + EpochNumber(2);
        self.pending_stake
            .insert_or_add(activation_epoch, amount_tape.into())
            .map_err(|_| PoolError::FailedToScheduleStake)?;

        Ok(())
    }

    /// Get the most recent rate at or before the given epoch, 
    /// returning None if no such rate exists.
    pub fn exchange_rate_at_epoch(&self, epoch: EpochNumber) -> Option<ExchangeRate> {
        self.exchange_rates.rate_at(epoch)
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

    /// Process pending stake/withdrawals for the current_epoch:
    /// - snapshot exchange rate
    /// - add net pending stake (added - pre-active cancellations) for current
    /// - remove scheduled share withdrawals at current
    /// - re-derive num_shares from current rate
    pub fn process_pending_stake(&mut self, current_epoch: EpochNumber) -> Result<(), PoolError> {
        let current_rate = ExchangeRate::new(
            self.tape_balance.into(),
            self.count_shares
        );

        self.exchange_rates.push(current_epoch, current_rate);

        // Add net stake scheduled at current_epoch (subtract pre-active cancellations)
        let added = self.pending_stake.flush(current_epoch);
        let canceled_pre_active = self.pre_active_withdrawals.flush(current_epoch);

        if added < canceled_pre_active {
            return Err(PoolError::PendingStakeExceeded);
        }

        let net_added = added - canceled_pre_active;
        self.tape_balance = self.tape_balance
            .saturating_add(TAPE(net_added));

        // Process share withdrawals scheduled for current_epoch
        let shares_withdraw = self.pending_shares_withdraw.flush(current_epoch);
        let tape_to_remove = current_rate.convert_to_tape_amount(shares_withdraw);

        if self.tape_balance.as_u64() < tape_to_remove {
            return Err(PoolError::TapeBalanceExceeded);
        }

        self.tape_balance = self.tape_balance
            .saturating_sub(tape_to_remove.into());

        self.count_shares = current_rate
            .convert_to_other_amount(self.tape_balance.into());

        Ok(())
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
                rewards_gross.as_u128() * self.commission_rate.as_u128()
                / BasisPoints::MAX as u128
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

    /// Projected active TAPE at epoch E.
    /// Uses: tape_balance + (pending_stake.value_at(E) - pre_active_cancellations.value_at(E))
    /// minus withdrawals (convert scheduled shares at E by current rate).
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

    /// Compute rewards from activation_epoch to withdraw_epoch via exchange rates
    pub fn calculate_rewards(
        &self,
        staked_principal: Coin<TAPE>,
        activation_epoch: EpochNumber,
        withdraw_epoch: EpochNumber,
    ) -> Result<Coin<TAPE>, PoolError> {
        let at_activation = self
            .exchange_rate_at_epoch(activation_epoch)
            .unwrap_or(ExchangeRate::flat());

        let shares = at_activation
            .convert_to_other_amount(staked_principal.into());

        let at_withdraw = self.exchange_rate_at_epoch(withdraw_epoch)
            .ok_or(PoolError::NoSuchRate)?;

        let tape_out = at_withdraw.convert_to_tape_amount(shares);

        Ok(tape_out
            .saturating_sub(staked_principal.into())
            .into())
    }

    /// Always E+2: request withdrawal schedules:
    /// - If pre-active (activation_epoch > current): record a pre-active cancel at activation_epoch.
    /// - Else (already active): schedule shares removal at withdraw_epoch (= current + 2).
    pub fn request_withdraw_stake(
        &mut self,
        stake_activation_epoch: EpochNumber,
        stake_principal: Coin<TAPE>,
        stake_rate: ExchangeRate,
        current_epoch: EpochNumber,
    ) -> Result<EpochNumber, PoolError> {

        if stake_principal == TAPE::zero() {
            return Err(PoolError::MustHaveStakedTape);
        }

        let withdraw_epoch = current_epoch + EpochNumber(2);

        if stake_activation_epoch > current_epoch {
            // Pre-active: never let it become active.
            // Record cancellation to net off the addition at activation_epoch.
            self.pre_active_withdrawals
                .insert_or_add(stake_activation_epoch, stake_principal.into())
                .map_err(|_| PoolError::FailedToScheduleStake)?;

            return Ok(withdraw_epoch);
        }

        // Stake already active: schedule shares removal at withdraw_epoch.
        let shares = stake_rate.convert_to_other_amount(stake_principal.into());
        if shares == 0 {
            return Err(PoolError::ZeroShares);
        }

        self.pending_shares_withdraw
            .insert_or_add(withdraw_epoch, shares)
            .map_err(|_| PoolError::FailedToScheduleWithdraw)?;

        Ok(withdraw_epoch)
    }


    /// Withdraw stake (two-step only):
    /// - Must be in Withdrawing state with withdraw_epoch <= current.
    /// - If withdraw_epoch <= activation_epoch: pre-active cancel → return principal, no rewards.
    /// - Else: rewards from activation_epoch to withdraw_epoch paid out of rewards_pool (capped).
    pub fn withdraw_stake(
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

        // Pre-active (never active long enough to accrue rewards).
        if stake_withdraw_epoch <= stake_activation_epoch {
            return Ok(TAPE::zero());
        }

        // Active case: pay rewards from activation
        let mut rewards = self.calculate_rewards(
            stake_principal, 
            stake_activation_epoch, 
            stake_withdraw_epoch
        )?;

        // Only pay out what is available.
        if rewards > self.rewards_pool {
            rewards = self.rewards_pool;
        }

        self.rewards_pool = self.rewards_pool
            .saturating_sub(rewards);

        Ok(rewards)
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
    fn rate(tape_amt: u64, shares: u64) -> ExchangeRate { ExchangeRate::new(tape_amt, shares) }

    #[test]
    fn new_ok() {
        let p = P::new(epoch(3), BasisPoints(1000));
        assert_eq!(p.activation_epoch, epoch(3));
        assert_eq!(p.latest_epoch, epoch(2));
        assert_eq!(p.tape_balance, TAPE::zero());
        assert_eq!(p.count_shares, 0);
        assert_eq!(p.commission_rate, BasisPoints(1000));
    }

    #[test]
    fn stake_sched() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        p.stake(epoch(5), tape(700)).unwrap();
        // E+2 scheduling
        assert_eq!(p.pending_stake.value_at(epoch(6)), 0);
        assert_eq!(p.pending_stake.value_at(epoch(7)), 700);
    }

    #[test]
    fn rate_none_flat() {
        let p = P::new(epoch(5), BasisPoints(0));
        // No rate at/before < activation → expect flat() from calculate path
        let r = p.exchange_rate_at_epoch(epoch(4));
        assert!(r.is_none());
    }

    #[test]
    fn adv_commission() {
        let mut p = P::new(epoch(1), BasisPoints(1000)); // 10%
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
        let mut p = P::new(epoch(1), BasisPoints(0));
        // Rewards with zero stake should error
        let err = p.advance_epoch(epoch(1), tape(10)).unwrap_err();
        assert!(matches!(err, PoolError::MustHaveStakedTape));
    }

    #[test]
    fn epoch_dupe_err() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        let err = p.advance_epoch(epoch(1), tape(0)).unwrap_err();
        assert!(matches!(err, PoolError::EpochAlreadyProcessed));
    }

    #[test]
    fn set_comm_next() {
        let mut p = P::new(epoch(1), BasisPoints(1000));
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
        let mut p = P::new(epoch(1), BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        assert_eq!(p.tape_balance, tape(1000));
        assert_eq!(p.count_shares, 1000); // flat rate at first snapshot
    }

    #[test]
    fn balance_proj() {
        let mut p = P::new(epoch(1), BasisPoints(0));
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
        let mut p = P::new(epoch(1), BasisPoints(0));
        // Cancel more than added at same epoch → error
        p.pre_active_withdrawals.insert_or_add(epoch(3), 200).unwrap();
        p.pending_stake.insert_or_add(epoch(3), 100).unwrap();
        let err = p.process_pending_stake(epoch(3)).unwrap_err();
        assert!(matches!(err, PoolError::PendingStakeExceeded));
    }

    #[test]
    fn tape_exceed_err() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // current_rate = 1000 tape / 1000 shares; withdrawing >1000 shares attempts >balance
        p.pending_shares_withdraw.insert_or_add(epoch(2), 1500).unwrap();
        let err = p.process_pending_stake(epoch(2)).unwrap_err();
        assert!(matches!(err, PoolError::TapeBalanceExceeded));
    }

    #[test]
    fn withdraw_sched_pre() {
        let mut p = P::new(epoch(5), BasisPoints(0));
        // pre-active stake (activation > current), should be canceled and no shares scheduled
        let we = p.request_withdraw_stake(
            epoch(7),
            tape(500),
            ExchangeRate::flat(),
            epoch(5),
        ).unwrap();
        assert_eq!(we, epoch(7)); // current(5)+2
        assert_eq!(p.pre_active_withdrawals.value_at(epoch(7)), 500);
        assert_eq!(p.pending_shares_withdraw.value_at(epoch(7)), 0);
    }

    #[test]
    fn withdraw_sched_act() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        // Activate 1000 at E1
        p.pending_stake.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // Stake became active at E1 under flat rate
        let we = p.request_withdraw_stake(
            epoch(1),
            tape(1000),
            rate(1000, 1000),
            epoch(2),
        ).unwrap();
        assert_eq!(we, epoch(4)); // E2+2
        assert_eq!(p.pending_shares_withdraw.value_at(epoch(4)), 1000);
    }

    #[test]
    fn shares_zero_err() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        // Any rate where (tape_amount * other) / tape == 0. 
        // Example: tape=3, other=1, tape_amount=1 -> 0 shares.
        let err = p.request_withdraw_stake(
            epoch(1),
            tape(1),      // tiny principal
            rate(3, 1),   // 1/3 share per tape
            epoch(2),
        ).unwrap_err();
        assert!(matches!(err, PoolError::ZeroShares));
    }

    #[test]
    fn withdraw_pre_no_rewards() {
        let mut p = P::new(epoch(5), BasisPoints(0));
        // Pre-active withdrawal: rewards should be zero
        let r = p.withdraw_stake(
            epoch(7),  // activation
            epoch(6),  // withdraw <= activation
            tape(500),
            epoch(8),
        ).unwrap();
        assert_eq!(r, tape(0));
    }

    #[test]
    fn withdraw_pay_cap() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        // Activate 100 at E1
        p.pending_stake.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // Add rewards at E2 (pool grows to 120)
        p.advance_epoch(epoch(2), tape(20)).unwrap();
        // Another +30 at E3 (pool grows to 150)
        p.advance_epoch(epoch(3), tape(30)).unwrap();
        // Snapshot at E3 set; request withdraw at E3 → withdraw_epoch=E5
        let we = p.request_withdraw_stake(epoch(1), tape(100), rate(100, 100), epoch(3)).unwrap();
        assert_eq!(we, epoch(5));

        // Make rewards_pool small to test cap
        p.rewards_pool = tape(10);

        // Push epochs to ensure rate history includes E5
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        // Rewards owed (from rate growth) may exceed pool; only 10 can be paid
        let paid = p.withdraw_stake(epoch(1), we, tape(100), epoch(5)).unwrap();
        assert_eq!(paid, tape(10));
        assert_eq!(p.rewards_pool, tape(0));
    }

    #[test]
    fn withdraw_early_err() {
        let mut p = P::new(epoch(1), BasisPoints(0));
        // Withdrawing before withdraw_epoch should error
        let err = p.withdraw_stake(
            epoch(1),
            epoch(6), // withdraw at 6
            tape(100),
            epoch(5), // current 5 < withdraw
        ).unwrap_err();
        assert!(matches!(err, PoolError::WithdrawEpochNotReached));
    }

    #[test]
    fn calc_simple() {
        let mut p = P::new(epoch(1), BasisPoints(0));
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
        let mut p = PS::new(epoch(1), BasisPoints(0));
        p.pending_stake.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // snapshot E1
        p.advance_epoch(epoch(2), tape(0)).unwrap(); // snapshot E2
        p.advance_epoch(epoch(3), tape(0)).unwrap(); // snapshot E3 (E1 likely evicted)
        // Ask for withdraw at E1 (older than kept history) → Err(NoSuchRate)
        let err = p.calculate_rewards(tape(100), epoch(1), epoch(1)).unwrap_err();
        assert!(matches!(err, PoolError::NoSuchRate));
    }
}
