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
    ZeroShares,
    ZeroStake,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StakingPool<const N: usize> {
    /// The total number of shares issued by this pool.
    pub shares: u64,

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
            shares: 0,
            stake: Coin::<TAPE>::zero(),
            rewards: Coin::<TAPE>::zero(),
            commission: Coin::<TAPE>::zero(),
            commission_rate,
            schedule: PoolSchedule::new(),
        }
    }

    /// Apply commission rate update if one is scheduled for current_epoch.
    fn apply_commission_change(&mut self, current_epoch: EpochNumber) {
        if let Some(new_rate) = self.schedule.take_commission_change(current_epoch) {
            self.commission_rate = new_rate;
        }
    }

    /// Project the stake at a future epoch (uses current exchange rate snapshot).
    pub fn stake_at(&self, epoch: EpochNumber) -> Coin<TAPE> {
        // Calculate current exchange rate (stake per share)
        let exchange_rate = ExchangeRate::new(
            self.stake.into(),
            self.shares
        );

        // Calculate net token additions (incoming - outgoing)
        let incoming = self.schedule.incoming_sum(epoch);
        let outgoing = self.schedule.outgoing_sum(epoch);
        let net_additions = incoming.saturating_sub(outgoing);

        // Convert outgoing shares to token amount
        let outgoing_shares = self.schedule.outgoing_shares_sum(epoch);
        let outgoing_tokens = exchange_rate.convert_to_tape_amount(outgoing_shares);

        // Compute final stake: current stake + net additions - outgoing tokens
        self.stake
            .as_u64()
            .saturating_add(net_additions)
            .saturating_sub(outgoing_tokens)
            .into()
    }

    /// Process pending stake/withdrawals for the current_epoch, using the provided epoch exchange rate.
    fn process_pending_stake(
        &mut self,
        current_epoch: EpochNumber,
        epoch_rate: ExchangeRate,
    ) -> Result<(), PoolError> {
        // Handle stake increases (due to pending stake additions)
        self.process_pending_additions(current_epoch)?;

        // Handle stake reductions (due to pending share withdrawals)
        self.process_pending_reductions(current_epoch, epoch_rate)?;

        // Correct the current number of shares using the newly updated stake
        self.shares = epoch_rate.convert_to_other_amount(self.stake.into());

        Ok(())
    }

    /// Process pending stake additions and pre-active cancellations for the current_epoch.
    fn process_pending_additions(&mut self, current_epoch: EpochNumber) -> Result<(), PoolError> {
        // Sum all pending stake before or at current_epoch
        let incoming = self.schedule.take_incoming(current_epoch);

        // Sum all pre-active cancellations before or at current_epoch
        let outgoing = self.schedule.take_outgoing(current_epoch);

        // Net pending stake must be non-negative
        if outgoing > incoming {
            return Err(PoolError::BalanceExceeded);
        }

        // Increase stake by net added stake
        let net_added = incoming - outgoing;
        if net_added > 0 {
            self.stake = self.stake.saturating_add(net_added.into());
        }

        Ok(())
    }

    /// Process pending share withdrawals for the current_epoch.
    fn process_pending_reductions(
        &mut self,
        current_epoch: EpochNumber,
        epoch_rate: ExchangeRate,
    ) -> Result<(), PoolError> {
        // Sum all pending shares withdrawing before or at current_epoch
        let outgoing_shares = self.schedule.take_outgoing_shares(current_epoch);

        // Convert shares to tape at provided epoch rate and remove from stake
        let net_removed = epoch_rate.convert_to_tape_amount(outgoing_shares);

        if self.stake < net_removed.into() {
            return Err(PoolError::BalanceExceeded);
        }

        if net_removed > 0 {
            self.stake = self.stake.saturating_sub(net_removed.into());
        }

        Ok(())
    }

    /// Add rewards for previous epoch, apply commission, and process pending I/O.
    /// Caller passes the previous exchange rate and receives the new rate for this epoch.
    pub fn advance_epoch(
        &mut self,
        current_epoch: EpochNumber,
        rewards_gross: Coin<TAPE>,
        prev_rate: ExchangeRate,
    ) -> Result<ExchangeRate, PoolError> {

        // Apply scheduled commission changes
        self.apply_commission_change(current_epoch);

        // Split rewards into commission and net, then add to pool stake
        if rewards_gross > TAPE::zero() {
            if self.stake.is_zero() {
                return Err(PoolError::StakeInvalid);
            }

            let commission_cut = (
                rewards_gross.as_u128() * self.commission_rate.as_u128() / BasisPoints::MAX as u128
            ) as u64;

            let rewards_net = rewards_gross.saturating_sub(commission_cut.into());

            self.commission = self.commission.saturating_add(commission_cut.into());
            self.rewards = self.rewards.saturating_add(rewards_net);
            self.stake = self.stake.saturating_add(rewards_net);
        }

        // Determine the exchange rate to use for this epoch (post-rewards).
        // If we have no shares yet, fall back to the provided prev_rate.
        let epoch_rate = if self.shares == 0 {
            prev_rate
        } else {
            ExchangeRate::new(self.stake.into(), self.shares)
        };

        // Process scheduled stake/withdrawals using this epoch's exchange rate
        self.process_pending_stake(current_epoch, epoch_rate)?;

        Ok(epoch_rate)
    }

    /// Stake tokens with this pool (schedules activation at E+2).
    pub fn stake(
        &mut self,
        current_epoch: EpochNumber,
        stake_amount: Coin<TAPE>,
    ) -> Result<StakedTape, PoolError> {
        if stake_amount.is_zero() {
            return Err(PoolError::ZeroStake);
        }

        let activation_epoch = current_epoch + EpochNumber(2);
        self.schedule
            .add_stake(activation_epoch, stake_amount)
            .map_err(|_| PoolError::ScheduleFailed)?;

        Ok(StakedTape {
            activation_epoch,
            amount: stake_amount,
            state: StakeState::new(),
        })
    }

    /// Request a withdrawal of stake from this pool.
    /// Caller provides the activation exchange rate for this stake.
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

        let withdraw_epoch = current_epoch + EpochNumber(2);

        // TODO: maybe a bug (check stake.activation_epoch vs E2)
        stake.set_withdrawing(withdraw_epoch);

        // If the stake activation was in the future, this is a pre-active cancel.
        if stake.activation_epoch > current_epoch {
            // Schedule the stake principal to be canceled at activation_epoch.
            // The net result is 0 change to tape_balance at that epoch for this stake.
            self.schedule
                .add_cancel(stake.activation_epoch, stake.amount)
                .map_err(|_| PoolError::ScheduleFailed)?;

            return Ok(withdraw_epoch);
        }

        // Otherwise, this is an active stake withdraw, so we need to schedule a share removal
        // which would calculate rewards at withdraw time.

        let shares = stake_activation_rate
            .convert_to_other_amount(stake.amount.into());

        if shares == 0 {
            return Err(PoolError::ZeroShares);
        }

        self.schedule
            .add_unstake(withdraw_epoch, shares)
            .map_err(|_| PoolError::ScheduleFailed)?;

        Ok(withdraw_epoch)
    }

    /// Withdrawn stake (capped by pool available rewards).
    /// Caller computes owed amount externally (e.g., via history or snapshot math).
    pub fn unstake(
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
}



#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }

    #[test]
    fn new_ok() {
        let p = StakingPool::<2>::new(BasisPoints(1000));

        assert_eq!(p.stake, TAPE::zero());
        assert_eq!(p.shares, 0);
        assert_eq!(p.commission_rate, BasisPoints(1000));
    }

    #[test]
    fn stake_sched() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let s = p.stake(epoch(5), tape(700)).unwrap();

        assert_eq!(s.activation_epoch, epoch(7));
        assert_eq!(p.schedule.incoming_sum(epoch(6)), 0);
        assert_eq!(p.schedule.incoming_sum(epoch(7)), 700);
    }

    #[test]
    fn advance_commission() {
        let mut p = StakingPool::<2>::new(BasisPoints(1000)); // 10%
        let flat = ExchangeRate::flat();

        // Activate 1_000 at E1 (raw insert for immediate activation)
        p.schedule.add_stake(epoch(1), tape(1_000)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        assert_eq!(p.stake, tape(1_000));

        // Add 500 gross at E2 → 10% commission, 450 net
        let _r2 = p.advance_epoch(epoch(2), tape(500), r1).unwrap();
        assert_eq!(p.commission, tape(50));
        assert_eq!(p.rewards, tape(450));
        assert_eq!(p.stake, tape(1_450));
    }

    #[test]
    fn adv_no_stake_err() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        let err = p.advance_epoch(epoch(1), tape(10), flat).unwrap_err();
        assert!(matches!(err, PoolError::StakeInvalid));
    }

    #[test]
    fn set_comm_next() {
        let mut p = StakingPool::<2>::new(BasisPoints(1000));
        let flat = ExchangeRate::flat();

        p.schedule.add_stake(epoch(1), tape(100)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();

        p.schedule.set_commission(epoch(4), BasisPoints(2000)).unwrap(); // applies at E4

        let r3 = p.advance_epoch(epoch(3), tape(0), r1).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(1000));

        p.advance_epoch(epoch(4), tape(0), r3).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(2000));
    }

    #[test]
    fn process_pend() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        p.schedule.add_stake(epoch(1), tape(1000)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();

        assert_eq!(p.stake, tape(1000));
        assert_eq!(p.shares, r1.convert_to_other_amount(p.stake.into()));
    }

    #[test]
    fn balance_proj() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        p.schedule.add_stake(epoch(1), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0), flat).unwrap(); // balance=1000

        // Schedule more stake for E5 and a withdraw at E6
        p.schedule.add_stake(epoch(5), tape(600)).unwrap();
        p.schedule.add_unstake(epoch(6), 200).unwrap();

        // Projection uses current rate (flat 1:1 here)
        assert_eq!(p.stake_at(epoch(4)), tape(1000));
        assert_eq!(p.stake_at(epoch(5)), tape(1600));
        assert_eq!(p.stake_at(epoch(6)), tape(1400));
    }

    #[test]
    fn pend_over_cancel_err() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        p.schedule.add_cancel(epoch(3), tape(200)).unwrap();
        p.schedule.add_stake(epoch(3), tape(100)).unwrap();
        let err = p.advance_epoch(epoch(3), tape(0), flat).unwrap_err();
        assert!(matches!(err, PoolError::BalanceExceeded));
    }

    #[test]
    fn tape_exceed_err() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        p.schedule.add_stake(epoch(1), tape(1000)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        p.schedule.add_unstake(epoch(2), 1500).unwrap();
        let err = p.advance_epoch(epoch(2), tape(0), r1).unwrap_err();
        assert!(matches!(err, PoolError::BalanceExceeded));
    }

    #[test]
    fn withdraw_sched_pre() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));

        // Create a pre-active stake at current=5 → activation=7
        let mut s = p.stake(epoch(5), tape(500)).unwrap();
        // Since activation is in the future, we can pass any dummy rate
        let we = p.request_withdraw(&mut s, epoch(5), ExchangeRate::new(0, 1)).unwrap();

        assert_eq!(we, epoch(7)); // current(5)+2
        assert_eq!(p.schedule.outgoing_sum(epoch(7)), 500);
        assert_eq!(p.schedule.outgoing_shares_sum(epoch(7)), 0);
    }

    #[test]
    fn withdraw_sched_act() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Stake at E1 → activation E3
        let mut s = p.stake(epoch(1), tape(1000)).unwrap();
        // Advance epochs so activation snapshot exists and stake is active
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap();
        let r3 = p.advance_epoch(epoch(3), tape(0), r2).unwrap();

        // Request at E3 → withdraw at E5; shares computed from rate at activation (E3)
        let activation_rate = r3;
        let we = p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        assert_eq!(we, epoch(5));
        assert_eq!(p.schedule.outgoing_shares_sum(epoch(5)), 1000); // flat
    }

    #[test]
    fn withdraw_pre_no_rewards() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Pre-active: stake at E5 → activation E7
        let mut s = p.stake(epoch(5), tape(500)).unwrap();
        p.request_withdraw(&mut s, epoch(5), ExchangeRate::new(0, 1)).unwrap(); // withdraw E7

        // Walk epochs
        let r6 = p.advance_epoch(epoch(6), tape(0), flat).unwrap();
        p.advance_epoch(epoch(7), tape(0), r6).unwrap();

        // No rewards owed for pre-active cancel
        let paid = p.unstake(&mut s, epoch(8), tape(0)).unwrap();
        assert_eq!(paid, tape(0));
    }

    #[test]
    fn withdraw_pay_cap() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Seed the pool so rewards can be earned.
        p.schedule.add_stake(epoch(1), tape(100)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();   // E1: balance=100
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap();     // E2
        let r3 = p.advance_epoch(epoch(3), tape(0), r2).unwrap();     // E3: snapshot exists

        // Create a user stake at current=E1 → activation=E3 (E+2)
        let mut s = p.stake(epoch(1), tape(100)).unwrap();

        // Request at E3 → withdraw epoch = E5 (E+2)
        let activation_rate = r3;
        p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        // Add rewards AFTER activation, so s accrues rewards (E4 only).
        let r4 = p.advance_epoch(epoch(4), tape(100), r3).unwrap();

        // Cap rewards pool to 10 to exercise the payout limit
        p.rewards = tape(10);

        // Ensure a snapshot exists at withdraw epoch
        let r5 = p.advance_epoch(epoch(5), tape(0), r4).unwrap();

        // Rewards owed (>10) but we cap at 10
        let shares = activation_rate.convert_to_other_amount(s.amount.into());
        let owed = tape(r5.convert_to_tape_amount(shares).saturating_sub(s.amount.into()));
        let paid = p.unstake(&mut s, epoch(5), owed).unwrap();
        assert_eq!(paid, tape(10));
        assert_eq!(p.rewards, tape(0));
    }

    #[test]
    fn withdraw_early_err() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Stake at E1 → activate E3
        let mut s = p.stake(epoch(1), tape(100)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap();
        let r3 = p.advance_epoch(epoch(3), tape(0), r2).unwrap();

        // Request at E3 → withdraw at E5
        let activation_rate = r3;
        p.request_withdraw(&mut s, epoch(3), activation_rate).unwrap();

        // Try to claim at E4 < withdraw → error
        let r4 = p.advance_epoch(epoch(4), tape(0), r3).unwrap(); // advance to E4
        let shares = activation_rate.convert_to_other_amount(s.amount.into());
        let owed = tape(r4.convert_to_tape_amount(shares).saturating_sub(s.amount.into()));
        let err = p.unstake(&mut s, epoch(4), owed).unwrap_err();
        assert!(matches!(err, PoolError::EpochInvalid));
    }

    #[test]
    fn alice_single() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // E0→E1: Alice stakes 1000 at E0 → activates E2
        let mut alice = p.stake(epoch(0), tape(1000)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap(); // E2: active

        // E3: pool earns 1000
        let r3 = p.advance_epoch(epoch(3), tape(1000), r2).unwrap();

        // Alice unstakes at E3 → withdraw at E5
        let activation_rate = r2; // activation snapshot
        let _we = p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();

        // E4: no rewards
        let r4 = p.advance_epoch(epoch(4), tape(0), r3).unwrap();

        // E5: claim (should be > 0)
        let r5 = p.advance_epoch(epoch(5), tape(0), r4).unwrap();
        let shares = activation_rate.convert_to_other_amount(alice.amount.into());
        let owed = tape(r5.convert_to_tape_amount(shares).saturating_sub(alice.amount.into()));
        let paid = p.unstake(&mut alice, epoch(5), owed).unwrap();

        assert!(paid > TAPE(0));
    }

    #[test]
    fn alice_bob_split() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // E0: both stake → activate E2
        let mut alice = p.stake(epoch(0), tape(1000)).unwrap();
        let mut bob   = p.stake(epoch(0), tape(1000)).unwrap();

        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap(); // E2: both active

        // E3: rewards 1000
        let r3 = p.advance_epoch(epoch(3), tape(1000), r2).unwrap();

        // Both request at E3 → withdraw E5
        let activation_rate = r2;
        let wa = p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();
        let wb = p.request_withdraw(&mut bob,   epoch(3), activation_rate).unwrap();
        assert_eq!(wa, epoch(5));
        assert_eq!(wb, epoch(5));

        // E4: settle, no rewards
        let r4 = p.advance_epoch(epoch(4), tape(0), r3).unwrap();
        // E5: claim
        let r5 = p.advance_epoch(epoch(5), tape(0), r4).unwrap();

        let shares = activation_rate.convert_to_other_amount(tape(1000).into());
        let owed_each = tape(r5.convert_to_tape_amount(shares).saturating_sub(1000));

        let ra = p.unstake(&mut alice, epoch(5), owed_each).unwrap();
        let rb = p.unstake(&mut bob,   epoch(5), owed_each).unwrap();

        // Rewards should split roughly equally (allow 1–2 units rounding drift)
        let diff = if ra > rb { ra - rb } else { rb - ra };
        assert!(diff.as_u64() <= 2);
    }

    #[test]
    fn commission_round() {
        let mut p = StakingPool::<2>::new(BasisPoints(1000)); // 10%
        let flat = ExchangeRate::flat();

        // E0 stake → activate E2
        let mut alice = p.stake(epoch(0), tape(1000)).unwrap();

        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();     // E1
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap();       // E2 active
        let r3 = p.advance_epoch(epoch(3), tape(202), r2).unwrap();     // E3 rewards gross=202 → commission=20, net=182

        assert_eq!(p.commission, tape(20));
        assert_eq!(p.rewards, tape(182));

        // Request at E3 → withdraw E5; no more rewards
        let activation_rate = r2;
        p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();
        let r4 = p.advance_epoch(epoch(4), tape(0), r3).unwrap();
        let r5 = p.advance_epoch(epoch(5), tape(0), r4).unwrap();

        let shares = activation_rate.convert_to_other_amount(alice.amount.into());
        let owed = tape(r5.convert_to_tape_amount(shares).saturating_sub(alice.amount.into()));
        let paid = p.unstake(&mut alice, epoch(5), owed).unwrap();

        assert!(paid <= tape(182));
        // Commission stays available
        assert_eq!(p.commission, tape(20));
    }

    #[test]
    fn early_blocked() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Stake at E1 → activate E3
        let mut alice = p.stake(epoch(1), tape(500)).unwrap();

        // Make sure activation snapshot exists
        let r2 = p.advance_epoch(epoch(2), tape(0), flat).unwrap();
        let r3 = p.advance_epoch(epoch(3), tape(0), r2).unwrap();

        // Request at E3 → withdraw E5
        let activation_rate = r3;
        p.request_withdraw(&mut alice, epoch(3), activation_rate).unwrap();

        // Trying to claim at E4 (< E5) must error
        let r4 = p.advance_epoch(epoch(4), tape(0), r3).unwrap();
        let shares = activation_rate.convert_to_other_amount(alice.amount.into());
        let owed = tape(r4.convert_to_tape_amount(shares).saturating_sub(alice.amount.into()));
        let err = p.unstake(&mut alice, epoch(4), owed).unwrap_err();
        assert!(matches!(err, PoolError::EpochInvalid));
    }

    #[test]
    fn maintain_ratio() {
        let mut p = StakingPool::<2>::new(BasisPoints(0));
        let flat = ExchangeRate::flat();

        // Alice stakes 1000 at E0 (E2 active)
        let mut alice = p.stake(epoch(0), tape(1000)).unwrap();
        let r1 = p.advance_epoch(epoch(1), tape(0), flat).unwrap();
        let r2 = p.advance_epoch(epoch(2), tape(0), r1).unwrap();

        // Bob stakes 2000 at E1 (E3 active)
        let mut bob = p.stake(epoch(1), tape(2000)).unwrap();
        let r3 = p.advance_epoch(epoch(3), tape(1000), r2).unwrap(); // Rewards when both are active

        // Alice requests at E3 → E5
        let activation_rate_e2 = r2;
        p.request_withdraw(&mut alice, epoch(3), activation_rate_e2).unwrap();

        // Bob requests at E4 → E6
        let r4 = p.advance_epoch(epoch(4), tape(1000), r3).unwrap();
        let activation_rate_e3 = r3;
        p.request_withdraw(&mut bob, epoch(4), activation_rate_e3).unwrap();

        // Walk to E5 and let Alice claim
        let r5 = p.advance_epoch(epoch(5), tape(0), r4).unwrap();
        let alice_shares = activation_rate_e2.convert_to_other_amount(alice.amount.into());
        let ra_owed = tape(r5.convert_to_tape_amount(alice_shares).saturating_sub(alice.amount.into()));
        let ra = p.unstake(&mut alice, epoch(5), ra_owed).unwrap();

        // Walk to E6 and let Bob claim
        let r6 = p.advance_epoch(epoch(6), tape(0), r5).unwrap();
        let bob_shares = activation_rate_e3.convert_to_other_amount(bob.amount.into());
        let rb_owed = tape(r6.convert_to_tape_amount(bob_shares).saturating_sub(bob.amount.into()));
        let rb = p.unstake(&mut bob, epoch(6), rb_owed).unwrap();

        // Basic sanity: both > 0, reflect different active windows
        assert!(ra > TAPE(0));
        assert!(rb > TAPE(0));
    }
}
