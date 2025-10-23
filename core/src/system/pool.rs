use crate::types::*;
use bytemuck::{Pod, Zeroable};

use super::{
    exchange::*,
    value::*,
    staking::*,
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
    EpochNotReached,
    MustHaveStakedTape,
    InvalidStakeState,
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
    pub stake: Coin<TAPE>,

    /// The rewards this pool has earned from being active and available to distribute to stakers
    pub rewards: Coin<TAPE>,

    /// The commission (in TAPE) earned by the pool operator, available for withdrawal.
    pub commission: Coin<TAPE>,

    /// The totlal number of shares issued by this pool.
    pub num_shares: u64,

    /// The commission rate (in basis points, 1/100 of a percent) taken from rewards earned by this pool.
    pub commission_rate: BasisPoints,

    /// The pending commission rate changes, scheduled for future epochs.
    /// epoch -> u64(bps)
    pub commission_changes: PendingValues<M>,  

    /// The pending stake additions and share withdrawals, scheduled for future epochs.
    /// activation_epoch -> principal
    pub incoming_tokens: PendingValues<M>,            

    /// The pending pre-active stake cancellations, scheduled for future epochs.
    /// activation_epoch -> principal canceled pre-active
    pub outgoing_tokens: PendingValues<M>,   

    /// The pending share withdrawals, scheduled for future epochs.
    /// withdraw_epoch -> shares
    pub outgoing_shares: PendingValues<M>,  

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
            num_shares: 0,
            stake: Coin::<TAPE>::zero(),
            rewards: Coin::<TAPE>::zero(),
            commission: Coin::<TAPE>::zero(),
            commission_rate,
            incoming_tokens: PendingValues::new(),
            outgoing_shares: PendingValues::new(),
            outgoing_tokens: PendingValues::new(),
            commission_changes: PendingValues::new(),
            history: PreviousRates::new(),
        }
    }

    /// Get the most recent rate at or before the given epoch, 
    /// returning None if no such rate exists.
    pub fn get_exchange_rate(&self, epoch: EpochNumber) -> Option<ExchangeRate> {
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
            if self.stake == TAPE::zero() {
                return Err(PoolError::MustHaveStakedTape);
            }

            let commission_cut = (
                rewards_gross.as_u128() * self.commission_rate.as_u128() / BasisPoints::MAX as u128
            ) as u64;

            let rewards_net = rewards_gross
                .saturating_sub(commission_cut.into());

            self.commission = self.commission
                .saturating_add(commission_cut.into());

            self.rewards = self.rewards
                .saturating_add(rewards_net);

            self.stake = self.stake
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
        self.commission_changes
            .insert_or_replace(effective_epoch, new_rate.into())
            .map_err(|_| PoolError::FailedToScheduleCommission)?;

        Ok(())
    }

    /// Apply commission rate update if one is scheduled for current_epoch.
    pub fn apply_pending_commission_rate(&mut self, current_epoch: EpochNumber) {
        if let Some(&new_rate) = self.commission_changes.get(&current_epoch) {
            self.commission_rate = BasisPoints(new_rate);

            // Clear all <= current
            self.commission_changes.flush(current_epoch);
        }
    }

    /// Project the stake at a future epoch
    pub fn get_stake_at(&self, epoch: EpochNumber) -> Coin<TAPE> {
        // Calculate current exchange rate (stake per share)
        let exchange_rate = ExchangeRate::new(self.stake.into(), self.num_shares);

        // Calculate net token additions (incoming - outgoing)
        let incoming = self.incoming_tokens.value_at(epoch);
        let outgoing = self.outgoing_tokens.value_at(epoch);
        let net_additions = incoming.saturating_sub(outgoing);

        // Convert outgoing shares to token amount
        let outgoing_shares = self.outgoing_shares.value_at(epoch);
        let outgoing_tokens = exchange_rate.convert_to_tape_amount(outgoing_shares);

        // Compute final stake: current stake + net additions - outgoing tokens
        self.stake
            .as_u64()
            .saturating_add(net_additions)
            .saturating_sub(outgoing_tokens)
            .into()
    }

    /// Process pending stake/withdrawals for the current_epoch
    pub fn process_pending_stake(&mut self, current_epoch: EpochNumber) -> Result<(), PoolError> {
        let current_rate = ExchangeRate::new(
            self.stake.into(),
            self.num_shares
        );

        self.history.push(current_epoch, current_rate);

        // Handle stake increases (due to pending stake additions)
        self.process_pending_additions(current_epoch)?;

        // Handle stake reductions (due to pending share withdrawals)
        self.process_pending_reductions(current_epoch, current_rate)?;

        // Correct the current number of shares using the newly updated stake
        self.num_shares = current_rate
            .convert_to_other_amount(self.stake.into());

        Ok(())
    }

    /// Process pending stake additions and pre-active cancellations for the current_epoch.
    fn process_pending_additions(&mut self, current_epoch: EpochNumber) -> Result<(), PoolError> {
        // Sum all pending stake before or at current_epoch
        let incoming = self.incoming_tokens.flush(current_epoch);

        // Sum all pre-active cancellations before or at current_epoch
        let outgoing = self.outgoing_tokens.flush(current_epoch);

        // Net pending stake must be non-negative 
        // (this should be guaranteed by scheduling logic)
        if outgoing > incoming {
            return Err(PoolError::PendingStakeExceeded);
        }

        // Increase stake by net added stake
        let net_added = incoming - outgoing;
        if net_added > 0 {
            self.stake = self.stake
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
        let outgoing_shares = self.outgoing_shares.flush(current_epoch);

        // Convert shares to tape at current rate and remove from stake
        let net_removed = current_rate
            .convert_to_tape_amount(outgoing_shares);

        // The net balance to remove must not exceed current balance
        // (this should be guaranteed by scheduling logic)
        if self.stake < net_removed.into() {
            return Err(PoolError::TapeBalanceExceeded);
        }

        if net_removed > 0 {
            self.stake = self.stake
                .saturating_sub(net_removed.into());
        }

        Ok(())
    }

    /// Stake tokens with this pool.
    pub fn stake_with_pool(
        &mut self, 
        current_epoch: EpochNumber, 
        stake_amount: Coin<TAPE>
    ) -> Result<StakedTape, PoolError> {
        if stake_amount == TAPE::zero() {
            return Err(PoolError::ZeroStake);
        }

        // Activation is always E+2 for simplicity 
        // (may be changed later)
        let activation_epoch = current_epoch + EpochNumber(2);

        self.incoming_tokens
            .insert_or_add(activation_epoch, stake_amount.into())
            .map_err(|e| { println!("{:?}", e); PoolError::FailedToScheduleStake})?;

        Ok(StakedTape {
            activation_epoch,
            amount: stake_amount,
            state: StakeState::new(),
        })
    }

    /// Request a withdrawal of stake from this pool.
    pub fn unstake_from_pool(
        &mut self,
        stake: &mut StakedTape,
        current_epoch: EpochNumber,
    ) -> Result<EpochNumber, PoolError> {

        if !stake.is_staked() {
            return Err(PoolError::InvalidStakeState);
        }

        if stake.amount == TAPE::zero() {
            return Err(PoolError::MustHaveStakedTape);
        }

        // Withdrawals are always E+2 for simplicity
        // (may be changed later)
        let withdraw_epoch = current_epoch + EpochNumber(2);

        stake.set_withdrawing(withdraw_epoch);

        // If the stake activation was in the future, this is a pre-active cancel.

        if stake.activation_epoch > current_epoch {
            // Schedule the stake principal to be canceled at activation_epoch. 
            // The net result is 0 change to stake at that epoch for this stake.
            self.outgoing_tokens
                .insert_or_add(stake.activation_epoch, stake.amount.into())
                .map_err(|_| PoolError::FailedToScheduleStake)?;

            return Ok(withdraw_epoch);
        }

        // Otherwise, this is an active stake withdraw, so we need to schedule a share removal
        // which would calculate rewards at withdraw time.

        let stake_activation_rate = self
            .get_exchange_rate(stake.activation_epoch)
            .ok_or(PoolError::NoSuchRate)?;

        let num_shares = stake_activation_rate
            .convert_to_other_amount(stake.amount.into());

        if num_shares == 0 {
            return Err(PoolError::ZeroShares);
        }

        self.outgoing_shares
            .insert_or_add(withdraw_epoch, num_shares)
            .map_err(|_| PoolError::FailedToScheduleWithdraw)?;

        Ok(withdraw_epoch)
    }


    /// Claim rewards earned by a stake from activation to withdraw epoch.
    pub fn claim_stake_rewards(
        &mut self,
        stake: &mut StakedTape,
        current_epoch: EpochNumber,
    ) -> Result<Coin<TAPE>, PoolError> {

        if !stake.is_withdrawing() {
            return Err(PoolError::InvalidStakeState);
        }

        let stake_withdraw_epoch = stake
            .state
            .withdraw_epoch()
            .ok_or(PoolError::InvalidStakeState)?;

        if stake_withdraw_epoch > current_epoch {
            return Err(PoolError::EpochNotReached);
        }

        if stake.amount == TAPE::zero() {
            return Err(PoolError::MustHaveStakedTape);
        }

        stake.set_withdrawn();

        // If the withdraw epoch is before or at activation, then no rewards are due.
        if stake_withdraw_epoch <= stake.activation_epoch {
            return Ok(TAPE::zero());
        }

        // Otherwise, calculate rewards from the activation to the withdraw epoch.
        let mut rewards = self.calculate_rewards(
            stake.amount, 
            stake.activation_epoch, 
            stake_withdraw_epoch
        )?;

        if rewards > self.rewards {
            rewards = self.rewards;
        }

        self.rewards = self.rewards
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

        let at_activation = self.get_exchange_rate(activation_epoch)
            .ok_or(PoolError::NoSuchRate)?;

        let at_withdraw = self.get_exchange_rate(withdraw_epoch)
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

    // -------------------- Basics --------------------

    #[test]
    fn new_ok() {
        let p = P::new(BasisPoints(1000));
        assert_eq!(p.latest_epoch, epoch(0));
        assert_eq!(p.stake, TAPE::zero());
        assert_eq!(p.num_shares, 0);
        assert_eq!(p.commission_rate, BasisPoints(1000));
    }

    #[test]
    fn stake_sched() {
        let mut p = P::new(BasisPoints(0));
        let s = p.stake_with_pool(epoch(5), tape(700)).unwrap();
        // E+2 scheduling
        assert_eq!(s.activation_epoch, epoch(7));
        assert_eq!(p.incoming_tokens.value_at(epoch(6)), 0);
        assert_eq!(p.incoming_tokens.value_at(epoch(7)), 700);
    }

    #[test]
    fn rate_none() {
        let p = P::new(BasisPoints(0));
        assert!(p.get_exchange_rate(epoch(4)).is_none());
    }

    // -------------------- Epoch & commission --------------------

    #[test]
    fn adv_commission() {
        let mut p = P::new(BasisPoints(1000)); // 10%
        // Activate 1_000 at E1
        p.incoming_tokens.insert_or_add(epoch(1), 1_000).unwrap();
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
        let mut p = P::new(BasisPoints(0));
        let err = p.advance_epoch(epoch(1), tape(10)).unwrap_err();
        assert!(matches!(err, PoolError::MustHaveStakedTape));
    }

    #[test]
    fn epoch_dupe_err() {
        let mut p = P::new(BasisPoints(0));
        p.incoming_tokens.insert_or_add(epoch(1), 1).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        let err = p.advance_epoch(epoch(1), tape(0)).unwrap_err();
        assert!(matches!(err, PoolError::EpochAlreadyProcessed));
    }

    #[test]
    fn set_comm_next() {
        let mut p = P::new(BasisPoints(1000));
        p.incoming_tokens.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.set_next_commission(epoch(2), BasisPoints(2000)).unwrap(); // applies at E4
        p.advance_epoch(epoch(3), tape(0)).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(1000));
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        assert_eq!(p.commission_rate, BasisPoints(2000));
    }

    // -------------------- Pending processing & projections --------------------

    #[test]
    fn process_pend() {
        let mut p = P::new(BasisPoints(0));
        p.incoming_tokens.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        assert_eq!(p.stake, tape(1000));
        assert_eq!(p.num_shares, 1000); // flat at first snapshot
    }

    #[test]
    fn balance_proj() {
        let mut p = P::new(BasisPoints(0));
        p.incoming_tokens.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // balance=1000, shares=1000
        // Schedule more stake for E5 and a withdraw at E6
        p.incoming_tokens.insert_or_add(epoch(5), 600).unwrap();
        p.outgoing_shares.insert_or_add(epoch(6), 200).unwrap();
        // Projection uses current rate (flat 1:1 here)
        assert_eq!(p.get_stake_at(epoch(4)), tape(1000));
        assert_eq!(p.get_stake_at(epoch(5)), tape(1600));
        assert_eq!(p.get_stake_at(epoch(6)), tape(1400));
    }

    #[test]
    fn pend_over_cancel_err() {
        let mut p = P::new(BasisPoints(0));
        p.outgoing_tokens.insert_or_add(epoch(3), 200).unwrap();
        p.incoming_tokens.insert_or_add(epoch(3), 100).unwrap();
        let err = p.process_pending_stake(epoch(3)).unwrap_err();
        assert!(matches!(err, PoolError::PendingStakeExceeded));
    }

    #[test]
    fn tape_exceed_err() {
        let mut p = P::new(BasisPoints(0));
        p.incoming_tokens.insert_or_add(epoch(1), 1000).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // current_rate = 1000/1000; withdrawing 1500 shares exceeds balance
        p.outgoing_shares.insert_or_add(epoch(2), 1500).unwrap();
        let err = p.process_pending_stake(epoch(2)).unwrap_err();
        assert!(matches!(err, PoolError::TapeBalanceExceeded));
    }

    // -------------------- Unstake scheduling --------------------

    #[test]
    fn withdraw_sched_pre() {
        let mut p = P::new(BasisPoints(0));
        // Create a pre-active stake at current=5 → activation=7
        let mut s = p.stake_with_pool(epoch(5), tape(500)).unwrap();
        let we = p.unstake_from_pool(&mut s, epoch(5)).unwrap();
        assert_eq!(we, epoch(7)); // current(5)+2
        assert_eq!(p.outgoing_tokens.value_at(epoch(7)), 500);
        assert_eq!(p.outgoing_shares.value_at(epoch(7)), 0);
    }

    #[test]
    fn withdraw_sched_act() {
        let mut p = P::new(BasisPoints(0));
        // Stake at E1 → activation E3
        let mut s = p.stake_with_pool(epoch(1), tape(1000)).unwrap();
        // Advance epochs so activation snapshot exists and stake is active
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();
        // Unstake at E3 → withdraw at E5; shares computed from rate at activation (E3)
        let we = p.unstake_from_pool(&mut s, epoch(3)).unwrap();
        assert_eq!(we, epoch(5));
        assert_eq!(p.outgoing_shares.value_at(epoch(5)), 1000); // flat
    }

    // -------------------- Reward claiming --------------------

    #[test]
    fn withdraw_pre_no_rewards() {
        let mut p = P::new(BasisPoints(0));
        // Pre-active: stake at E5 → activation E7
        let mut s = p.stake_with_pool(epoch(5), tape(500)).unwrap();
        p.unstake_from_pool(&mut s, epoch(5)).unwrap(); // withdraw E7
        // Claim at/after E7 → zero rewards
        p.advance_epoch(epoch(6), tape(0)).unwrap();
        p.advance_epoch(epoch(7), tape(0)).unwrap();
        let r = p.claim_stake_rewards(&mut s, epoch(8)).unwrap();
        assert_eq!(r, tape(0));
    }

    #[test]
    fn withdraw_pay_cap() {
        let mut p = P::new(BasisPoints(0));

        // Seed the pool so rewards can be earned.
        p.incoming_tokens.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();   // E1: balance=100
        p.advance_epoch(epoch(2), tape(0)).unwrap();   // E2
        p.advance_epoch(epoch(3), tape(0)).unwrap();   // E3: snapshot exists

        // Create a user stake at current=E1 → activation=E3 (E+2)
        let mut s = p.stake_with_pool(epoch(1), tape(100)).unwrap();

        // Unstake at E3 → withdraw epoch = E5 (E+2)
        let we = p.unstake_from_pool(&mut s, epoch(3)).unwrap();
        assert_eq!(we, epoch(5));

        // Add rewards AFTER activation, so s accrues rewards (E4 only).
        p.advance_epoch(epoch(4), tape(100)).unwrap(); // now rewards=100, rate increases

        // Cap rewards pool to 10 to exercise the payout limit
        p.rewards = tape(10);

        // Ensure a snapshot exists at withdraw epoch
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        // Rewards owed (>10) but we cap at 10
        let paid = p.claim_stake_rewards(&mut s, epoch(5)).unwrap();
        assert_eq!(paid, tape(10));
        assert_eq!(p.rewards, tape(0));
    }

    #[test]
    fn withdraw_early_err() {
        let mut p = P::new(BasisPoints(0));
        // Stake at E1 → activation E3
        let mut s = p.stake_with_pool(epoch(1), tape(100)).unwrap();
        // Unstake at E3 → withdraw at E5
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();
        let _we = p.unstake_from_pool(&mut s, epoch(3)).unwrap();
        // Try to claim at E4 < withdraw → error
        let err = p.claim_stake_rewards(&mut s, epoch(4)).unwrap_err();
        assert!(matches!(err, PoolError::EpochNotReached));
    }

    #[test]
    fn calc_minimal() {
        let mut p = P::new(BasisPoints(0));
        // E1: +100 stake → shares 100
        p.incoming_tokens.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        // E2: +20 rewards; now rate = 120/100
        p.advance_epoch(epoch(2), tape(20)).unwrap();
        // Rewards from E1 -> E2 = 20
        let r = p.calculate_rewards(tape(100), epoch(1), epoch(2)).unwrap();
        assert_eq!(r, tape(20));
    }

    #[test]
    fn rate_missing_err() {
        // Keep only 2 rates
        type PS = PoolN<2, 2>;
        let mut p = PS::new(BasisPoints(0));
        p.incoming_tokens.insert_or_add(epoch(1), 100).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // snapshot E1
        p.advance_epoch(epoch(2), tape(0)).unwrap(); // snapshot E2
        p.advance_epoch(epoch(3), tape(0)).unwrap(); // snapshot E3 (E1 likely evicted)
        let err = p.calculate_rewards(tape(100), epoch(1), epoch(1)).unwrap_err();
        assert!(matches!(err, PoolError::NoSuchRate));
    }

    #[test]
    fn alice_single() {
        let mut p = P::new(BasisPoints(0));

        // E0→E1: Alice stakes 1000 at E0 → activates E2
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap(); // E1
        p.advance_epoch(epoch(2), tape(0)).unwrap(); // E2: active

        // E3: pool earns 1000
        p.advance_epoch(epoch(3), tape(1000)).unwrap();

        // Alice unstakes at E3 → withdraw at E5
        let we = p.unstake_from_pool(&mut alice, epoch(3)).unwrap();
        assert_eq!(we, epoch(5));

        // E4: no rewards
        p.advance_epoch(epoch(4), tape(0)).unwrap();

        // E5: claim (should get 1000 principal growth as rewards from E2→E5 window)
        p.advance_epoch(epoch(5), tape(0)).unwrap();
        let r = p.claim_stake_rewards(&mut alice, epoch(5)).unwrap();

        assert!(r > TAPE(0));
    }

    #[test]
    fn alice_bob_split() {
        let mut p = P::new(BasisPoints(0));

        // E0: both stake → activate E2
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();
        let mut bob   = p.stake_with_pool(epoch(0), tape(1000)).unwrap();

        p.advance_epoch(epoch(1), tape(0)).unwrap(); // E1
        p.advance_epoch(epoch(2), tape(0)).unwrap(); // E2: both active

        // E3: rewards 1000
        p.advance_epoch(epoch(3), tape(1000)).unwrap();

        // Both request at E3 → withdraw E5
        let wa = p.unstake_from_pool(&mut alice, epoch(3)).unwrap();
        let wb = p.unstake_from_pool(&mut bob,   epoch(3)).unwrap();
        assert_eq!(wa, epoch(5));
        assert_eq!(wb, epoch(5));

        // E4: settle, no rewards
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        // E5: claim
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        let ra = p.claim_stake_rewards(&mut alice, epoch(5)).unwrap();
        let rb = p.claim_stake_rewards(&mut bob,   epoch(5)).unwrap();

        // Rewards should split roughly equally (allow 1–2 units rounding drift)
        let diff = if ra > rb { ra - rb } else { rb - ra };
        assert!(diff.as_u64() <= 2);
    }

    #[test]
    fn commission_round() {
        let mut p = P::new(BasisPoints(1000)); // 10%
        // E0 stake → activate E2
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();

        p.advance_epoch(epoch(1), tape(0)).unwrap();     // E1
        p.advance_epoch(epoch(2), tape(0)).unwrap();     // E2 active
        p.advance_epoch(epoch(3), tape(202)).unwrap();   // E3 rewards gross=202 → commission=20, net=182

        assert_eq!(p.commission, tape(20));
        assert_eq!(p.rewards, tape(182));

        // Unstake at E3 → withdraw E5; no more rewards
        p.unstake_from_pool(&mut alice, epoch(3)).unwrap();
        p.advance_epoch(epoch(4), tape(0)).unwrap();
        p.advance_epoch(epoch(5), tape(0)).unwrap();

        let r = p.claim_stake_rewards(&mut alice, epoch(5)).unwrap();
        // Alice should receive net pool rewards accrued after activation
        assert!(r <= tape(182));
        // Commission stays available
        assert_eq!(p.commission, tape(20));
    }

    #[test]
    fn early_blocked() {
        let mut p = P::new(BasisPoints(0));
        // Stake at E1 → activate E3
        let mut alice = p.stake_with_pool(epoch(1), tape(500)).unwrap();

        // Make sure activation snapshot exists
        p.advance_epoch(epoch(2), tape(0)).unwrap();
        p.advance_epoch(epoch(3), tape(0)).unwrap();

        // Unstake at E3 → withdraw E5
        let _ = p.unstake_from_pool(&mut alice, epoch(3)).unwrap();

        // Trying to claim at E4 (< E5) must error
        let err = p.claim_stake_rewards(&mut alice, epoch(4)).unwrap_err();
        assert!(matches!(err, PoolError::EpochNotReached));
    }

    #[test]
    fn maintain_ratio() {
        let mut p = P::new(BasisPoints(0));

        // Alice stakes 1000 at E0 (E2 active)
        let mut alice = p.stake_with_pool(epoch(0), tape(1000)).unwrap();
        p.advance_epoch(epoch(1), tape(0)).unwrap();
        p.advance_epoch(epoch(2), tape(0)).unwrap();

        // Bob stakes 2000 at E1 (E3 active)
        let mut bob = p.stake_with_pool(epoch(1), tape(2000)).unwrap();
        p.advance_epoch(epoch(3), tape(1000)).unwrap(); // Rewards when both are (partly) active

        // Alice requests at E3 → E5
        let _ = p.unstake_from_pool(&mut alice, epoch(3)).unwrap();

        // Bob requests at E4 → E6
        p.advance_epoch(epoch(4), tape(1000)).unwrap();
        let _ = p.unstake_from_pool(&mut bob, epoch(4)).unwrap();

        // Walk to E5 and let Alice claim
        p.advance_epoch(epoch(5), tape(0)).unwrap();
        let ra = p.claim_stake_rewards(&mut alice, epoch(5)).unwrap();

        // Walk to E6 and let Bob claim
        p.advance_epoch(epoch(6), tape(0)).unwrap();
        let rb = p.claim_stake_rewards(&mut bob, epoch(6)).unwrap();

        // Basic sanity: both > 0, reflect different active windows
        assert!(ra > TAPE(0));
        assert!(rb > TAPE(0));
    }
}
