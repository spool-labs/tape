
use std::collections::BTreeMap;

// ========================
// Pool Exchange Rate
// ========================

#[derive(Debug, Clone, Copy)]
pub enum PoolExchangeRate {
    Flat, // 1:1
    Variable { wal_amount: u128, share_amount: u128 },
}

impl PoolExchangeRate {
    pub fn flat() -> Self {
        PoolExchangeRate::Flat
    }

    // If zero on either side, fallback to flat 1:1.
    pub fn new(wal_amount: u64, share_amount: u64) -> Self {
        if wal_amount == 0 || share_amount == 0 {
            PoolExchangeRate::Flat
        } else {
            PoolExchangeRate::Variable {
                wal_amount: wal_amount as u128,
                share_amount: share_amount as u128,
            }
        }
    }

    pub fn convert_to_wal_amount(&self, shares: u64) -> u64 {
        match *self {
            PoolExchangeRate::Flat => shares,
            PoolExchangeRate::Variable {
                wal_amount,
                share_amount,
            } => {
                let shares = shares as u128;
                ((shares * wal_amount) / share_amount) as u64
            }
        }
    }

    pub fn convert_to_share_amount(&self, wal: u64) -> u64 {
        match *self {
            PoolExchangeRate::Flat => wal,
            PoolExchangeRate::Variable {
                wal_amount,
                share_amount,
            } => {
                let wal = wal as u128;
                ((wal * share_amount) / wal_amount) as u64
            }
        }
    }
}

// ========================
// Pending Values (epoch -> amount)
// ========================

#[derive(Debug, Clone)]
pub struct PendingValues(BTreeMap<u32, u64>);

impl PendingValues {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert_or_add(&mut self, epoch: u32, value: u64) {
        *self.0.entry(epoch).or_insert(0) += value;
    }

    pub fn insert_or_replace(&mut self, epoch: u32, value: u64) {
        self.0.insert(epoch, value);
    }

    // reduce exactly for an epoch; error if missing or too large (matches move semantics)
    pub fn reduce(&mut self, epoch: u32, value: u64) {
        let entry = self
            .0
            .get_mut(&epoch)
            .expect("PendingValues: missing epoch value");
        assert!(*entry >= value, "PendingValues: reduce too large");
        *entry -= value;
    }

    // sum of values with e <= epoch
    pub fn value_at(&self, epoch: u32) -> u64 {
        self.0
            .range(..=epoch)
            .map(|(_, v)| *v)
            .sum()
    }

    // remove and sum all entries with e <= epoch
    pub fn flush(&mut self, epoch: u32) -> u64 {
        let keys: Vec<u32> = self.0.range(..=epoch).map(|(k, _)| *k).collect();
        let mut total = 0u64;
        for k in keys {
            if let Some(v) = self.0.remove(&k) {
                total += v;
            }
        }
        total
    }

    pub fn inner(&self) -> &BTreeMap<u32, u64> {
        &self.0
    }
}

// ========================
// Staked WAL
// ========================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StakedWalState {
    Staked,
    Withdrawing { withdraw_epoch: u32 },
}

#[derive(Debug, Clone)]
pub struct StakedWal {
    pub node_id: u64,
    pub principal: u64,
    pub activation_epoch: u32,
    pub state: StakedWalState,
}

impl StakedWal {
    pub fn new(node_id: u64, principal: u64, activation_epoch: u32) -> Self {
        Self {
            node_id,
            principal,
            activation_epoch,
            state: StakedWalState::Staked,
        }
    }

    pub fn is_staked(&self) -> bool {
        self.state == StakedWalState::Staked
    }

    pub fn is_withdrawing(&self) -> bool {
        matches!(self.state, StakedWalState::Withdrawing { .. })
    }

    pub fn withdraw_epoch(&self) -> Option<u32> {
        match self.state {
            StakedWalState::Withdrawing { withdraw_epoch } => Some(withdraw_epoch),
            _ => None,
        }
    }

    pub fn set_withdrawing(&mut self, epoch: u32) {
        assert!(self.is_staked(), "Already withdrawing");
        self.state = StakedWalState::Withdrawing { withdraw_epoch: epoch };
    }

    // With "always E+2" there is no early withdrawal path.
    pub fn can_withdraw_early(
        &self,
        _in_next_committee: bool,
        _current_epoch: u32,
    ) -> bool {
        false
    }
}

// ========================
// Staking Pool (with commission & rewards)
// ========================

const N_BASIS_POINTS: u16 = 10_000;

#[derive(Debug, Clone)]
pub struct StakingPool {
    // epoch -> exchange rate
    pub exchange_rates: BTreeMap<u32, PoolExchangeRate>,

    // core balances
    pub wal_balance: u64,   // net, excluding commission; grows by net rewards
    pub num_shares: u64,

    // epochs
    pub activation_epoch: u32,
    pub latest_epoch: u32,

    // reward accounting (pool-level rewards and commission)
    pub rewards_pool: u64,    // net rewards available to distribute to stakers
    pub commission: u64,      // accumulated commission
    pub commission_rate: u16, // bp
    pub pending_commission_rate: PendingValues, // epoch -> u64(bps), applied at epoch

    // pending stake and withdrawals
    pub pending_stake: PendingValues,            // activation_epoch -> principal
    pub pending_shares_withdraw: PendingValues,  // withdraw_epoch -> shares
    pub pre_active_withdrawals: PendingValues,   // activation_epoch -> principal canceled pre-active
}

impl StakingPool {
    pub fn new(activation_epoch: u32, initial_commission_rate_bps: u16) -> Self {
        let mut exchange_rates = BTreeMap::new();
        exchange_rates.insert(activation_epoch, PoolExchangeRate::flat());
        Self {
            exchange_rates,
            wal_balance: 0,
            num_shares: 0,
            activation_epoch,
            latest_epoch: activation_epoch.saturating_sub(1),
            rewards_pool: 0,
            commission: 0,
            commission_rate: initial_commission_rate_bps,
            pending_commission_rate: PendingValues::new(),
            pending_stake: PendingValues::new(),
            pending_shares_withdraw: PendingValues::new(),
            pre_active_withdrawals: PendingValues::new(),
        }
    }

    // Convenience: schedule stake to activate at current + 2.
    pub fn stake(&mut self, current_epoch: u32, amount_wal: u64) {
        let activation_epoch = current_epoch + 2;
        self.pending_stake.insert_or_add(activation_epoch, amount_wal);
    }

    // Backward search: from epoch down to activation_epoch; Flat if none found
    pub fn exchange_rate_at_epoch(&self, mut epoch: u32) -> PoolExchangeRate {
        while epoch >= self.activation_epoch {
            if let Some(rate) = self.exchange_rates.get(&epoch) {
                return *rate;
            }
            if epoch == 0 {
                break;
            }
            epoch -= 1;
        }
        PoolExchangeRate::flat()
    }

    // Apply commission rate update if one is scheduled for current_epoch.
    fn apply_pending_commission_rate(&mut self, current_epoch: u32) {
        if let Some(&new_rate_u64) = self.pending_commission_rate.inner().get(&current_epoch) {
            let new_rate = u16::try_from(new_rate_u64).expect("bps overflow");
            self.commission_rate = new_rate;
            // Clear all <= current
            self.pending_commission_rate.flush(current_epoch);
        }
    }

    // Process for current_epoch:
    // - snapshot exchange rate
    // - add net pending stake (added - pre-active cancellations) for current
    // - remove scheduled share withdrawals at current
    // - re-derive num_shares from current rate
    pub fn process_pending_stake(&mut self, current_epoch: u32) {
        // record current exchange rate snapshot
        let current_rate = PoolExchangeRate::new(self.wal_balance, self.num_shares);
        self.exchange_rates.insert(current_epoch, current_rate);

        // add net stake scheduled at current_epoch (subtract pre-active cancellations)
        let added = self.pending_stake.flush(current_epoch);
        let canceled_pre_active = self.pre_active_withdrawals.flush(current_epoch);
        assert!(
            added >= canceled_pre_active,
            "Calculation error: pre-active cancellations exceed pending stake"
        );
        let net_added = added - canceled_pre_active;
        self.wal_balance = self.wal_balance.saturating_add(net_added);

        // process share withdrawals scheduled for current_epoch
        let shares_withdraw = self.pending_shares_withdraw.flush(current_epoch);
        let wal_to_remove = current_rate.convert_to_wal_amount(shares_withdraw);
        assert!(
            self.wal_balance >= wal_to_remove,
            "Calculation error: withdraw > wal_balance"
        );
        self.wal_balance -= wal_to_remove;

        // re-derive num_shares
        self.num_shares = current_rate.convert_to_share_amount(self.wal_balance);
    }

    // Add rewards from previous epoch to this pool, split commission vs net rewards.
    // rewards_gross is the total earned by this pool in the previous epoch.
    pub fn advance_epoch(&mut self, current_epoch: u32, rewards_gross: u64) {
        assert!(
            current_epoch > self.latest_epoch,
            "Epoch already processed or not advancing"
        );

        // Commission scheduling applies at the beginning of the new epoch.
        self.apply_pending_commission_rate(current_epoch);

        if rewards_gross > 0 {
            // Sanity check: if there are rewards, pool must not be empty.
            assert!(
                self.wal_balance > 0,
                "Pool must have staked WAL to receive rewards"
            );

            // Split commission
            let commission_cut =
                (rewards_gross as u128 * self.commission_rate as u128 / N_BASIS_POINTS as u128)
                    as u64;
            let rewards_net = rewards_gross.saturating_sub(commission_cut);

            // Accumulate commission and rewards_pool
            self.commission = self.commission.saturating_add(commission_cut);
            self.rewards_pool = self.rewards_pool.saturating_add(rewards_net);

            // Net rewards increase pool's WAL balance
            self.wal_balance = self.wal_balance.saturating_add(rewards_net);
        }

        // Process stake/withdrawals for the new epoch
        self.process_pending_stake(current_epoch);
        self.latest_epoch = current_epoch;
    }

    // Schedule a commission rate change for E+2.
    pub fn set_next_commission(&mut self, current_epoch: u32, new_rate_bps: u16) {
        assert!(new_rate_bps <= N_BASIS_POINTS, "Invalid commission bps");
        let effective_epoch = current_epoch + 2;
        self.pending_commission_rate
            .insert_or_replace(effective_epoch, new_rate_bps as u64);
    }

    pub fn commission_amount(&self) -> u64 {
        self.commission
    }

    pub fn collect_commission(&mut self) -> u64 {
        let amt = self.commission;
        self.commission = 0;
        amt
    }

    pub fn rewards_amount(&self) -> u64 {
        self.rewards_pool
}

    // Always E+2: request withdrawal schedules:
    // - If pre-active (activation_epoch > current): record a pre-active cancel at activation_epoch.
    // - Else (already active): schedule shares removal at withdraw_epoch (= current + 2).
    pub fn request_withdraw_stake(
        &mut self,
        sw: &mut StakedWal,
        _in_current_committee: bool,
        _in_next_committee: bool,
        current_epoch: u32,
    ) {
        assert!(sw.principal > 0, "Zero stake");
        assert!(sw.is_staked(), "Not Staked");

        let withdraw_epoch = current_epoch + 2;

        if sw.activation_epoch > current_epoch {
            // Pre-active: never let it become active.
            // Record cancellation to net off the addition at activation_epoch.
            self.pre_active_withdrawals
                .insert_or_add(sw.activation_epoch, sw.principal);
            sw.set_withdrawing(withdraw_epoch);
            return;
        }

        // Already active: schedule shares removal at withdraw_epoch.
        let rate = self.exchange_rate_at_epoch(sw.activation_epoch);
        let shares = rate.convert_to_share_amount(sw.principal);
        assert!(shares > 0, "Zero shares");
        self.pending_shares_withdraw
            .insert_or_add(withdraw_epoch, shares);
        sw.set_withdrawing(withdraw_epoch);
    }

    // Compute rewards from activation_epoch to withdraw_epoch via exchange rates
    pub fn calculate_rewards(
        &self,
        staked_principal: u64,
        activation_epoch: u32,
        withdraw_epoch: u32,
    ) -> u64 {
        let at_activation = self.exchange_rate_at_epoch(activation_epoch);
        let shares = at_activation.convert_to_share_amount(staked_principal);
        let at_withdraw = self.exchange_rate_at_epoch(withdraw_epoch);
        let wal_out = at_withdraw.convert_to_wal_amount(shares);
        wal_out.saturating_sub(staked_principal)
    }

    // Withdraw stake (two-step only):
    // - Must be in Withdrawing state with withdraw_epoch <= current.
    // - If withdraw_epoch <= activation_epoch: pre-active cancel → return principal, no rewards.
    // - Else: rewards from activation_epoch to withdraw_epoch paid out of rewards_pool (capped).
    pub fn withdraw_stake(
        &mut self,
        mut sw: StakedWal,
        _in_current_committee: bool,
        _in_next_committee: bool,
        current_epoch: u32,
    ) -> (u64 /*principal*/, u64 /*rewards paid*/) {
        assert!(sw.principal > 0, "Zero stake");
        let we = sw
            .withdraw_epoch()
            .expect("Not in Withdrawing state");
        assert!(we <= current_epoch, "Withdraw epoch not reached");

        // Pre-active (never active long enough to accrue rewards).
        if we <= sw.activation_epoch {
            let principal = sw.principal;
            sw.principal = 0;
            return (principal, 0);
        }

        // Active case: pay rewards from activation -> we (capped by rewards_pool)
        let mut rewards = self.calculate_rewards(sw.principal, sw.activation_epoch, we);
        if rewards > self.rewards_pool {
            rewards = self.rewards_pool;
        }
        self.rewards_pool -= rewards;

        let principal = sw.principal;
        sw.principal = 0;
        (principal, rewards)
    }

    // Projected active WAL at epoch E.
    // Uses: wal_balance + (pending_stake.value_at(E) - pre_active_cancellations.value_at(E))
    // minus withdrawals (convert scheduled shares at E by current rate).
    pub fn wal_balance_at_epoch(&self, epoch: u32) -> u64 {
        let current_rate = PoolExchangeRate::new(self.wal_balance, self.num_shares);

        let stake_additions = self.pending_stake.value_at(epoch);
        let canceled_pre_active = self.pre_active_withdrawals.value_at(epoch);
        let net_additions = stake_additions.saturating_sub(canceled_pre_active);

        let shares_withdraw = self.pending_shares_withdraw.value_at(epoch);
        let withdrawals_wal = current_rate.convert_to_wal_amount(shares_withdraw);

        self.wal_balance
            .saturating_add(net_additions)
            .saturating_sub(withdrawals_wal)
    }
}

// ========================
// Example minimal usage
// ========================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_rewards_flow_with_commission() {
        // Pool activates at epoch 1, commission 10%
        let mut pool = StakingPool::new(1, 1_000);

        // E0: pending stake for E1 (caller controls this in this simplified model)
        pool.pending_stake.insert_or_add(1, 1_000);

        // Advance to E1 with no rewards
        pool.advance_epoch(1, 0);
        assert_eq!(pool.wal_balance, 1_000);
        assert_eq!(pool.rewards_pool, 0);
        assert_eq!(pool.commission, 0);

        // Next, suppose the pool earned 500 gross rewards in E1, added at start of E2
        // 10% commission => 50 commission, 450 net rewards
        pool.advance_epoch(2, 500);
        assert_eq!(pool.commission, 50);
        assert_eq!(pool.rewards_pool, 450);
        assert_eq!(pool.wal_balance, 1_000 + 450);

        // Set next commission to 20% (applies at E+2 => will apply at E4 if set at E2)
        pool.set_next_commission(2, 2_000);

        // E3, no rewards, just process pending stake/withdrawals
        pool.advance_epoch(3, 0);
        assert_eq!(pool.commission_rate, 1_000); // still 10%

        // E4, no rewards; pending commission takes effect
        pool.advance_epoch(4, 0);
        assert_eq!(pool.commission_rate, 2_000); // now 20%
    }

    #[test]
    fn two_step_withdraw_pays_from_rewards_pool() {
        let mut pool = StakingPool::new(1, 0);
        pool.pending_stake.insert_or_add(1, 100);

        // E1: stake becomes active
        pool.advance_epoch(1, 0);
        assert_eq!(pool.wal_balance, 100);

        // E2: pool earns 20 gross rewards, no commission => all net
        pool.advance_epoch(2, 20);
        assert_eq!(pool.wal_balance, 120);
        assert_eq!(pool.rewards_pool, 20);

        // Create stake object; request withdraw (always E+2 => withdraw_epoch=4)
        let mut sw = StakedWal::new(42, 100, 1);
        pool.request_withdraw_stake(&mut sw, true, true, 2);
        assert!(sw.is_withdrawing());
        assert_eq!(sw.withdraw_epoch(), Some(4));

        // E3/E4: process epochs
        pool.advance_epoch(3, 0);
        pool.advance_epoch(4, 0);

        // Now withdraw: rewards from E1->E4 are paid from rewards_pool (capped)
        let (principal, rewards) = pool.withdraw_stake(sw, true, true, 4);
        assert_eq!(principal, 100);
        assert_eq!(rewards, 20);
        assert_eq!(pool.rewards_pool, 0);
    }

    #[test]
    fn equal_wal_staked_at_different_epochs_mint_fewer_shares_later() {
        // Commission = 0 to focus on exchange-rate effects.
        let mut pool = StakingPool::new(/*activation_epoch=*/ 1, /*commission_bps=*/ 0);

        fn stake_and_mint_shares(
            pool: &mut StakingPool,
            who: &str,
            amount_wal: u64,
            activation_epoch: u32,
            apply_at_epoch: u32,
            rewards_gross_for_that_epoch: u64,
        ) -> u64 {
            let old = pool.num_shares;
            pool.pending_stake.insert_or_add(activation_epoch, amount_wal);
            pool.advance_epoch(apply_at_epoch, rewards_gross_for_that_epoch);
            let minted = pool.num_shares.saturating_sub(old);
            println!(
                "[{}] staked {} WAL at activation E{} -> applied E{} | minted {} shares",
                who, amount_wal, activation_epoch, apply_at_epoch, minted
            );
            minted
        }

        // E0 -> E1: Alice stakes 1000 WAL, becomes active at E1 (no rewards yet).
        let alice_minted = stake_and_mint_shares(&mut pool, "Alice", 1000, 1, 1, 0);
        assert_eq!(alice_minted, 1000);
        assert_eq!(pool.wal_balance, 1000);
        assert_eq!(pool.num_shares, 1000);

        // E1 -> E2: Earn rewards 200 WAL. Share price -> 1200 / 1000 = 1.2
        pool.advance_epoch(2, 200);
        assert_eq!(pool.wal_balance, 1200);
        assert_eq!(pool.num_shares, 1000);

        // E2 -> E3: Bob stakes 1000 (activation=3) → ~833 shares
        let bob_minted = stake_and_mint_shares(&mut pool, "Bob", 1000, 3, 3, 0);
        assert_eq!(bob_minted, 833);
        assert_eq!(pool.wal_balance, 2200);
        assert_eq!(pool.num_shares, 1833);

        // E3 -> E4: Earn 300 rewards
        pool.advance_epoch(4, 300);
        assert_eq!(pool.wal_balance, 2500);
        assert_eq!(pool.num_shares, 1833);

        // E4 -> E5: Charlie stakes 1000 (activation=5) → ~733 shares
        let charlie_minted = stake_and_mint_shares(&mut pool, "Charlie", 1000, 5, 5, 0);
        assert_eq!(charlie_minted, 733);
        assert_eq!(pool.wal_balance, 3500);
        assert_eq!(pool.num_shares, 2566);

        // E5 -> E6: Earn 600 rewards
        pool.advance_epoch(6, 600);
        assert_eq!(pool.wal_balance, 4100);
        assert_eq!(pool.num_shares, 2566);

        // E6 -> E7: Daisy stakes 1000 (activation=7) → ~625 shares
        let daisy_minted = stake_and_mint_shares(&mut pool, "Daisy", 1000, 7, 7, 0);
        assert_eq!(daisy_minted, 625);
        assert_eq!(pool.wal_balance, 5100);
        assert_eq!(pool.num_shares, 3191);

        assert!(alice_minted > bob_minted);
        assert!(bob_minted > charlie_minted);
        assert!(charlie_minted > daisy_minted);
    }
}
