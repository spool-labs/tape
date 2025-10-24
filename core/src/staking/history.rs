use bytemuck::{Pod, Zeroable};
use crate::types::*;
use crate::system::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryError {
    RateMissing,
}

/// Externalized exchange-rate history for a staking pool.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PoolHistory<const N: usize> {
    history: PreviousRates<N>,
}

unsafe impl<const N: usize> Zeroable for PoolHistory<N> {}
unsafe impl<const N: usize> Pod for PoolHistory<N> {}

impl<const N: usize> PoolHistory<N> {
    pub fn new() -> Self {
        Self { history: PreviousRates::new() }
    }

    /// Record the exchange rate snapshot for an epoch.
    pub fn push(&mut self, epoch: EpochNumber, rate: ExchangeRate) {

        // TODO: add a merkle tree proof for older rates. 
        // Shapshots should add to a history root value.
        // (the current desing will work for *years*)

        self.history.push(epoch, rate);
    }

    /// Get the most recent rate at or before the given epoch.
    pub fn rate_at(&self, epoch: EpochNumber) -> Option<ExchangeRate> {
        self.history.on_or_before(epoch)
    }

    /// Compute rewards from activation_epoch to withdraw_epoch via stored exchange rates.
    pub fn calculate_rewards(
        &self,
        staked_principal: Coin<TAPE>,
        activation_epoch: EpochNumber,
        withdraw_epoch: EpochNumber,
    ) -> Result<Coin<TAPE>, HistoryError> {

        let at_activation = self.rate_at(activation_epoch)
            .ok_or(HistoryError::RateMissing)?;

        let at_withdraw = self.rate_at(withdraw_epoch)
            .ok_or(HistoryError::RateMissing)?;

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

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }
    fn rate(stake: u64, shares: u64) -> ExchangeRate {
        ExchangeRate::new(stake, shares)
    }

    #[test]
    fn new_and_rate_none() {
        let h = PoolHistory::<16>::new();
        assert!(h.rate_at(epoch(0)).is_none());
        assert!(h.rate_at(epoch(10)).is_none());
    }

    #[test]
    fn push_and_lookup() {
        let mut h = PoolHistory::<8>::new();
        h.push(epoch(2), rate(200, 100));
        h.push(epoch(5), rate(500, 200));

        assert_eq!(h.rate_at(epoch(1)), None);
        assert_eq!(h.rate_at(epoch(2)), Some(rate(200, 100)));
        assert_eq!(h.rate_at(epoch(3)), Some(rate(200, 100)));
        assert_eq!(h.rate_at(epoch(5)), Some(rate(500, 200)));
        assert_eq!(h.rate_at(epoch(6)), Some(rate(500, 200)));
    }

    #[test]
    fn calc_minimal() {
        let mut h = PoolHistory::<4>::new();

        // E1: 100 stake / 100 shares
        h.push(epoch(1), rate(100, 100));
        // E2: 120 stake / 100 shares
        h.push(epoch(2), rate(120, 100));

        // Rewards from E1 -> E2 on 100 principal = 20
        let r = h.calculate_rewards(tape(100), epoch(1), epoch(2)).unwrap();
        assert_eq!(r, tape(20));
    }

    #[test]
    fn rate_missing_err() {
        let mut h = PoolHistory::<2>::new();
        // Only push one rate at E2
        h.push(epoch(2), rate(200, 100));
        let err = h.calculate_rewards(tape(100), epoch(1), epoch(1)).unwrap_err();
        assert!(matches!(err, HistoryError::RateMissing));
    }
}
