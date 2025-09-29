use bytemuck::{Pod, Zeroable};

use crate::{
    types::*,
    ring::*,
    coin::*,
};
use super::SystemError;
use super::utils::get_offsets;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RewardAccounting<const N: usize> {
    /// The rewards to be distributed in future epochs.
    rewards: RingBuffer<Coin::<TAPE>, N>,

    /// The current epoch number for index 0 in the usage buffer.
    now: EpochNumber,
}

unsafe impl<const N: usize> Zeroable for RewardAccounting<N> {}
unsafe impl<const N: usize> Pod for RewardAccounting<N> {}

impl<const N: usize> RewardAccounting<N> {
    pub fn new() -> Self {
        let now = EpochNumber(0);
        let mut rewards = RingBuffer::new();

        while rewards.len() < N {
            rewards.push(Coin::<TAPE>::zero());
        }

        Self { rewards, now }
    }

    /// Get the current epoch number.
    pub fn current_epoch(&self) -> EpochNumber {
        self.now
    }

    /// Advance to the next epoch, returning the rewards of the current epoch.
    pub fn advance_epoch(&mut self) -> Coin::<TAPE> {
        let current_rewards = *self.rewards
            .front()
            .unwrap_or(&Coin::<TAPE>::zero());

        // Push a new zeroed entry for the new future epoch
        self.rewards.push(Coin::<TAPE>::zero());

        // Advance the epoch number
        self.now.increment();

        current_rewards
    }

    /// Get the rewards for the provided epoch.
    #[inline]
    pub fn get(&self, epoch: EpochNumber) -> Result<Coin::<TAPE>, SystemError> {
        if epoch < self.now {
            return Err(SystemError::EpochInPast);
        }

        if epoch >= EpochNumber(self.now.as_u64() + N as u64) {
            return Err(SystemError::EpochTooFar);
        }

        let index = (epoch - self.now).as_u64() as usize;
        self.rewards.get(index).copied().ok_or(SystemError::IndexOutOfBounds)
    }

    /// Add rewards in the specified epoch range.
    pub fn add_rewards(
        &mut self,
        amount: Coin::<TAPE>,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<(), SystemError> {
        let (start_offset, end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        for i in start_offset..end_offset {
            let entry = self.rewards
                .get_mut(i)
                .ok_or(SystemError::IndexOutOfBounds)?;

            *entry = entry
                .checked_add(amount)
                .ok_or(SystemError::Overflow)?;
        }

        Ok(())
    }

    /// Slash rewards in the specified epoch range.
    pub fn slash_rewards(
        &mut self,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
        amount: Coin::<TAPE>,
    ) -> Result<(), SystemError> {
        let (start_offset, mut end_offset) = get_offsets::<N>(self.now, start_epoch, end_epoch)?;

        // Clamp to current length
        end_offset = end_offset.min(self.rewards.len());

        for i in start_offset..end_offset {
            let entry = self.rewards
                .get_mut(i)
                .ok_or(SystemError::IndexOutOfBounds)?;

            *entry = entry
                .checked_sub(amount)
                .ok_or(SystemError::Underflow)?;
        }

        Ok(())
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EpochNumber;
    use crate::coin::Coin;

    const N: usize = 5;

    #[test]
    fn test_future_rewards_new() {
        let db: RewardAccounting<N> = RewardAccounting::new();
        assert_eq!(db.now, EpochNumber(0));
        assert_eq!(db.rewards.len(), N);

        for i in 0..N {
            assert_eq!(
                db.get(EpochNumber(i as u64)).unwrap(),
                Coin::<TAPE>::zero()
            );
        }
    }

    #[test]
    fn test_future_rewards_get_rewards_at() {
        let db: RewardAccounting<N> = RewardAccounting::new();

        // Valid ranges
        for i in 0..N as u64 {
            assert_eq!(db.get(EpochNumber(i)).unwrap(), Coin::<TAPE>::zero());
        }

        // Errors
        assert_eq!(
            db.get(EpochNumber(u64::MAX)),
            Err(SystemError::EpochTooFar)
        );
    }

    #[test]
    fn test_future_rewards_advance_epoch() {
        let mut db: RewardAccounting<N> = RewardAccounting::new();

        for _ in 0..10 {
            let current = db.advance_epoch();
            assert_eq!(current, Coin::<TAPE>::zero());
            assert_eq!(db.rewards.len(), N);
        }

        assert_eq!(db.now, EpochNumber(10));
        for i in 0..N as u64 {
            assert_eq!(
                db.get(EpochNumber(10 + i)).unwrap(),
                Coin::<TAPE>::zero()
            );
        }
    }

    #[test]
    fn test_future_rewards_add_and_slash_rewards() {
        let mut db: RewardAccounting<N> = RewardAccounting::new();
        let amount = TAPE::new(100);

        // Add in epochs 1 to 3
        db.add_rewards(amount, EpochNumber(1), EpochNumber(3))
            .unwrap();

        assert_eq!(db.get(EpochNumber(0)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(1)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(2)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(4)).unwrap(), Coin::<TAPE>::zero());

        // Slash
        db.slash_rewards(EpochNumber(1), EpochNumber(3), amount)
            .unwrap();

        for i in 0..N as u64 {
            assert_eq!(db.get(EpochNumber(i)).unwrap(), Coin::<TAPE>::zero());
        }
    }

    #[test]
    fn test_future_rewards_add_errors() {
        let mut db: RewardAccounting<N> = RewardAccounting::new();
        let amount = TAPE::new(100);

        // Invalid ranges
        assert_eq!(
            db.add_rewards(amount, EpochNumber(0), EpochNumber(0)),
            Err(SystemError::EndNotAfterStart)
        );
        assert_eq!(
            db.add_rewards(amount, EpochNumber(0), EpochNumber(N as u64 + 1)),
            Err(SystemError::RangeTooLarge)
        );
        assert_eq!(
            db.add_rewards(amount, EpochNumber(N as u64), EpochNumber(N as u64 + 1)),
            Err(SystemError::ExceedsFutureEpochs)
        );

        // Overflow: assume max, add 1
        let max_amount = TAPE(u64::MAX);
        db.add_rewards(max_amount, EpochNumber(0), EpochNumber(1))
            .unwrap();
        assert_eq!(
            db.add_rewards(TAPE::new(1), EpochNumber(0), EpochNumber(1)),
            Err(SystemError::Overflow)
        );
    }

    #[test]
    fn test_future_rewards_slash_errors() {
        let mut db: RewardAccounting<N> = RewardAccounting::new();
        let amount = TAPE::new(100);

        // Underflow
        assert_eq!(
            db.slash_rewards(EpochNumber(0), EpochNumber(1), amount),
            Err(SystemError::Underflow)
        );

        // Invalid ranges
        assert_eq!(
            db.slash_rewards(EpochNumber(0), EpochNumber(0), amount),
            Err(SystemError::EndNotAfterStart)
        );
    }

    #[test]
    fn test_future_rewards_advance_with_additions() {
        let mut db: RewardAccounting<N> = RewardAccounting::new();
        let amount = TAPE::new(100);

        // Add in future epochs 2-4
        db.add_rewards(amount, EpochNumber(2), EpochNumber(4))
            .unwrap();

        assert_eq!(db.get(EpochNumber(0)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(1)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(2)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), Coin::<TAPE>::zero());

        // Advance once: return 0, now=1, new for 5=0
        let ret = db.advance_epoch();
        assert_eq!(ret, Coin::<TAPE>::zero());
        assert_eq!(db.now, EpochNumber(1));
        assert_eq!(db.get(EpochNumber(1)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(2)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(3)).unwrap(), amount);
        assert_eq!(db.get(EpochNumber(4)).unwrap(), Coin::<TAPE>::zero());
        assert_eq!(db.get(EpochNumber(5)).unwrap(), Coin::<TAPE>::zero());

        // Advance again: return 0, now=2
        let ret = db.advance_epoch();
        assert_eq!(ret, Coin::<TAPE>::zero());
        assert_eq!(db.now, EpochNumber(2));

        // Advance again: return amount, now=3
        let ret = db.advance_epoch();
        assert_eq!(ret, amount);
        assert_eq!(db.now, EpochNumber(3));
    }
}
