//! One round's attestations, gathered so a signer posts once per peer rather
//! than once per answer it verified.
//!
//! Sending is driven by arrivals rather than a clock, so a round's first
//! signature goes out at once and the rest ride the posts behind it.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use tape_core::bls::BlsSignature;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::hash::Hash;

/// The round a batch belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BatchKey {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
    pub round: RoundNumber,
    pub block: Hash,
}

/// Signatures waiting to be sent, and the handle that wakes their sender.
struct Batch {
    held: Vec<(SpoolIndex, BlsSignature)>,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct State {
    /// Signatures waiting to be sent, by round.
    batches: HashMap<BatchKey, Batch>,
    /// Oldest round still worth gathering for.
    floor: Option<(EpochNumber, RoundNumber)>,
}

/// Signatures waiting to be sent, by round.
#[derive(Default)]
pub struct AttestQueue {
    state: Mutex<State>,
}

impl AttestQueue {
    /// Adds one signature, returning the waker when it opened the batch.
    /// An open batch wakes its existing sender, so a round has exactly one.
    pub fn push(
        &self,
        key: BatchKey,
        spool: SpoolIndex,
        signature: BlsSignature,
    ) -> Option<Arc<Notify>> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.floor.is_some_and(|floor| (key.epoch, key.round) < floor) {
            return None;
        }

        match state.batches.entry(key) {
            Entry::Occupied(mut batch) => {
                let batch = batch.get_mut();
                batch.held.push((spool, signature));
                batch.wake.notify_one();
                None
            }
            Entry::Vacant(slot) => {
                let batch = slot.insert(Batch {
                    held: vec![(spool, signature)],
                    wake: Arc::new(Notify::new()),
                });
                Some(batch.wake.clone())
            }
        }
    }

    /// Takes what has gathered, leaving the batch open so later signatures
    /// join the sender already running.
    pub fn take(&self, key: &BatchKey) -> Vec<(SpoolIndex, BlsSignature)> {
        let Ok(mut state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .batches
            .get_mut(key)
            .map(|batch| core::mem::take(&mut batch.held))
            .unwrap_or_default()
    }

    /// Whether the round is still gathering, which is what ends its sender.
    pub fn is_open(&self, key: &BatchKey) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.batches.contains_key(key))
    }

    /// Ends senders for rounds at or behind the round buffer's cutoff.
    pub fn retire_before(&self, epoch: EpochNumber, round: RoundNumber) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.floor = Some((epoch, round));
        state.batches.retain(|key, batch| {
            if (key.epoch, key.round) >= (epoch, round) {
                return true;
            }
            batch.wake.notify_one();
            false
        });
    }

    /// Ends the senders for rounds seeded by a block that lost its fork.
    pub fn discard_block(&self, block: Hash) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.batches.retain(|key, batch| {
            if key.block != block {
                return true;
            }
            batch.wake.notify_one();
            false
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::bls::BlsPrivateKey;

    fn key(round: u64) -> BatchKey {
        BatchKey {
            epoch: EpochNumber(3),
            group: GroupIndex(1),
            round: RoundNumber(round),
            block: Hash([7; 32]),
        }
    }

    fn signature() -> BlsSignature {
        BlsPrivateKey::from_random().sign(b"attest queue").expect("sign")
    }

    #[test]
    fn one_sender_per_round() {
        let queue = AttestQueue::default();
        assert!(queue.push(key(1), SpoolIndex(0), signature()).is_some());
        assert!(queue.push(key(1), SpoolIndex(1), signature()).is_none());
        assert!(queue.push(key(2), SpoolIndex(0), signature()).is_some());
    }

    // draining leaves the batch open, so the next signature rides the sender
    // that is already running rather than starting one of its own
    #[test]
    fn draining_does_not_reopen() {
        let queue = AttestQueue::default();
        queue.push(key(1), SpoolIndex(0), signature());

        assert_eq!(queue.take(&key(1)).len(), 1);
        assert!(queue.is_open(&key(1)));
        assert!(queue.push(key(1), SpoolIndex(1), signature()).is_none());
        assert_eq!(queue.take(&key(1)).len(), 1);
    }

    #[test]
    fn retiring_closes_the_batch() {
        let queue = AttestQueue::default();
        queue.push(key(1), SpoolIndex(0), signature());
        queue.push(key(4), SpoolIndex(0), signature());

        queue.retire_before(EpochNumber(3), RoundNumber(4));
        assert!(!queue.is_open(&key(1)));
        assert!(queue.is_open(&key(4)));
    }

    // a signature arriving after its round retired would otherwise open a fresh
    // batch and leave a sender posting for a round nobody settles
    #[test]
    fn retired_rounds_do_not_reopen() {
        let queue = AttestQueue::default();
        queue.retire_before(EpochNumber(3), RoundNumber(4));

        assert!(queue.push(key(1), SpoolIndex(0), signature()).is_none());
        assert!(!queue.is_open(&key(1)));
        assert!(queue.push(key(4), SpoolIndex(0), signature()).is_some());
    }

    #[test]
    fn discarding_a_block_closes_its_rounds() {
        let queue = AttestQueue::default();
        queue.push(key(1), SpoolIndex(0), signature());

        queue.discard_block(Hash([7; 32]));
        assert!(!queue.is_open(&key(1)));
    }
}
