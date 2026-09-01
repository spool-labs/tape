//! One round's attestations, gathered so a signer posts once per peer rather
//! than once per answer it verified.
//!
//! Sending is driven by arrivals rather than a clock, so a round's first
//! signature goes out at once and the rest ride the posts behind it.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Mutex;

use tokio::sync::watch;

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

/// Signatures gathered for a round, held rather than drained so every per-peer
/// sender reads them at its own cursor
struct Batch {
    held: Vec<(SpoolIndex, BlsSignature)>,
    /// Watched count; a notify would wake one of a round's many senders
    gathered: watch::Sender<usize>,
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
    /// Adds one signature, returning a watch on the count when it opened the
    /// batch. An open batch wakes the senders already carrying it.
    pub fn push(
        &self,
        key: BatchKey,
        spool: SpoolIndex,
        signature: BlsSignature,
    ) -> Option<watch::Receiver<usize>> {
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
                batch.gathered.send_replace(batch.held.len());
                None
            }
            Entry::Vacant(slot) => {
                let (gathered, watch) = watch::channel(1);
                slot.insert(Batch {
                    held: vec![(spool, signature)],
                    gathered,
                });
                Some(watch)
            }
        }
    }

    /// The signatures gathered past a sender's cursor.
    pub fn since(&self, key: &BatchKey, from: usize) -> Vec<(SpoolIndex, BlsSignature)> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .batches
            .get(key)
            .and_then(|batch| batch.held.get(from..))
            .map(<[_]>::to_vec)
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
        // Dropping the batch drops the watch, which ends every sender carrying it.
        state.batches.retain(|key, _| (key.epoch, key.round) >= (epoch, round));
    }

    /// Ends the senders for rounds seeded by a block that lost its fork.
    pub fn discard_block(&self, block: Hash) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.batches.retain(|key, _| key.block != block);
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

    // reading leaves the batch in place, so a later signature reaches the
    // senders already running rather than starting more of its own
    #[test]
    fn reading_does_not_reopen() {
        let queue = AttestQueue::default();
        queue.push(key(1), SpoolIndex(0), signature());

        assert_eq!(queue.since(&key(1), 0).len(), 1);
        assert!(queue.is_open(&key(1)));
        assert!(queue.push(key(1), SpoolIndex(1), signature()).is_none());
        assert_eq!(queue.since(&key(1), 1).len(), 1);
        // Every sender reads the same signatures, each from its own cursor.
        assert_eq!(queue.since(&key(1), 0).len(), 2);
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
