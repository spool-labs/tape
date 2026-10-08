//! In-memory buffer of confirmed blocks awaiting promotion.
//!
//! Maintains a single connected blockhash chain. A new block is appended
//! only after its `previous_blockhash` matches some entry's `blockhash`;
//! if it matches a middle entry, every entry after that point belonged to
//! a fork and gets dropped. The last promoted block is the floor the chain
//! has to keep descending from.
//!
//! Purely structural: appending and rolling back. Every block is fetched at
//! confirmed commitment, so the block ingestor promotes the whole queue.

use std::collections::VecDeque;
use std::sync::Arc;

use tape_crypto::Hash;

use crate::features::block::ingestor::ParsedBlock;

#[derive(Debug)]
pub enum AppendOutcome {
    /// Block chained cleanly to the tail, or was the first entry.
    Appended,

    /// Block chained to a non-tail entry — entries after that point are on a
    /// fork and have been dropped.
    Forked { dropped: Vec<Arc<ParsedBlock>> },

    /// Nothing queued or promoted chains to the block; the caller clears the queue and starts over
    ChainBroken,

    /// The block chains below the promoted head, so the promoted branch lost
    Diverged,
}

#[derive(Debug)]
pub struct PendingBlocks {
    entries: VecDeque<Arc<ParsedBlock>>,
    promoted: Option<Hash>,
}

impl PendingBlocks {
    /// `promoted` is the block durable state stands on, which the first block has to chain to
    pub fn new(promoted: Option<Hash>) -> Self {
        Self { entries: VecDeque::new(), promoted }
    }

    /// Append `block` if it chains. See `AppendOutcome` for the cases.
    pub fn append(&mut self, block: Arc<ParsedBlock>) -> AppendOutcome {
        let parent = self
            .entries
            .iter()
            .rposition(|entry| entry.blockhash == block.previous_blockhash);

        match parent {
            Some(pos) if pos + 1 == self.entries.len() => {
                self.entries.push_back(block);
                AppendOutcome::Appended
            }
            Some(pos) => {
                let dropped: Vec<_> = self.entries.drain((pos + 1)..).collect();
                self.entries.push_back(block);
                AppendOutcome::Forked { dropped }
            }
            // Nothing queued chains to it, so the promoted head decides; before any promotion it is trusted
            None => match self.promoted {
                Some(promoted) if promoted == block.previous_blockhash => {
                    let dropped = self.drain();
                    self.entries.push_back(block);
                    if dropped.is_empty() {
                        AppendOutcome::Appended
                    } else {
                        AppendOutcome::Forked { dropped }
                    }
                }
                Some(_) => AppendOutcome::Diverged,
                None if self.entries.is_empty() => {
                    self.entries.push_back(block);
                    AppendOutcome::Appended
                }
                None => AppendOutcome::ChainBroken,
            },
        }
    }

    /// Drain the queue, returning every entry. Used after a `ChainBroken`
    /// outcome to discard a stale fork.
    pub fn drain(&mut self) -> Vec<Arc<ParsedBlock>> {
        self.entries.drain(..).collect()
    }

    /// Promotes the head, which becomes the floor the chain has to keep descending from
    pub fn pop_front(&mut self) -> Option<Arc<ParsedBlock>> {
        let block = self.entries.pop_front()?;
        self.promoted = Some(block.blockhash);
        Some(block)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use tape_core::types::SlotNumber;
    use tape_crypto::tx::Txid;

    use super::*;

    fn block(slot: u64, blockhash: Hash, previous_blockhash: Hash) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(slot),
            blockhash,
            previous_blockhash,
            block_time: None,
            instructions: Vec::new(),
            instruction_tx_ids: Vec::<Txid>::new(),
        })
    }

    #[test]
    fn first_append_skips_chain_check() {
        let mut queue = PendingBlocks::new(None);
        let h = Hash::new_unique();
        let outcome = queue.append(block(10, h, Hash::new_unique()));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn chained_append_extends_tail() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        let outcome = queue.append(block(11, h1, h0));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn fork_drops_after_match_point() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        queue.append(block(11, h1, h0));
        queue.append(block(12, h2, h1));

        // New block at slot 12 chains to slot 10's blockhash, bypassing h1/h2.
        let h2b = Hash::new_unique();
        let outcome = queue.append(block(12, h2b, h0));
        match outcome {
            AppendOutcome::Forked { dropped } => {
                assert_eq!(dropped.len(), 2);
                assert_eq!(dropped[0].slot, SlotNumber(11));
                assert_eq!(dropped[1].slot, SlotNumber(12));
            }
            other => panic!("expected Forked, got {other:?}"),
        }
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.entries.back().expect("tail").blockhash, h2b);
    }

    // after a promotion the next block has to chain to the promoted head
    #[test]
    fn promoted_floor() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        queue.pop_front();
        assert!(queue.is_empty());

        let outcome = queue.append(block(12, Hash::new_unique(), Hash::new_unique()));
        assert!(matches!(outcome, AppendOutcome::Diverged));
        assert!(queue.is_empty());

        let outcome = queue.append(block(11, Hash::new_unique(), h0));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 1);
    }

    // a fork onto the promoted head drops only what was queued
    #[test]
    fn boundary_fork() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        queue.append(block(11, h1, h0));
        queue.append(block(12, h2, h1));
        queue.pop_front();

        let h1b = Hash::new_unique();
        match queue.append(block(11, h1b, h0)) {
            AppendOutcome::Forked { dropped } => {
                assert_eq!(dropped.len(), 2);
                assert_eq!(dropped[0].blockhash, h1);
                assert_eq!(dropped[1].blockhash, h2);
            }
            other => panic!("expected Forked, got {other:?}"),
        }
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.entries.back().expect("tail").blockhash, h1b);

        // Below the promoted head is the losing branch even with entries queued
        let outcome = queue.append(block(11, Hash::new_unique(), Hash::new_unique()));
        assert!(matches!(outcome, AppendOutcome::Diverged));
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn unmatched_parent_returns_broken_without_appending() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));

        let outcome = queue.append(block(11, Hash::new_unique(), Hash::new_unique()));
        assert!(matches!(outcome, AppendOutcome::ChainBroken));
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn skipped_slot_chains_via_blockhash() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        let h2 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        // Slot 11 was skipped; slot 12 chains to slot 10.
        let outcome = queue.append(block(12, h2, h0));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn drain_empties_queue() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        queue.append(block(11, h1, h0));

        let drained = queue.drain();
        assert_eq!(drained.len(), 2);
        assert!(queue.is_empty());
    }

    // every queued block promotes in order, with no gate on a later block
    #[test]
    fn pop_order() {
        let mut queue = PendingBlocks::new(None);
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()));
        queue.append(block(11, h1, h0));

        assert_eq!(queue.pop_front().expect("first block").slot, SlotNumber(10));
        assert_eq!(queue.pop_front().expect("second block").slot, SlotNumber(11));
        assert!(queue.pop_front().is_none());
    }
}
