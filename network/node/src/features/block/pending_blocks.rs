//! In-memory buffer of confirmed blocks awaiting finalization.
//!
//! Maintains a single connected blockhash chain. A new block is appended
//! only after its `previous_blockhash` matches some entry's `blockhash`;
//! if it matches a middle entry, every entry after that point belonged to
//! a fork and gets dropped.
//!
//! Purely structural: appending and rolling back. Promotion is decided by
//! the block ingestor (and, in the finality-gated path, the finalized tip
//! poller).

use std::collections::VecDeque;
use std::sync::Arc;

use tape_core::types::SlotNumber;

use crate::features::block::ingestor::ParsedBlock;

#[derive(Debug)]
pub enum AppendOutcome {
    /// Block chained cleanly to the tail, or was the first entry.
    Appended,

    /// Block chained to a non-tail entry — entries after that point are on a
    /// fork and have been dropped.
    Forked { dropped: Vec<Arc<ParsedBlock>> },

    /// No entry in the queue matched the block's `previous_blockhash`. The
    /// new block is not appended; the caller decides recovery (typically:
    /// clear the queue and start over).
    ChainBroken,
}

#[derive(Debug)]
struct PendingEntry {
    block: Arc<ParsedBlock>,
    /// Fetched at a slot already behind the finalized tip, so it cannot
    /// have been on a fork.
    finalized_when_fetched: bool,
}

#[derive(Debug, Default)]
pub struct PendingBlocks {
    entries: VecDeque<PendingEntry>,
}

impl PendingBlocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `block` if it chains. See `AppendOutcome` for the cases.
    pub fn append(&mut self, block: Arc<ParsedBlock>, finalized_when_fetched: bool) -> AppendOutcome {
        if self.entries.is_empty() {
            // First block since startup or since the queue was last cleared.
            // We do not persist the most-recently-applied blockhash, so the
            // first block has nothing to chain against and is trusted on the
            // basis that promotion only releases finalized blocks.
            self.entries.push_back(PendingEntry { block, finalized_when_fetched });
            return AppendOutcome::Appended;
        }

        let parent = self
            .entries
            .iter()
            .rposition(|entry| entry.block.blockhash == block.previous_blockhash);

        match parent {
            Some(pos) if pos == self.entries.len() - 1 => {
                self.entries.push_back(PendingEntry { block, finalized_when_fetched });
                AppendOutcome::Appended
            }
            Some(pos) => {
                let dropped: Vec<_> = self
                    .entries
                    .drain((pos + 1)..)
                    .map(|entry| entry.block)
                    .collect();
                self.entries.push_back(PendingEntry { block, finalized_when_fetched });
                AppendOutcome::Forked { dropped }
            }
            None => AppendOutcome::ChainBroken,
        }
    }

    /// Drain the queue, returning every entry. Used after a `ChainBroken`
    /// outcome to discard a stale fork.
    pub fn drain(&mut self) -> Vec<Arc<ParsedBlock>> {
        self.entries.drain(..).map(|entry| entry.block).collect()
    }

    pub fn pop_front(&mut self) -> Option<Arc<ParsedBlock>> {
        self.entries.pop_front().map(|entry| entry.block)
    }

    pub fn front(&self) -> Option<&Arc<ParsedBlock>> {
        self.entries.front().map(|entry| &entry.block)
    }

    pub fn back(&self) -> Option<&Arc<ParsedBlock>> {
        self.entries.back().map(|entry| &entry.block)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// A head that was already behind the finalized tip when fetched is
    /// always safe to promote: the RPC can only return the finalized block
    /// at such slots.
    ///
    /// A head fetched ahead of finality may have been on a fork. It is safe
    /// to promote once the finalized tip has passed its slot AND a later
    /// block is queued past the finalized tip: the queue chains by
    /// blockhash, so that later block proves the head's chain survived
    /// finalization.
    ///
    /// `finalized_tip` is the most recently observed finalized slot.
    pub fn front_promotable(&self, finalized_tip: SlotNumber) -> bool {
        let (Some(head), Some(tail)) = (self.entries.front(), self.entries.back()) else {
            return false;
        };
        if head.finalized_when_fetched {
            return true;
        }
        head.block.slot <= finalized_tip && tail.block.slot > finalized_tip
    }
}

#[cfg(test)]
mod tests {
    use tape_core::types::SlotNumber;
    use tape_crypto::Hash;
    use tape_crypto::tx::Txid;

    use super::*;

    fn block(slot: u64, blockhash: Hash, previous_blockhash: Hash) -> Arc<ParsedBlock> {
        Arc::new(ParsedBlock {
            slot: SlotNumber(slot),
            parent_slot: SlotNumber(slot.saturating_sub(1)),
            blockhash,
            previous_blockhash,
            block_time: None,
            instructions: Vec::new(),
            instruction_tx_ids: Vec::<Txid>::new(),
        })
    }

    impl PendingBlocks {
        /// Append a block fetched ahead of finality.
        fn append_confirmed(&mut self, block: Arc<ParsedBlock>) -> AppendOutcome {
            self.append(block, false)
        }
    }

    #[test]
    fn first_append_skips_chain_check() {
        let mut queue = PendingBlocks::new();
        let h = Hash::new_unique();
        let outcome = queue.append_confirmed(block(10, h, Hash::new_unique()));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn chained_append_extends_tail() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));
        let outcome = queue.append_confirmed(block(11, h1, h0));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn fork_drops_after_match_point() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));
        queue.append_confirmed(block(11, h1, h0));
        queue.append_confirmed(block(12, h2, h1));

        // New block at slot 12 chains to slot 10's blockhash, bypassing h1/h2.
        let h2b = Hash::new_unique();
        let outcome = queue.append_confirmed(block(12, h2b, h0));
        match outcome {
            AppendOutcome::Forked { dropped } => {
                assert_eq!(dropped.len(), 2);
                assert_eq!(dropped[0].slot, SlotNumber(11));
                assert_eq!(dropped[1].slot, SlotNumber(12));
            }
            other => panic!("expected Forked, got {other:?}"),
        }
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.back().unwrap().blockhash, h2b);
    }

    #[test]
    fn unmatched_parent_returns_broken_without_appending() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));

        let outcome = queue.append_confirmed(block(11, Hash::new_unique(), Hash::new_unique()));
        assert!(matches!(outcome, AppendOutcome::ChainBroken));
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn skipped_slot_chains_via_blockhash() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h2 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));
        // Slot 11 was skipped; slot 12 chains to slot 10.
        let outcome = queue.append_confirmed(block(12, h2, h0));
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn drain_empties_queue() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));
        queue.append_confirmed(block(11, h1, h0));

        let drained = queue.drain();
        assert_eq!(drained.len(), 2);
        assert!(queue.is_empty());
    }

    #[test]
    fn empty_queue_never_promotable() {
        let queue = PendingBlocks::new();
        assert!(!queue.front_promotable(SlotNumber(100)));
    }

    #[test]
    fn single_entry_waits_for_later_block() {
        // One block in queue, finalized has reached its slot, but no later
        // block queued past finalization → not promotable.
        let mut queue = PendingBlocks::new();
        let h = Hash::new_unique();
        queue.append_confirmed(block(10, h, Hash::new_unique()));
        assert!(!queue.front_promotable(SlotNumber(10)));
        assert!(!queue.front_promotable(SlotNumber(11)));
    }

    #[test]
    fn head_promotes_after_later_confirmed_block() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));
        queue.append_confirmed(block(11, h1, h0));
        // finalized_tip = 10, a later block is queued at slot 11.
        assert!(queue.front_promotable(SlotNumber(10)));
    }

    #[test]
    fn head_above_finalized_not_promotable() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append_confirmed(block(10, h0, Hash::new_unique()));
        queue.append_confirmed(block(11, h1, h0));
        // finalized_tip = 9, head at 10 not yet finalized.
        assert!(!queue.front_promotable(SlotNumber(9)));
    }

    #[test]
    fn already_finalized_head_promotes_alone() {
        let mut queue = PendingBlocks::new();
        let h = Hash::new_unique();
        queue.append(block(10, h, Hash::new_unique()), true);
        assert!(queue.front_promotable(SlotNumber(100)));
    }

    #[test]
    fn finalized_when_fetched_applies_per_entry() {
        let mut queue = PendingBlocks::new();
        let h0 = Hash::new_unique();
        let h1 = Hash::new_unique();
        queue.append(block(10, h0, Hash::new_unique()), true);
        queue.append(block(11, h1, h0), false);

        // The already-finalized head promotes; the successor was fetched
        // ahead of finality and must wait for a later block again.
        assert!(queue.front_promotable(SlotNumber(50)));
        queue.pop_front();
        assert!(!queue.front_promotable(SlotNumber(50)));
    }
}
