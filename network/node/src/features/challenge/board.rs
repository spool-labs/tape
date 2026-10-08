//! A node's ring of recent rounds and what it saw from each peer spool

use std::collections::VecDeque;
use std::time::Instant;

use tape_core::types::{EpochNumber, SlotNumber, SpoolIndex};

use crate::features::challenge::round::Round;

/// The board keeps this many rounds, about an hour at five slots a round on devnet
pub const RECENT_ROUNDS: usize = 4_300;

/// One answer to a poll carries at most this many rounds
pub const BOARD_REPLY_ROUNDS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ok,
    Invalid,
    WrongTrack,
}

/// A proof's outcome and how long after the round opened it arrived, in
/// milliseconds. The time is negative for a proof that arrived first.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Seen {
    pub outcome: Outcome,
    pub ms: i64,
}

/// One peer spool in one round. `seen` is empty while nothing has arrived.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    pub spool: SpoolIndex,
    pub seen: Option<Seen>,
}

/// How a node opened a round: when, and which epoch's holders answer in it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Opened {
    pub round: Round,
    pub at: Instant,
    pub answering: EpochNumber,
}

/// One round on the board. `opened` is empty for a round the node sat out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoardRound {
    pub slot: SlotNumber,
    pub opened: Option<Opened>,
    pub entries: Vec<Entry>,
}

/// The most recent rounds, oldest first
#[derive(Clone, Debug)]
pub struct Board {
    rounds: VecDeque<BoardRound>,
    capacity: usize,
}

impl Board {
    pub fn new(capacity: usize) -> Self {
        Self { rounds: VecDeque::new(), capacity }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.rounds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rounds.is_empty()
    }

    pub fn newest(&self) -> Option<SlotNumber> {
        self.rounds.back().map(|round| round.slot)
    }

    pub fn round(&self, slot: SlotNumber) -> Option<&BoardRound> {
        let index = self.rounds.binary_search_by_key(&slot, |round| round.slot).ok()?;
        self.rounds.get(index)
    }

    pub fn round_mut(&mut self, slot: SlotNumber) -> Option<&mut BoardRound> {
        let index = self.rounds.binary_search_by_key(&slot, |round| round.slot).ok()?;
        self.rounds.get_mut(index)
    }

    /// Copy the rounds after `slot`, oldest first, up to `limit` of them
    pub fn since(&self, slot: SlotNumber, limit: usize) -> Vec<BoardRound> {
        let first = self.rounds.partition_point(|round| round.slot <= slot);

        self.rounds
            .range(first..)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Add a round newer than every round held, pushing the oldest out when full
    pub fn push(&mut self, round: BoardRound) -> bool {
        if self.newest().is_some_and(|newest| round.slot <= newest) {
            return false;
        }

        if self.rounds.len() >= self.capacity {
            self.rounds.pop_front();
        }
        self.rounds.push_back(round);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sat_out(slot: u64) -> BoardRound {
        BoardRound { slot: SlotNumber(slot), opened: None, entries: Vec::new() }
    }

    // the ring keeps its newest rounds and drops the oldest
    #[test]
    fn ring() {
        let mut board = Board::new(3);

        for slot in [10, 15, 20, 25, 30] {
            assert!(board.push(sat_out(slot)));
        }

        assert_eq!(board.len(), 3);
        assert_eq!(board.newest(), Some(SlotNumber(30)));
        assert!(board.round(SlotNumber(15)).is_none());
        assert!(board.round(SlotNumber(20)).is_some());
        assert!(board.round_mut(SlotNumber(30)).is_some());
        assert!(board.round(SlotNumber(22)).is_none());
    }

    // a poll gets the rounds after its slot, oldest first, and no more than the cap
    #[test]
    fn since_slot() {
        let mut board = Board::new(10);
        for slot in [10, 15, 20, 25, 30] {
            board.push(sat_out(slot));
        }

        let slots = |rounds: Vec<BoardRound>| -> Vec<u64> {
            rounds.iter().map(|round| round.slot.0).collect()
        };

        assert_eq!(slots(board.since(SlotNumber(0), 10)), vec![10, 15, 20, 25, 30]);
        assert_eq!(slots(board.since(SlotNumber(15), 10)), vec![20, 25, 30]);
        assert_eq!(slots(board.since(SlotNumber(17), 2)), vec![20, 25]);
        assert!(board.since(SlotNumber(30), 10).is_empty());
    }

    // a round at or before the newest slot is refused
    #[test]
    fn in_order() {
        let mut board = Board::new(3);
        board.push(sat_out(20));

        assert!(!board.push(sat_out(20)));
        assert!(!board.push(sat_out(15)));
        assert_eq!(board.len(), 1);
    }

    // the board is sized to about an hour of rounds
    #[test]
    fn an_hour() {
        let slot_ms = 166;
        let cadence = 5;

        let hour = 3_600_000 / (slot_ms * cadence);

        assert!(RECENT_ROUNDS.abs_diff(hour) < 100);
    }
}
