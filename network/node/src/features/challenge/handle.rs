//! What the HTTP handlers share with the challenge task

use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use tape_core::encoding::ErasureCode;
use tape_core::types::SlotNumber;
use tape_crypto::Address;
use tokio::sync::mpsc;

use crate::features::challenge::board::{Board, BoardRound, RECENT_ROUNDS};
use crate::features::challenge::challenger::Challenger;
use crate::features::challenge::round::Batch;

/// The queue holds this many batches for the task before new ones are turned away
const INBOX_CAPACITY: usize = 1_024;

/// A peer's batch and when it reached this node
pub struct Arrival {
    pub sender: Address,
    pub batch: Batch,
    pub at: Instant,
}

/// The node's challenger, and the queue its peers' batches wait in
pub struct ChallengeHandle {
    is_enabled: bool,
    challenger: Mutex<Challenger>,
    arrivals: Mutex<Option<mpsc::Sender<Arrival>>>,
}

impl ChallengeHandle {
    pub fn new(node: Address, profile: ErasureCode, is_enabled: bool) -> Self {
        let challenger = Challenger::new(node, profile, Board::new(RECENT_ROUNDS));

        Self {
            is_enabled,
            challenger: Mutex::new(challenger),
            arrivals: Mutex::new(None),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.is_enabled
    }

    /// Open a fresh queue and hand the task its receiving end
    ///
    /// A task that starts later takes the queue over, so a runtime started
    /// again on the same context gets its peers' batches.
    pub fn attach(&self) -> mpsc::Receiver<Arrival> {
        let (arrivals, inbox) = mpsc::channel(INBOX_CAPACITY);
        *self.sender() = Some(arrivals);

        inbox
    }

    /// Queue a peer's batch for the task. It is turned away when the queue is
    /// full or no task is running.
    pub fn submit(&self, arrival: Arrival) -> bool {
        match self.sender().as_ref() {
            Some(arrivals) => arrivals.try_send(arrival).is_ok(),
            None => false,
        }
    }

    pub fn challenger(&self) -> MutexGuard<'_, Challenger> {
        self.challenger
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Copy the rounds after `slot`, oldest first, up to `limit` of them
    pub fn rounds_since(&self, slot: SlotNumber, limit: usize) -> Vec<BoardRound> {
        self.challenger().board().since(slot, limit)
    }

    fn sender(&self) -> MutexGuard<'_, Option<mpsc::Sender<Arrival>>> {
        self.arrivals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
