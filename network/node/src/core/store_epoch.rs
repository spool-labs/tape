use tape_core::types::EpochNumber;
use tokio::sync::watch;

/// The latest epoch whose first block the store has applied, or nothing before
/// the store's place on the chain is known.
#[derive(Debug)]
pub struct StoreEpoch {
    tx: watch::Sender<Option<EpochNumber>>,
}

impl StoreEpoch {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(None);

        Self { tx }
    }

    pub fn current(&self) -> Option<EpochNumber> {
        *self.tx.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<EpochNumber>> {
        self.tx.subscribe()
    }

    /// Record that the store has reached `epoch`. An epoch behind the recorded
    /// one changes nothing.
    pub fn reach(&self, epoch: EpochNumber) {
        self.tx.send_if_modified(|reached| {
            if reached.is_some_and(|reached| reached >= epoch) {
                return false;
            }

            *reached = Some(epoch);
            true
        });
    }
}

impl Default for StoreEpoch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // the recorded epoch only moves forward
    #[test]
    fn moves_forward() {
        let store_epoch = StoreEpoch::new();
        assert_eq!(store_epoch.current(), None);

        store_epoch.reach(EpochNumber(4));
        store_epoch.reach(EpochNumber(3));
        assert_eq!(store_epoch.current(), Some(EpochNumber(4)));

        store_epoch.reach(EpochNumber(5));
        assert_eq!(store_epoch.current(), Some(EpochNumber(5)));
    }

    // a reader waiting on the epoch wakes when it moves, and stays asleep when it does not
    #[tokio::test]
    async fn wakes_on_change() {
        let store_epoch = StoreEpoch::new();
        let mut reader = store_epoch.subscribe();

        store_epoch.reach(EpochNumber(4));
        reader.changed().await.expect("sender alive");
        assert_eq!(*reader.borrow_and_update(), Some(EpochNumber(4)));

        store_epoch.reach(EpochNumber(4));
        assert!(!reader.has_changed().expect("sender alive"));
    }
}
