//! One round's sample set, held so every answer against it costs one scan.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, RoundNumber};

/// Rounds kept at once: one per group, with room for those still settling.
const KEPT: usize = 32;

type Key = (EpochNumber, RoundNumber, GroupIndex);

/// The sample sets of the rounds currently in flight.
///
/// Held until a backdated row lands. A write at the tip stamps far above any
/// live round's cutoff and cannot enter a set already built; only the snapshot
/// and sync paths, which backdate, can.
#[derive(Default)]
pub struct SampleSets<T> {
    held: Mutex<HashMap<Key, (u64, Arc<T>)>>,
    backdated: AtomicU64,
}

impl<T> SampleSets<T> {
    pub fn get(&self, key: &Key) -> Option<Arc<T>> {
        let now = self.backdated.load(Ordering::Acquire);
        let held = self.held.lock().ok()?;
        let (built_at, set) = held.get(key)?;
        (*built_at == now).then(|| set.clone())
    }

    /// The generation to stamp a set with, read before the store is.
    pub fn generation(&self) -> u64 {
        self.backdated.load(Ordering::Acquire)
    }

    /// Called after a backdated row lands; bumping first would stamp a stale set fresh
    pub fn backdated_write(&self) {
        self.backdated.fetch_add(1, Ordering::Release);
    }

    pub fn put(&self, key: Key, built_at: u64, set: Arc<T>) {
        let Ok(mut held) = self.held.lock() else { return };
        // Drop the oldest rounds rather than the whole map: a round still
        // settling would otherwise rebuild its set on every late attestation.
        if held.len() >= KEPT {
            let mut keys: Vec<Key> = held.keys().copied().collect();
            keys.sort_unstable();
            for stale in keys.into_iter().take(held.len() + 1 - KEPT) {
                held.remove(&stale);
            }
        }
        held.insert(key, (built_at, set));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Key {
        (EpochNumber(3), RoundNumber(1), GroupIndex(0))
    }

    // a set stamped before a backdated row lands is not served once it has
    #[test]
    fn backdated_row_invalidates() {
        let sets: SampleSets<u64> = SampleSets::default();

        let built_at = sets.generation();
        sets.put(key(), built_at, Arc::new(7));
        assert_eq!(sets.get(&key()).as_deref(), Some(&7));

        sets.backdated_write();

        assert!(sets.get(&key()).is_none());
    }

    // a set built after the row landed stands, because the scan already saw it
    #[test]
    fn later_set_stands() {
        let sets: SampleSets<u64> = SampleSets::default();
        sets.backdated_write();

        let built_at = sets.generation();
        sets.put(key(), built_at, Arc::new(7));

        assert_eq!(sets.get(&key()).as_deref(), Some(&7));
    }
}
