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

    /// Called when a row lands below the tip, which is the only write that can
    /// join a set already built.
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
