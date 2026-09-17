//! The blacklists a gateway follows, consulted before every content read

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tape_blacklist::blacklists::{Blacklists, Changes};
use tape_blacklist::extract::Change;
use tape_blacklist::list::Skipped;
use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;
use tape_rack::reach::TrackRef;
use tape_rack::source::TapeSource;

/// The followed blacklists, and whether they have caught up since the gateway started
///
/// Reads vastly outnumber changes, hence the read-write lock. A writer that
/// persists content holds the read side across its check and its write, so a
/// change waits for that write and the purge after the change deletes it.
pub struct GatewayBlacklists {
    blacklists: RwLock<Blacklists>,
    is_caught_up: AtomicBool,
}

impl GatewayBlacklists {
    pub fn new(blacklists: Blacklists) -> Self {
        Self {
            blacklists: RwLock::new(blacklists),
            is_caught_up: AtomicBool::new(false),
        }
    }

    /// Whether every list has read everything the store holds for it
    pub fn is_caught_up(&self) -> bool {
        self.is_caught_up.load(Ordering::Acquire)
    }

    pub fn mark_caught_up(&self) {
        self.is_caught_up.store(true, Ordering::Release);
    }

    pub fn is_removed(&self, track: Address) -> bool {
        self.read().is_denied(&BlacklistEntry::track(track))
    }

    pub fn is_unlisted(&self, tape: Address) -> bool {
        self.read().is_denied(&BlacklistEntry::tape(tape))
    }

    /// Whether reads of a track are denied: the track is removed or its tape unlisted
    pub fn denies(&self, track: Address, tape: Address) -> bool {
        self.read().denies_track(track, tape)
    }

    /// Run a write of a track's content unless reads of the track are denied
    ///
    /// Returns `None` without running the write for a denied track.
    pub fn write_unless_denied<Output>(
        &self,
        track: Address,
        tape: Address,
        write: impl FnOnce() -> Output,
    ) -> Option<Output> {
        let blacklists = self.read();
        if blacklists.denies_track(track, tape) {
            return None;
        }
        Some(write())
    }

    pub fn rebuild<S: TapeSource>(&self, source: &S) -> Result<Changes, S::Error> {
        self.write().rebuild(source)
    }

    pub fn apply<S: TapeSource>(&self, changes: &[Change], source: &S) -> Result<Changes, S::Error> {
        self.write().apply(changes, source)
    }

    pub fn watches(&self, tape: Address) -> bool {
        self.read().watches(tape)
    }

    pub fn reached_tapes(&self) -> HashSet<Address> {
        self.read().reached_tapes()
    }

    pub fn missing(&self) -> Vec<TrackRef> {
        self.read().missing()
    }

    pub fn denied_count(&self) -> usize {
        self.read().denied_count()
    }

    pub fn skipped(&self) -> Vec<(String, Skipped)> {
        self.read().skipped()
    }

    fn read(&self) -> RwLockReadGuard<'_, Blacklists> {
        self.blacklists.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Blacklists> {
        self.blacklists.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for GatewayBlacklists {
    fn default() -> Self {
        Self::new(Blacklists::empty())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tape_blacklist::batch::{render, render_first};
    use tape_blacklist::entry::Entry;
    use tape_blacklist::subscription::{HonoredAction, Subscription};
    use tape_core::types::TrackNumber;
    use tape_rack::format::render_first as render_rack;
    use tape_rack::record::Record;
    use tape_rack::source::MemorySource;

    use super::*;

    /// A rack naming one blacklist tape, whose track 1 holds `entries`
    fn source(rack: Address, tape: Address, entries: &[Entry]) -> MemorySource {
        let mut source = MemorySource::default();
        source.reserve(rack);
        source.write(rack, TrackNumber(0), &render_rack(&[Record::new(tape)]));
        source.reserve(tape);
        source.write(tape, TrackNumber(0), &render_first(&[]).expect("render header"));
        source.write(tape, TrackNumber(1), &render(entries).expect("render entries"));
        source
    }

    fn following(rack: Address, source: &MemorySource) -> GatewayBlacklists {
        let subscription = Subscription {
            name: "list".to_string(),
            tape: rack,
            actions: vec![HonoredAction::Remove, HonoredAction::Unlist],
        };
        let blacklists =
            GatewayBlacklists::new(Blacklists::try_new(vec![subscription]).expect("subscriptions"));
        let rebuilt: Result<Changes, Infallible> = blacklists.rebuild(source);
        assert!(rebuilt.is_ok());
        blacklists
    }

    // a removed track and every track on an unlisted tape are denied, nothing else
    #[test]
    fn denied_reads() {
        let (rack, tape) = (Address::new_unique(), Address::new_unique());
        let (removed, unlisted, other) = (Address::new_unique(), Address::new_unique(), Address::new_unique());
        let source = source(rack, tape, &[Entry::remove(removed), Entry::unlist(unlisted)]);

        let blacklists = following(rack, &source);

        assert!(blacklists.denies(removed, other));
        assert!(blacklists.denies(Address::new_unique(), unlisted));
        assert!(!blacklists.denies(Address::new_unique(), other));
        assert!(blacklists.watches(tape));
        assert_eq!(blacklists.reached_tapes(), HashSet::from([rack, tape]));
    }

    // a write for a denied track never runs
    #[test]
    fn denied_write() {
        let (rack, tape) = (Address::new_unique(), Address::new_unique());
        let (track, content_tape) = (Address::new_unique(), Address::new_unique());
        let before = GatewayBlacklists::default().write_unless_denied(track, content_tape, || 7);

        let blacklists = following(rack, &source(rack, tape, &[Entry::remove(track)]));
        let after = blacklists.write_unless_denied(track, content_tape, || 7);

        assert_eq!(before, Some(7));
        assert_eq!(after, None);
    }

    // reads stay closed until catch-up is marked
    #[test]
    fn catch_up_flag() {
        let blacklists = GatewayBlacklists::default();
        assert!(!blacklists.is_caught_up());

        blacklists.mark_caught_up();

        assert!(blacklists.is_caught_up());
    }
}
