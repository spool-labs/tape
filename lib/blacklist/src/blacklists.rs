//! Following every subscription and combining what they deny

use std::collections::{HashMap, HashSet};

use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;
use tape_rack::reach::TrackRef;
use tape_rack::source::TapeSource;

use crate::error::BlacklistError;
use crate::extract::Change;
use crate::list::{List, ListChanges, Skipped};
use crate::subscription::{Subscription, validate};

/// What changed across every subscription
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Changes {
    /// Targets denied now that were not before
    pub denied: Vec<BlacklistEntry>,
    /// Targets no subscription denies any more
    pub allowed: Vec<BlacklistEntry>,
}

struct Followed {
    subscription: Subscription,
    list: List,
}

/// A consumer's subscriptions, followed and combined
pub struct Blacklists {
    lists: Vec<Followed>,
    /// How many subscriptions deny each denied target
    denied: HashMap<BlacklistEntry, usize>,
}

impl Blacklists {
    /// Follow nothing: no target is denied
    pub fn empty() -> Self {
        Self {
            lists: Vec::new(),
            denied: HashMap::new(),
        }
    }

    /// Follow every subscription, with nothing read yet
    pub fn try_new(subscriptions: Vec<Subscription>) -> Result<Self, BlacklistError> {
        validate(&subscriptions)?;
        let mut lists = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            lists.push(Followed {
                list: List::new(subscription.tape),
                subscription,
            });
        }
        Ok(Self {
            lists,
            denied: HashMap::new(),
        })
    }

    /// Read every list again from the source
    pub fn rebuild<S: TapeSource>(&mut self, source: &S) -> Result<Changes, S::Error> {
        let mut touched = Vec::new();
        for followed in &mut self.lists {
            let list_changes = followed.list.rebuild(source)?;
            combine(&followed.subscription, list_changes, &mut self.denied, &mut touched);
        }
        Ok(settle(touched, &self.denied))
    }

    /// Apply what a block or a fetch changed, once the source reflects it
    pub fn apply<S: TapeSource>(&mut self, changes: &[Change], source: &S) -> Result<Changes, S::Error> {
        let mut touched = Vec::new();
        for followed in &mut self.lists {
            let list_changes = followed.list.apply(changes, source)?;
            combine(&followed.subscription, list_changes, &mut self.denied, &mut touched);
        }
        Ok(settle(touched, &self.denied))
    }

    /// Whether a change to a tape can matter to any list
    pub fn watches(&self, tape: Address) -> bool {
        self.lists.iter().any(|followed| followed.list.watches(tape))
    }

    /// Every live tape a subscription reaches
    pub fn reached_tapes(&self) -> HashSet<Address> {
        let mut tapes = HashSet::new();
        for followed in &self.lists {
            tapes.extend(followed.list.reached().iter().copied());
        }
        tapes
    }

    /// Present tracks whose bytes some list still needs
    pub fn missing(&self) -> Vec<TrackRef> {
        let mut missing = HashMap::new();
        for followed in &self.lists {
            for track in followed.list.missing() {
                missing.insert(track.address, track);
            }
        }
        missing.into_values().collect()
    }

    /// Whether every list has every byte it needs
    pub fn is_caught_up(&self) -> bool {
        self.lists.iter().all(|followed| followed.list.missing().is_empty())
    }

    /// Whether any subscription denies the target
    pub fn is_denied(&self, target: &BlacklistEntry) -> bool {
        self.denied.contains_key(target)
    }

    /// Whether reads of a track are denied: the track is removed or its tape unlisted
    pub fn denies_track(&self, track: Address, tape: Address) -> bool {
        self.is_denied(&BlacklistEntry::track(track)) || self.is_denied(&BlacklistEntry::tape(tape))
    }

    pub fn denied_count(&self) -> usize {
        self.denied.len()
    }

    /// Every denied target
    pub fn denied(&self) -> Vec<BlacklistEntry> {
        self.denied.keys().copied().collect()
    }

    /// Tracks each list skipped, by subscription name
    pub fn skipped(&self) -> Vec<(String, Skipped)> {
        let mut skipped = Vec::new();
        for followed in &self.lists {
            for track in followed.list.skipped() {
                skipped.push((followed.subscription.name.clone(), track));
            }
        }
        skipped
    }
}

/// Fold one list's changes into the union, as far as its subscription honors them
fn combine(
    subscription: &Subscription,
    list_changes: ListChanges,
    denied: &mut HashMap<BlacklistEntry, usize>,
    touched: &mut Vec<(BlacklistEntry, bool)>,
) {
    for target in list_changes.denied {
        if subscription.honors(&target) {
            note(touched, denied, target);
            *denied.entry(target).or_insert(0) += 1;
        }
    }
    for target in list_changes.allowed {
        if subscription.honors(&target) {
            note(touched, denied, target);
            release(denied, target);
        }
    }
}

fn settle(touched: Vec<(BlacklistEntry, bool)>, denied: &HashMap<BlacklistEntry, usize>) -> Changes {
    let mut changes = Changes::default();
    for (target, was_denied) in touched {
        match (was_denied, denied.contains_key(&target)) {
            (false, true) => changes.denied.push(target),
            (true, false) => changes.allowed.push(target),
            (true, true) | (false, false) => {}
        }
    }
    changes
}

/// Remember whether a target was denied before it was first touched
fn note(touched: &mut Vec<(BlacklistEntry, bool)>, denied: &HashMap<BlacklistEntry, usize>, target: BlacklistEntry) {
    if !touched.iter().any(|(seen, _)| *seen == target) {
        touched.push((target, denied.contains_key(&target)));
    }
}

/// Drop one subscription's denial of a target
fn release(denied: &mut HashMap<BlacklistEntry, usize>, target: BlacklistEntry) {
    let is_last = match denied.get_mut(&target) {
        Some(count) => {
            *count = count.saturating_sub(1);
            *count == 0
        }
        None => false,
    };
    if is_last {
        denied.remove(&target);
    }
}

#[cfg(test)]
mod tests {
    use tape_core::types::{EpochNumber, TrackNumber};
    use tape_rack::format::{render as render_rack_records, render_first as render_rack};
    use tape_rack::record::Record;
    use tape_rack::source::MemorySource;

    use super::*;
    use crate::batch::{render, render_first};
    use crate::entry::Entry;
    use crate::subscription::HonoredAction;

    const BOTH: &[HonoredAction] = &[HonoredAction::Remove, HonoredAction::Unlist];
    const FIRST: TrackNumber = TrackNumber(0);

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn subscription(name: &str, tape: Address, actions: &[HonoredAction]) -> Subscription {
        Subscription {
            name: name.to_string(),
            tape,
            actions: actions.to_vec(),
        }
    }

    /// A reserved rack whose track 0 names `tapes`
    fn rack(source: &mut MemorySource, rack: Address, tapes: &[Address]) {
        let records: Vec<Record> = tapes.iter().map(|tape| Record::new(*tape)).collect();
        source.reserve(rack);
        source.write(rack, FIRST, &render_rack(&records));
    }

    /// A reserved blacklist tape with its header
    fn blacklist_tape(source: &mut MemorySource, tape: Address) {
        source.reserve(tape);
        source.write(tape, FIRST, &render_first(&[]).expect("render header"));
    }

    fn batch(source: &mut MemorySource, tape: Address, number: u64, entries: &[Entry]) -> Address {
        source.write(tape, TrackNumber(number), &render(entries).expect("render batch"))
    }

    fn follow(subscriptions: Vec<Subscription>, source: &MemorySource) -> Blacklists {
        let mut blacklists = Blacklists::try_new(subscriptions).expect("subscriptions");
        infallible(blacklists.rebuild(source));
        blacklists
    }

    fn apply(blacklists: &mut Blacklists, changes: &[Change], source: &MemorySource) -> Changes {
        infallible(blacklists.apply(changes, source))
    }

    fn infallible<T>(result: Result<T, std::convert::Infallible>) -> T {
        match result {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }

    // a rack's blacklist tapes deny what their tracks name
    #[test]
    fn named_tapes() {
        let (root, tape, removed, unlisted) = (address(1), address(2), address(9), address(8));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::remove(removed), Entry::unlist(unlisted)]);

        let blacklists = follow(vec![subscription("a", root, BOTH)], &source);

        assert!(blacklists.is_denied(&BlacklistEntry::track(removed)));
        assert!(blacklists.denies_track(address(42), unlisted));
        assert!(blacklists.is_caught_up());
    }

    // a subscription to a blacklist tape itself is a list of that one tape
    #[test]
    fn direct_tape() {
        let (tape, removed, unlisted) = (address(2), address(9), address(8));
        let mut source = MemorySource::default();
        source.reserve(tape);
        let header = render_first(&[Entry::remove(removed)]).expect("render header");
        source.write(tape, FIRST, &header);
        batch(&mut source, tape, 1, &[Entry::unlist(unlisted)]);

        let blacklists = follow(vec![subscription("a", tape, BOTH)], &source);

        assert!(blacklists.is_denied(&BlacklistEntry::track(removed)));
        assert!(blacklists.denies_track(address(42), unlisted));
        assert!(blacklists.is_caught_up());
    }

    // deleting the track that names a target allows it again
    #[test]
    fn deleted_track() {
        let (root, tape, removed) = (address(1), address(2), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::remove(removed)]);
        let mut blacklists = follow(vec![subscription("a", root, BOTH)], &source);

        let track = source.remove(tape, TrackNumber(1)).expect("written track");
        let changes = apply(&mut blacklists, &[Change::Removed { track }], &source);

        assert_eq!(changes.allowed, vec![BlacklistEntry::track(removed)]);
        assert!(!blacklists.is_denied(&BlacklistEntry::track(removed)));
    }

    // a target two lists deny stays denied until both stop naming it
    #[test]
    fn lists_combine() {
        let (rack_a, tape_a, rack_b, tape_b, removed) = (address(1), address(2), address(3), address(4), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, rack_a, &[tape_a]);
        rack(&mut source, rack_b, &[tape_b]);
        blacklist_tape(&mut source, tape_a);
        blacklist_tape(&mut source, tape_b);
        batch(&mut source, tape_a, 1, &[Entry::remove(removed)]);
        batch(&mut source, tape_b, 1, &[Entry::remove(removed)]);
        let mut blacklists = follow(
            vec![subscription("a", rack_a, BOTH), subscription("b", rack_b, BOTH)],
            &source,
        );

        let track_a = source.remove(tape_a, TrackNumber(1)).expect("track a");
        let first = apply(&mut blacklists, &[Change::Removed { track: track_a }], &source);
        let track_b = source.remove(tape_b, TrackNumber(1)).expect("track b");
        let second = apply(&mut blacklists, &[Change::Removed { track: track_b }], &source);

        assert_eq!(first, Changes::default());
        assert_eq!(second.allowed, vec![BlacklistEntry::track(removed)]);
    }

    // a subscription that honors only removals ignores its list's unlistings
    #[test]
    fn honored_actions() {
        let (root, tape, unlisted) = (address(1), address(2), address(8));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::unlist(unlisted)]);

        let blacklists = follow(vec![subscription("a", root, &[HonoredAction::Remove])], &source);

        assert!(!blacklists.denies_track(address(42), unlisted));
    }

    // a track that does not parse is skipped and the list's other tracks still count
    #[test]
    fn skipped_track() {
        let (root, tape, removed) = (address(1), address(2), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        blacklist_tape(&mut source, tape);
        source.write(tape, TrackNumber(1), b"garbage");
        batch(&mut source, tape, 2, &[Entry::remove(removed)]);

        let blacklists = follow(vec![subscription("a", root, BOTH)], &source);

        assert!(blacklists.is_denied(&BlacklistEntry::track(removed)));
        assert_eq!(blacklists.skipped().len(), 1);
    }

    // every order of a list's writes, repeats included, denies the same set
    #[test]
    fn any_order() {
        let (root, tape, removed, unlisted) = (address(1), address(2), address(9), address(8));
        let entries = [
            vec![Entry::remove(removed)],
            vec![Entry::unlist(unlisted)],
            vec![Entry::remove(removed), Entry::unlist(unlisted)],
        ];
        let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];

        for order in orders {
            let mut source = MemorySource::default();
            rack(&mut source, root, &[tape]);
            blacklist_tape(&mut source, tape);
            let mut blacklists = follow(vec![subscription("a", root, BOTH)], &source);
            for index in order.into_iter().chain(order) {
                let number = index as u64 + 1;
                batch(&mut source, tape, number, &entries[index]);
                apply(
                    &mut blacklists,
                    &[Change::Written { tape, track_number: TrackNumber(number) }],
                    &source,
                );
            }

            let denied: HashSet<BlacklistEntry> = blacklists.denied().into_iter().collect();
            assert_eq!(
                denied,
                HashSet::from([BlacklistEntry::track(removed), BlacklistEntry::tape(unlisted)]),
                "order {order:?}"
            );
        }
    }

    // only track 0 declares a blacklist tape, and track 0 may carry entries
    #[test]
    fn declarations() {
        let (root, declared, undeclared, first, later) = (address(1), address(2), address(3), address(9), address(8));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[declared, undeclared]);
        source.reserve(declared);
        source.write(declared, FIRST, &render_first(&[Entry::remove(first)]).expect("render"));
        source.reserve(undeclared);
        source.write(undeclared, FIRST, b"hello");
        batch(&mut source, undeclared, 1, &[Entry::remove(later)]);

        let blacklists = follow(vec![subscription("a", root, BOTH)], &source);

        assert!(blacklists.is_denied(&BlacklistEntry::track(first)));
        assert!(!blacklists.is_denied(&BlacklistEntry::track(later)));
    }

    // an expired blacklist tape, or an expired rack, stops counting at the epoch
    #[test]
    fn expired_tapes() {
        let (root, tape, removed) = (address(1), address(2), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::remove(removed)]);
        let mut blacklists = follow(vec![subscription("a", root, BOTH)], &source);

        source.expire(tape);
        let changes = apply(&mut blacklists, &[Change::Epoch { epoch: EpochNumber(9) }], &source);

        assert_eq!(changes.allowed, vec![BlacklistEntry::track(removed)]);
        assert!(!blacklists.reached_tapes().contains(&tape));
    }

    // nested racks and loops deny the same set as one rack naming the blacklist tape
    #[test]
    fn nested_racks() {
        let (root, inner, tape, removed) = (address(1), address(2), address(3), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[inner]);
        rack(&mut source, inner, &[tape, root]);
        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::remove(removed)]);

        let blacklists = follow(vec![subscription("a", root, BOTH)], &source);

        assert!(blacklists.is_denied(&BlacklistEntry::track(removed)));
        assert_eq!(blacklists.reached_tapes(), HashSet::from([root, inner, tape]));
    }

    /// A rack whose track 1 names one blacklist tape removing `removed`
    fn named_by_record(root: Address, tape: Address, removed: Address) -> MemorySource {
        let mut source = MemorySource::default();
        rack(&mut source, root, &[]);
        source.write(root, TrackNumber(1), &render_rack_records(&[Record::new(tape)]));
        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::remove(removed)]);
        source
    }

    // deleting a blacklist tape's header, or the rack record naming it, drops everything on it
    #[test]
    fn removed_headers() {
        let (root, tape, removed) = (address(1), address(2), address(9));
        let mut header_source = named_by_record(root, tape, removed);
        let mut record_source = named_by_record(root, tape, removed);
        let mut header_lists = follow(vec![subscription("a", root, BOTH)], &header_source);
        let mut record_lists = follow(vec![subscription("a", root, BOTH)], &record_source);

        let header = header_source.remove(tape, FIRST).expect("header");
        let without_header = apply(&mut header_lists, &[Change::Removed { track: header }], &header_source);
        let record = record_source.remove(root, TrackNumber(1)).expect("rack record");
        let without_record = apply(&mut record_lists, &[Change::Removed { track: record }], &record_source);

        assert_eq!(without_header.allowed, vec![BlacklistEntry::track(removed)]);
        assert_eq!(without_record.allowed, vec![BlacklistEntry::track(removed)]);
    }

    // a track without bytes holds back catching up until its bytes arrive
    #[test]
    fn missing_bytes() {
        let (root, tape, removed) = (address(1), address(2), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        blacklist_tape(&mut source, tape);
        let pending = source.pend(tape, TrackNumber(1));
        let mut blacklists = follow(vec![subscription("a", root, BOTH)], &source);
        let waiting = blacklists.missing();

        batch(&mut source, tape, 1, &[Entry::remove(removed)]);
        apply(
            &mut blacklists,
            &[Change::Written { tape, track_number: TrackNumber(1) }],
            &source,
        );

        assert_eq!(waiting.iter().map(|track| track.address).collect::<Vec<_>>(), vec![pending]);
        assert!(blacklists.is_caught_up());
        assert!(blacklists.is_denied(&BlacklistEntry::track(removed)));
    }

    // a named tape reserved and declared later joins the list
    #[test]
    fn late_reservation() {
        let (root, tape, removed) = (address(1), address(2), address(9));
        let mut source = MemorySource::default();
        rack(&mut source, root, &[tape]);
        let mut blacklists = follow(vec![subscription("a", root, BOTH)], &source);
        let watched = blacklists.watches(tape);

        blacklist_tape(&mut source, tape);
        batch(&mut source, tape, 1, &[Entry::remove(removed)]);
        let changes = apply(
            &mut blacklists,
            &[
                Change::Tape { tape },
                Change::Written { tape, track_number: FIRST },
                Change::Written { tape, track_number: TrackNumber(1) },
            ],
            &source,
        );

        assert!(watched);
        assert_eq!(changes.denied, vec![BlacklistEntry::track(removed)]);
        assert!(!blacklists.watches(address(77)));
    }
}
