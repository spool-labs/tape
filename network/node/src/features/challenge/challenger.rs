//! One node's part in the rounds: it opens each round, proves its own spools
//! and records what arrives from its peers

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use tape_core::encoding::ErasureCode;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, RoundNumber, SlotNumber};
use tape_crypto::{Address, Hash};
use tape_protocol::{EpochBundle, ProtocolState};
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::warn;

use crate::features::challenge::board::{Board, BoardRound, Opened};
use crate::features::challenge::holders::bundle_at;
use crate::features::challenge::observer::{expected_entries, record};
use crate::features::challenge::pick::has_tracks;
use crate::features::challenge::prover::build_batches;
use crate::features::challenge::round::{Batch, Round};
use crate::features::challenge::schedule::Schedule;

/// A node's board, the epoch whose rounds it is running and the proofs it holds for rounds to come
pub struct Challenger {
    node: Address,
    profile: ErasureCode,
    board: Board,
    current: Option<Current>,
    held: BTreeMap<(RoundNumber, Address), Held>,
}

/// The epoch whose rounds are running
struct Current {
    epoch: EpochNumber,
    schedule: Schedule,
    has_turned_active: bool,
    newest: RoundNumber,
    empty: HashSet<(EpochNumber, GroupIndex)>,
}

/// A batch that arrived before this node handled its round's seed block
struct Held {
    batch: Batch,
    arrived: Instant,
}

impl Challenger {
    pub fn new(node: Address, profile: ErasureCode, board: Board) -> Self {
        Self { node, profile, board, current: None, held: BTreeMap::new() }
    }

    pub fn board(&self) -> &Board {
        &self.board
    }

    pub fn held_count(&self) -> usize {
        self.held.len()
    }

    /// Start an epoch's rounds
    ///
    /// The proofs held for the epoch before are dropped, and so is what was
    /// learned about empty groups. The rounds on the board stay.
    pub fn begin_epoch(&mut self, epoch: EpochNumber, schedule: Schedule, has_turned_active: bool) {
        self.held.clear();
        self.current = Some(Current {
            epoch,
            schedule,
            has_turned_active,
            newest: RoundNumber::zero(),
            empty: HashSet::new(),
        });
    }

    /// Switch to the epoch's own holders, from the block where it turns Active
    pub fn turn_active(&mut self) {
        if let Some(current) = self.current.as_mut() {
            current.has_turned_active = true;
        }
    }

    /// Handle a block, and return the proofs to send when it seeds a round
    ///
    /// `at` is when this node handled the block. A node off the chain tip
    /// sits the round out: it sends nothing and records nothing.
    pub fn on_block<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        state: &ProtocolState,
        slot: SlotNumber,
        blockhash: Hash,
        is_at_tip: bool,
        at: Instant,
    ) -> Vec<(Address, Batch)> {
        let Some(round) = self.next_round(slot, blockhash) else {
            return Vec::new();
        };
        let early = self.take_held(round.number);

        if !is_at_tip {
            self.board.push(BoardRound { slot, opened: None, entries: Vec::new() });
            return Vec::new();
        }

        let answering = self.answering().unwrap_or(round.epoch);
        let mut opened = BoardRound {
            slot,
            opened: Some(Opened { round, at, answering }),
            entries: Vec::new(),
        };

        let mut batches = Vec::new();
        if let Some(bundle) = self.answering().and_then(|epoch| bundle_at(state, epoch)) {
            let empty = self.empty_groups(store, bundle, answering, round);
            opened.entries = expected_entries(bundle, self.node, &empty);
            for (sender, held) in &early {
                record(store, bundle, self.profile, &mut opened, *sender, &held.batch, held.arrived);
            }
            batches = build_batches(store, bundle, self.node, round, self.profile, &empty);
        }

        self.board.push(opened);
        batches
    }

    /// List the groups this node answers in that have no track in the set
    ///
    /// A group found empty is remembered for the rest of the epoch, so it
    /// costs one walk of the track index and no more. A lookup that fails
    /// counts as empty for this round and is not remembered.
    fn empty_groups<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        bundle: &EpochBundle,
        answering: EpochNumber,
        round: Round,
    ) -> HashSet<GroupIndex> {
        let mut empty = HashSet::new();
        let (Some(fold), Some(current)) = (bundle.fold(), self.current.as_mut()) else {
            return empty;
        };

        for group in &bundle.groups {
            if !group.spools.iter().any(|spool| spool.node == self.node) {
                continue;
            }

            let group = group.id;
            if current.empty.contains(&(answering, group)) {
                empty.insert(group);
                continue;
            }
            match has_tracks(store, group, fold, round.epoch) {
                Ok(true) => {}
                Ok(false) => {
                    current.empty.insert((answering, group));
                    empty.insert(group);
                }
                Err(error) => {
                    warn!(%group, %error, "challenge set lookup failed");
                    empty.insert(group);
                }
            }
        }
        empty
    }

    /// Take in a peer's proofs for a round
    ///
    /// `arrived` is stamped by the caller before anything else. The proofs
    /// are recorded when the round is on the board, held when the round is
    /// still to come, and dropped for another epoch or a round that is gone.
    pub fn on_proofs<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        state: &ProtocolState,
        sender: Address,
        batch: Batch,
        arrived: Instant,
    ) {
        let Some(current) = self.current.as_ref() else {
            return;
        };
        if batch.epoch != current.epoch {
            return;
        }
        let Some(slot) = current.schedule.slot_of(batch.round) else {
            return;
        };
        let capacity = self.board.capacity();

        match self.board.round_mut(slot) {
            Some(round) => {
                let answering = round.opened.map(|opened| opened.answering);
                if let Some(bundle) = answering.and_then(|epoch| bundle_at(state, epoch)) {
                    record(store, bundle, self.profile, round, sender, &batch, arrived);
                }
            }
            None if is_ahead(current, batch.round, capacity) => {
                self.held
                    .entry((batch.round, sender))
                    .or_insert(Held { batch, arrived });
            }
            None => {}
        }
    }

    /// The round a block seeds, when its slot is on the schedule and new to the board
    fn next_round(&mut self, slot: SlotNumber, blockhash: Hash) -> Option<Round> {
        let current = self.current.as_mut()?;
        let number = current.schedule.round_at(slot)?;
        if self.board.newest().is_some_and(|newest| slot <= newest) {
            return None;
        }

        current.newest = number;
        Some(Round { epoch: current.epoch, number, slot, seed: blockhash })
    }

    /// The epoch whose holders answer now: the previous one until the current turns Active
    fn answering(&self) -> Option<EpochNumber> {
        let current = self.current.as_ref()?;
        if current.has_turned_active {
            return Some(current.epoch);
        }

        current.epoch.checked_prev()
    }

    /// Take the batches held for a round, and drop the ones held for rounds before it
    fn take_held(&mut self, number: RoundNumber) -> Vec<(Address, Held)> {
        let later = match number.checked_next() {
            Some(next) => self.held.split_off(&(next, Address::default())),
            None => BTreeMap::new(),
        };
        let earlier = std::mem::replace(&mut self.held, later);

        let mut due = Vec::new();
        for ((round, sender), held) in earlier {
            if round == number {
                due.push((sender, held));
            }
        }
        due
    }
}

/// Whether a round is still to come and close enough to hold proofs for
fn is_ahead(current: &Current, round: RoundNumber, capacity: usize) -> bool {
    let furthest = current.newest.as_u64().saturating_add(capacity as u64);

    round > current.newest && round.as_u64() <= furthest
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytemuck::Zeroable;
    use tape_api::state::{Epoch, Group};
    use tape_core::erasure::MAX_GROUP_SIZE;
    use tape_core::spooler::{GroupIndex, VirtualGroup};
    use tape_core::system::Spool;
    use tape_core::track::types::TrackState;
    use tape_core::types::{SpoolIndex, TrackNumber};
    use tape_protocol::EpochBundle;
    use tape_store::ops::TrackDataOps;
    use tape_store_memory::MemoryStore;

    use crate::features::challenge::board::{Outcome, Seen, RECENT_ROUNDS};
    use crate::features::challenge::fixture::{
        every_case, fill, put_tape, test_store, write_inline, EPOCH, PREVIOUS, PROFILE, WRITTEN,
    };
    use crate::features::challenge::proof::Proof;

    use super::*;

    const START: SlotNumber = SlotNumber(1_000);
    const CADENCE: u64 = 5;
    const FILLER_TAPE: u8 = 100;
    const FILLER_TRACKS: u64 = 24;

    /// The node at `index`, counted from 1
    fn node(index: usize) -> Address {
        Address::from([index as u8; 32])
    }

    /// An assignment of `groups` groups. `shift` moves every holder along, so
    /// two epochs give one spool to two nodes.
    fn bundle(epoch: EpochNumber, groups: u64, shift: usize) -> EpochBundle {
        let mut accounts = Vec::new();
        for id in 0..groups {
            let mut group = Group { id: GroupIndex(id), epoch, ..Group::zeroed() };
            for position in 0..MAX_GROUP_SIZE {
                let holder = node((position + shift) % MAX_GROUP_SIZE + 1);
                group.spools[position] = Spool { node: holder, ..Spool::zeroed() };
            }
            accounts.push(group);
        }

        EpochBundle {
            epoch: Epoch { id: epoch, total_groups: groups, start_slot: START, ..Epoch::zeroed() },
            committee: Vec::new(),
            groups: accounts,
        }
    }

    /// The previous epoch ran one group, and the current one runs two with every holder moved
    fn state() -> ProtocolState {
        ProtocolState {
            current: bundle(EPOCH, 2, 1),
            previous: Some(bundle(PREVIOUS, 1, 0)),
            ..ProtocolState::default()
        }
    }

    fn store() -> TapeStore<MemoryStore> {
        let store = test_store();
        every_case(&store);
        fill(&store, FILLER_TAPE, FILLER_TRACKS);
        store
    }

    fn challenger(index: usize, capacity: usize, has_turned_active: bool) -> Challenger {
        let mut challenger = Challenger::new(node(index), PROFILE, Board::new(capacity));
        challenger.begin_epoch(EPOCH, Schedule::new(START, CADENCE), has_turned_active);
        challenger
    }

    fn slot(round: u64) -> SlotNumber {
        SlotNumber(START.0 + round * CADENCE)
    }

    fn seed(round: u64) -> Hash {
        Hash::from([round as u8; 32])
    }

    fn ms(base: Instant, offset: u64) -> Instant {
        base + Duration::from_millis(offset)
    }

    /// Open a round on a node and return what it sends
    fn open(
        challenger: &mut Challenger,
        store: &TapeStore<MemoryStore>,
        round: u64,
        at: Instant,
    ) -> Vec<(Address, Batch)> {
        challenger.on_block(store, &state(), slot(round), seed(round), true, at)
    }

    /// The batch a node sends to one peer in a round
    fn batch_for(
        store: &TapeStore<MemoryStore>,
        from: usize,
        to: usize,
        round: u64,
        has_turned_active: bool,
    ) -> Batch {
        let mut sender = challenger(from, RECENT_ROUNDS, has_turned_active);
        let batches = open(&mut sender, store, round, Instant::now());

        batches
            .into_iter()
            .find(|(peer, _)| *peer == node(to))
            .map(|(_, batch)| batch)
            .expect("a batch for the peer")
    }

    fn seen(challenger: &Challenger, round: u64, spool: SpoolIndex) -> Option<Seen> {
        let round = challenger.board().round(slot(round)).expect("round on the board");

        round
            .entries
            .iter()
            .find(|entry| entry.spool == spool)
            .expect("an entry for the spool")
            .seen
    }

    /// The spool node `index` holds in group 0 of the previous epoch
    fn previous_spool(index: usize) -> SpoolIndex {
        GroupIndex(0).spool_at(index - 1)
    }

    // a proof is timed from when the round opened to when it arrived, and the first one counts
    #[test]
    fn timing() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);
        open(&mut observer, &store, 1, base);

        let batch = batch_for(&store, 2, 1, 1, false);
        observer.on_proofs(&store, &state(), node(2), batch.clone(), ms(base, 38));
        observer.on_proofs(&store, &state(), node(2), batch, ms(base, 900));

        assert_eq!(
            seen(&observer, 1, previous_spool(2)),
            Some(Seen { outcome: Outcome::Ok, ms: 38 })
        );
        assert_eq!(seen(&observer, 1, previous_spool(3)), None);
    }

    // a proof that arrives before the seed block is held and comes out negative
    #[test]
    fn early_proof() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);
        open(&mut observer, &store, 1, base);

        let batch = batch_for(&store, 2, 1, 2, false);
        observer.on_proofs(&store, &state(), node(2), batch, ms(base, 800));
        assert_eq!(observer.held_count(), 1);
        open(&mut observer, &store, 2, ms(base, 830));

        assert_eq!(observer.held_count(), 0);
        assert_eq!(
            seen(&observer, 2, previous_spool(2)),
            Some(Seen { outcome: Outcome::Ok, ms: -30 })
        );
    }

    // a proof that comes rounds late records its real time while the round is on the board
    #[test]
    fn late_proof() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, 3, false);
        open(&mut observer, &store, 1, base);
        open(&mut observer, &store, 2, ms(base, 830));
        open(&mut observer, &store, 3, ms(base, 1_660));

        let batch = batch_for(&store, 2, 1, 1, false);
        observer.on_proofs(&store, &state(), node(2), batch.clone(), ms(base, 2_140));
        assert_eq!(
            seen(&observer, 1, previous_spool(2)),
            Some(Seen { outcome: Outcome::Ok, ms: 2_140 })
        );

        let other = batch_for(&store, 3, 1, 1, false);
        open(&mut observer, &store, 4, ms(base, 2_490));
        observer.on_proofs(&store, &state(), node(3), other, ms(base, 2_500));

        assert!(observer.board().round(slot(1)).is_none());
        assert_eq!(observer.held_count(), 0);
    }

    // once the epoch has advanced, held proofs are gone and a proof for the old epoch is dropped
    #[test]
    fn epoch_bound() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);
        open(&mut observer, &store, 1, base);
        let late = batch_for(&store, 2, 1, 1, false);
        let early = batch_for(&store, 2, 1, 2, false);
        observer.on_proofs(&store, &state(), node(2), early, ms(base, 800));
        assert_eq!(observer.held_count(), 1);

        observer.begin_epoch(EPOCH.next(), Schedule::new(slot(2), CADENCE), false);
        observer.on_proofs(&store, &state(), node(2), late, ms(base, 900));

        assert_eq!(observer.held_count(), 0);
        assert_eq!(seen(&observer, 1, previous_spool(2)), None);
        assert_eq!(observer.board().len(), 1);
    }

    // proofs are held for rounds up to the board's size ahead and no further
    #[test]
    fn held_bound() {
        let store = store();
        let capacity = 3;
        let mut observer = challenger(1, capacity, false);
        open(&mut observer, &store, 1, Instant::now());

        for round in 1..=8 {
            let batch = Batch { epoch: EPOCH, round: RoundNumber(round), proofs: Vec::new() };
            observer.on_proofs(&store, &state(), node(2), batch.clone(), Instant::now());
            observer.on_proofs(&store, &state(), node(2), batch, Instant::now());
        }

        assert_eq!(observer.held_count(), capacity);
    }

    // a held proof for a round whose slot was skipped goes when a later round opens
    #[test]
    fn skipped_round() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);
        open(&mut observer, &store, 1, base);
        let batch = batch_for(&store, 2, 1, 2, false);
        observer.on_proofs(&store, &state(), node(2), batch, ms(base, 800));

        open(&mut observer, &store, 3, ms(base, 1_660));

        assert_eq!(observer.held_count(), 0);
        assert!(observer.board().round(slot(2)).is_none());
        assert_eq!(seen(&observer, 3, previous_spool(2)), None);
    }

    // a valid proof, a changed one, one for another track and a silent peer each show as such
    #[test]
    fn outcomes() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);
        open(&mut observer, &store, 1, base);

        let valid = batch_for(&store, 2, 1, 1, false);
        let mut changed = batch_for(&store, 3, 1, 1, false);
        changed.proofs[0].proof = match &changed.proofs[0].proof {
            Proof::Inline(payload) => Proof::Inline(payload[1..].to_vec()),
            Proof::Coded(leaf) => Proof::Inline(leaf.sub_leaf.clone()),
        };
        let mut other_track = batch_for(&store, 4, 1, 1, false);
        other_track.proofs[0].track = Address::from([9u8; 32]);

        observer.on_proofs(&store, &state(), node(2), valid, ms(base, 10));
        observer.on_proofs(&store, &state(), node(3), changed, ms(base, 20));
        observer.on_proofs(&store, &state(), node(4), other_track, ms(base, 30));

        assert_eq!(seen(&observer, 1, previous_spool(2)), Some(Seen { outcome: Outcome::Ok, ms: 10 }));
        assert_eq!(
            seen(&observer, 1, previous_spool(3)),
            Some(Seen { outcome: Outcome::Invalid, ms: 20 })
        );
        assert_eq!(
            seen(&observer, 1, previous_spool(4)),
            Some(Seen { outcome: Outcome::WrongTrack, ms: 30 })
        );
        assert_eq!(seen(&observer, 1, previous_spool(5)), None);
    }

    // a proof from a node that does not hold the spool is not recorded
    #[test]
    fn wrong_sender() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);
        open(&mut observer, &store, 1, base);

        let batch = batch_for(&store, 2, 1, 1, false);
        observer.on_proofs(&store, &state(), node(3), batch, ms(base, 10));

        assert_eq!(seen(&observer, 1, previous_spool(2)), None);
    }

    // a holder that has the track rows and none of the data sends nothing, and
    // its peers expect its proof all the same
    #[test]
    fn syncing_holder() {
        let full = store();
        let rows_only = test_store();
        let mut cases = every_case(&rows_only);
        cases.extend(fill(&rows_only, FILLER_TAPE, FILLER_TRACKS));
        for case in cases {
            rows_only.delete_track_data(case.address).expect("data deleted");
        }
        let mut syncing = challenger(2, RECENT_ROUNDS, false);
        let mut observer = challenger(1, RECENT_ROUNDS, false);

        let sent = open(&mut syncing, &rows_only, 1, Instant::now());
        open(&mut observer, &full, 1, Instant::now());

        let own = syncing.board().round(slot(1)).expect("round on the board");
        assert!(sent.is_empty());
        assert_eq!(own.entries.len(), MAX_GROUP_SIZE - 1);
        assert_eq!(seen(&observer, 1, previous_spool(2)), None);
    }

    // off the tip a node sends nothing, marks the round as not observed and takes no proofs for it
    #[test]
    fn off_tip() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);

        let sent = observer.on_block(&store, &state(), slot(1), seed(1), false, base);
        let batch = batch_for(&store, 2, 1, 1, false);
        observer.on_proofs(&store, &state(), node(2), batch, ms(base, 10));

        let round = observer.board().round(slot(1)).expect("round on the board");
        assert!(sent.is_empty());
        assert!(round.opened.is_none());
        assert!(round.entries.is_empty());
        assert_eq!(observer.held_count(), 0);
    }

    // a block that seeds no round, or a second block for a slot, opens nothing
    #[test]
    fn other_blocks() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);

        let between = observer.on_block(&store, &state(), SlotNumber(1_003), seed(1), true, base);
        open(&mut observer, &store, 1, base);
        let again = open(&mut observer, &store, 1, ms(base, 5));

        assert!(between.is_empty());
        assert!(again.is_empty());
        assert_eq!(observer.board().len(), 1);
    }

    // until the epoch turns Active the previous epoch's holders answer, under its
    // spool indexes and fold. From then on the epoch's own do.
    #[test]
    fn who_answers() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);

        let before = open(&mut observer, &store, 1, base);
        observer.turn_active();
        let after = open(&mut observer, &store, 2, ms(base, 830));

        let first = observer.board().round(slot(1)).expect("round on the board");
        let second = observer.board().round(slot(2)).expect("round on the board");
        let opened = first.opened.expect("opened");
        assert_eq!(opened.answering, PREVIOUS);
        assert_eq!(second.opened.expect("opened").answering, EPOCH);

        // One group of 20 before, with this node at position 0.
        assert_eq!(first.entries.len(), MAX_GROUP_SIZE - 1);
        assert!(first.entries.iter().all(|entry| GroupIndex(0).contains(entry.spool)));
        assert_eq!(before.len(), MAX_GROUP_SIZE - 1);
        assert_eq!(before[0].1.proofs[0].spool, GroupIndex(0).spool_at(0));

        // Two groups after, with this node at position 19 of each.
        assert_eq!(second.entries.len(), 2 * (MAX_GROUP_SIZE - 1));
        assert!(second.entries.iter().any(|entry| GroupIndex(1).contains(entry.spool)));
        assert_eq!(after[0].1.proofs.len(), 2);
        assert_eq!(after[0].1.proofs[0].spool, GroupIndex(0).spool_at(MAX_GROUP_SIZE - 1));
    }

    // a node that held nothing in the previous epoch sends and expects nothing before Active
    #[test]
    fn new_holder() {
        let store = store();
        let base = Instant::now();
        let mut newcomer = Challenger::new(node(21), PROFILE, Board::new(RECENT_ROUNDS));
        let mut joined = state();
        joined.current.groups[1].spools[0].node = node(21);
        newcomer.begin_epoch(EPOCH, Schedule::new(START, CADENCE), false);

        let before = newcomer.on_block(&store, &joined, slot(1), seed(1), true, base);
        newcomer.turn_active();
        let after = newcomer.on_block(&store, &joined, slot(2), seed(2), true, ms(base, 830));

        let first = newcomer.board().round(slot(1)).expect("round on the board");
        assert!(before.is_empty());
        assert!(first.opened.is_some());
        assert!(first.entries.is_empty());
        assert_eq!(after.len(), MAX_GROUP_SIZE - 1);
    }

    // a peer gets the proofs for the groups it shares with the sender and no others
    #[test]
    fn shared_groups() {
        let store = store();
        let mut shared = state();
        // Node 2 gives its group 1 spool to node 21, so it shares only group 0 with node 1.
        shared.current.groups[1].spools[0].node = node(21);
        let mut sender = challenger(1, RECENT_ROUNDS, true);

        let batches = sender.on_block(&store, &shared, slot(1), seed(1), true, Instant::now());

        let own_first = GroupIndex(0).spool_at(MAX_GROUP_SIZE - 1);
        let own_second = GroupIndex(1).spool_at(MAX_GROUP_SIZE - 1);
        assert_eq!(spools_for(&batches, 2), vec![own_first]);
        assert_eq!(spools_for(&batches, 3), vec![own_first, own_second]);
        assert_eq!(spools_for(&batches, 21), vec![own_second]);
    }

    /// The spools a peer's batch carries proofs for
    fn spools_for(batches: &[(Address, Batch)], peer: usize) -> Vec<SpoolIndex> {
        let mut spools = Vec::new();
        for (address, batch) in batches {
            if *address == node(peer) {
                for proof in &batch.proofs {
                    spools.push(proof.spool);
                }
            }
        }
        spools
    }

    // a group with no track in the set expects no proofs, and its spools send none
    #[test]
    fn empty_group() {
        let store = test_store();
        let mut observer = challenger(1, RECENT_ROUNDS, true);

        let sent = open(&mut observer, &store, 1, Instant::now());

        let round = observer.board().round(slot(1)).expect("round on the board");
        assert!(sent.is_empty());
        assert!(round.opened.is_some());
        assert!(round.entries.is_empty());
    }

    // a group found empty is not walked again in the epoch: tracks that reach
    // the store afterwards are seen once the next epoch begins, and no sooner
    #[test]
    fn empty_remembered() {
        let store = test_store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, true);
        let mut fresh = challenger(1, RECENT_ROUNDS, true);

        open(&mut observer, &store, 1, base);
        every_case(&store);
        fill(&store, FILLER_TAPE, FILLER_TRACKS);
        let same_epoch = open(&mut observer, &store, 2, ms(base, 830));
        open(&mut fresh, &store, 2, ms(base, 830));

        // A node with no memory of the group sees the tracks. This one does not walk again.
        let unseen = observer.board().round(slot(2)).expect("round on the board");
        let seen = fresh.board().round(slot(2)).expect("round on the board");
        assert!(same_epoch.is_empty());
        assert!(unseen.entries.is_empty());
        assert_eq!(seen.entries.len(), 2 * (MAX_GROUP_SIZE - 1));

        let next_start = slot(2);
        observer.begin_epoch(EPOCH.next(), Schedule::new(next_start, CADENCE), false);
        let next = SlotNumber(next_start.0 + CADENCE);
        let next_epoch = observer.on_block(&store, &state(), next, seed(3), true, ms(base, 1_660));

        let third = observer.board().round(next).expect("round on the board");
        assert_eq!(third.entries.len(), 2 * (MAX_GROUP_SIZE - 1));
        assert_eq!(next_epoch.len(), MAX_GROUP_SIZE - 1);
    }

    // what was learned while one epoch's holders answered says nothing about
    // the next epoch's: under another fold the same group carries other tracks
    #[test]
    fn empty_per_holders() {
        let store = test_store();
        put_tape(&store, FILLER_TAPE, EPOCH.next(), TrackNumber(0));
        for number in 0..4 {
            write_inline(&store, FILLER_TAPE, number, VirtualGroup(1), TrackState::Certified, WRITTEN);
        }
        // Two groups in the previous epoch, one in the current.
        let shrinking = ProtocolState {
            current: bundle(EPOCH, 1, 1),
            previous: Some(bundle(PREVIOUS, 2, 0)),
            ..ProtocolState::default()
        };
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, false);

        observer.on_block(&store, &shrinking, slot(1), seed(1), true, base);
        observer.turn_active();
        observer.on_block(&store, &shrinking, slot(2), seed(2), true, ms(base, 830));

        // Under two groups the tracks are all group 1's, and group 0 is empty.
        let before = observer.board().round(slot(1)).expect("round on the board");
        assert_eq!(before.entries.len(), MAX_GROUP_SIZE - 1);
        assert!(before.entries.iter().all(|entry| GroupIndex(1).contains(entry.spool)));

        // Under one group, group 0 carries them.
        let after = observer.board().round(slot(2)).expect("round on the board");
        assert_eq!(after.entries.len(), MAX_GROUP_SIZE - 1);
        assert!(after.entries.iter().all(|entry| GroupIndex(0).contains(entry.spool)));
    }

    // the rounds of an epoch stay on the board when the next epoch begins
    #[test]
    fn epoch_turn() {
        let store = store();
        let base = Instant::now();
        let mut observer = challenger(1, RECENT_ROUNDS, true);
        open(&mut observer, &store, 1, base);
        open(&mut observer, &store, 2, ms(base, 830));

        let next_start = slot(2);
        observer.begin_epoch(EPOCH.next(), Schedule::new(next_start, CADENCE), false);
        let next = SlotNumber(next_start.0 + CADENCE);
        observer.on_block(&store, &state(), next, seed(3), true, ms(base, 1_660));

        assert_eq!(observer.board().len(), 3);
        assert!(observer.board().round(slot(1)).is_some());
        assert_eq!(observer.board().newest(), Some(next));
        assert_eq!(observer.board().capacity(), RECENT_ROUNDS);
    }
}
