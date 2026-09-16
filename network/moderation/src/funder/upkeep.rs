//! The funder: upkeep of the rack and every blacklist tape, creating the rack and adding blacklist tapes

use std::path::PathBuf;
use std::sync::Arc;

use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::chain::keys::{load_key, persist_new_key};
use crate::chain::tape_chain::{TapeChain, TapeStatus};
use crate::config::settings::{FunderConfig, TapesConfig};
use crate::funder::error::FunderError;
use crate::funder::plan::{is_expired, is_expiring, low_balances, plan_tape, LowBalance, TapeDuty};
use crate::publisher::flush::Publisher;
use crate::publisher::writer::TrackWriter;
use crate::records::rows::{ListTape, ListTapes};
use crate::records::states::{TapeRole, TapeState};
use crate::records::store::Records;
use crate::types::timestamp::Timestamp;

/// Funds this producer's tapes, moves decisions off tapes near expiry and grows the list
pub struct Funder<Chain>
where
    Chain: TapeChain + TrackWriter,
{
    records: Records,
    chain: Arc<Chain>,
    config: FunderConfig,
    tapes: TapesConfig,
}

/// What one run did
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpkeepReport {
    pub extended_expiry: Vec<Address>,
    pub extended_capacity: Vec<Address>,
    pub low_balances: Vec<LowBalance>,
    /// The rack's free track slots are down to the headroom
    pub is_rack_short_of_tracks: bool,
    /// The rack is inside the move horizon; subscriptions name it, so nothing can move off it
    pub is_rack_expiring: bool,
    /// Blacklist tapes whose decisions started moving to the newest one
    pub vacated: Vec<Address>,
    /// Blacklist tapes found expired, whose decisions queue again
    pub expired: Vec<Address>,
    /// The blacklist tape added because the newest ran short of tracks or time
    pub new_tape: Option<Address>,
}

/// A rack and the first blacklist tape it names
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedRack {
    pub rack: ListTape,
    pub blacklist: ListTape,
}

impl<Chain> Funder<Chain>
where
    Chain: TapeChain + TrackWriter,
{
    pub fn new(records: Records, chain: Arc<Chain>, config: FunderConfig, tapes: TapesConfig) -> Self {
        Self {
            records,
            chain,
            config,
            tapes,
        }
    }

    /// Extend what needs extending, move decisions off tapes near expiry, warn on the payer or rack, add a tape when needed
    pub async fn run_once(
        &self,
        publisher: &Publisher<Chain>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<UpkeepReport, FunderError> {
        let mut report = UpkeepReport::default();
        let current_epoch = self.chain.current_epoch().await?;
        let tapes = self.records.list_tapes().await?;
        let newest = tapes.newest().map(|tape| tape.address);
        let mut needs_tape = newest.is_none() && has_left(&tapes);
        for tape in tapes.all() {
            if !tape.is_active() {
                continue;
            }
            // Safe to cancel: each step reads the tape first, so a later run sees an earlier one's effect
            let upkeep = self.upkeep_tape(tape, duty(tape, newest), current_epoch, now, &mut report);
            let Some(upkept) = cancel.run_until_cancelled(upkeep).await else {
                return Ok(report);
            };
            match upkept {
                Ok(is_newest_short) => needs_tape |= is_newest_short,
                Err(error) => warn!(tape = %tape.address, %error, "blacklist tape upkeep failed"),
            }
        }

        self.check_balances(&mut report).await;
        if needs_tape {
            report.new_tape = self.add_tape(publisher, now, cancel).await?;
        }
        Ok(report)
    }

    /// Add a blacklist tape: reserve it, then publish its header, its rack record and entries
    ///
    /// A run resumes a tape an earlier run left unnamed: a tape already on chain is
    /// not reserved again, and the publisher settles its header and rack record
    /// against the chain before writing either again.
    pub async fn add_tape(
        &self,
        publisher: &Publisher<Chain>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<Option<Address>, FunderError> {
        let tapes = self.records.list_tapes().await?;
        if !is_rack_ready(&tapes) {
            return Err(FunderError::NoRack);
        }
        let tape = match tapes.pending() {
            Some(tape) => tape.clone(),
            None => self.create_tape(TapeRole::Blacklist, now).await?,
        };

        // Safe to cancel: the next run resumes from whichever step finished last
        let Some(reserved) = cancel.run_until_cancelled(self.finish_reservation(tape, now)).await else {
            return Ok(None);
        };
        let address = reserved?.address;
        publisher.flush(now, cancel).await?;
        let tapes = self.records.list_tapes().await?;
        let is_named = tapes.find(address).is_some_and(|tape| tape.is_named);
        if is_named {
            info!(tape = %address, "added a blacklist tape to the rack");
        }
        Ok(is_named.then_some(address))
    }

    /// Create the list's rack and first blacklist tape, resuming an interrupted run
    ///
    /// Each tape's key is persisted before the tape is reserved. The publisher then
    /// declares both tapes before the rack names the blacklist tape.
    pub async fn create_rack(&self, publisher: &Publisher<Chain>, now: Timestamp) -> Result<CreatedRack, FunderError> {
        let tapes = self.records.list_tapes().await?;
        if let (Some(rack), Some(_)) = (&tapes.rack, tapes.newest()) {
            return Err(FunderError::AlreadyCreated(rack.address));
        }

        let rack = match tapes.rack.clone() {
            Some(rack) => rack,
            None => self.create_tape(TapeRole::Rack, now).await?,
        };
        self.finish_reservation(rack, now).await?;
        let blacklist = match tapes.pending() {
            Some(tape) => tape.clone(),
            None => self.create_tape(TapeRole::Blacklist, now).await?,
        };
        self.finish_reservation(blacklist, now).await?;
        publisher.flush(now, &CancellationToken::new()).await?;

        let tapes = self.records.list_tapes().await?;
        let newest = tapes.newest().cloned();
        if let (Some(rack), Some(blacklist)) = (tapes.rack, newest) {
            return Ok(CreatedRack { rack, blacklist });
        }
        Err(FunderError::Unfinished)
    }

    /// Keep one tape alive, returning whether the list needs a new blacklist tape
    async fn upkeep_tape(
        &self,
        tape: &ListTape,
        duty: TapeDuty,
        current_epoch: EpochNumber,
        now: Timestamp,
        report: &mut UpkeepReport,
    ) -> Result<bool, FunderError> {
        let Some(status) = self.chain.tape_status(tape.address).await? else {
            warn!(tape = %tape.address, "blacklist tape is missing on chain");
            return Ok(false);
        };
        self.records.observe_tape(tape.address, status.observation(), now).await?;
        if tape.role == TapeRole::Blacklist && is_expired(status.expiry_epoch, current_epoch) {
            self.records.expire_tape(tape.address, now).await?;
            warn!(tape = %tape.address, expiry_epoch = status.expiry_epoch.0, "blacklist tape expired; its decisions are written again");
            report.expired.push(tape.address);
            return Ok(duty == TapeDuty::Newest);
        }

        let upkeep = plan_tape(&status, duty, current_epoch, &self.config);
        let expiry_epoch = self.extend(tape, &status, upkeep.extend_expiry, report).await;
        if let Some(extra) = upkeep.extend_capacity {
            self.chain.extend_capacity(tape.address, extra).await?;
            info!(tape = %tape.address, bytes = extra.0, "extended blacklist tape capacity");
            report.extended_capacity.push(tape.address);
        }

        let is_expiring = is_expiring(expiry_epoch, current_epoch, &self.config);
        match duty {
            TapeDuty::Rack => note_rack(tape, &status, upkeep.is_short_of_tracks, is_expiring, report),
            TapeDuty::Newest => return Ok(upkeep.is_short_of_tracks || is_expiring),
            TapeDuty::Kept => self.vacate(tape, is_expiring, now, report).await?,
        }
        Ok(false)
    }

    /// Extend a tape's expiry when planned, returning the expiry it has now
    async fn extend(
        &self,
        tape: &ListTape,
        status: &TapeStatus,
        extension: Option<EpochNumber>,
        report: &mut UpkeepReport,
    ) -> EpochNumber {
        let Some(epochs) = extension else {
            return status.expiry_epoch;
        };
        if let Err(error) = self.chain.extend_expiry(tape.address, epochs).await {
            warn!(tape = %tape.address, %error, "extending blacklist tape expiry failed");
            return status.expiry_epoch;
        }
        info!(tape = %tape.address, extension = epochs.0, "extended blacklist tape expiry");
        report.extended_expiry.push(tape.address);
        status.expiry_epoch + epochs
    }

    /// Start moving a named older tape's decisions to the newest once extending has not kept it past the move horizon
    async fn vacate(
        &self,
        tape: &ListTape,
        is_expiring: bool,
        now: Timestamp,
        report: &mut UpkeepReport,
    ) -> Result<(), FunderError> {
        if !is_expiring || tape.state != TapeState::Reserved || !tape.is_named {
            return Ok(());
        }
        self.records.set_tape_state(tape.address, TapeState::Vacating, now).await?;
        warn!(tape = %tape.address, "blacklist tape is near expiry; moving its decisions to the newest tape");
        report.vacated.push(tape.address);
        Ok(())
    }

    async fn check_balances(&self, report: &mut UpkeepReport) {
        match self.chain.payer_balances().await {
            Ok(balances) => report.low_balances = low_balances(&balances, &self.config),
            Err(error) => warn!(%error, "reading payer balances failed"),
        }
        for low in &report.low_balances {
            warn!(?low, "blacklist payer balance is low");
        }
    }

    /// Reserve a tape recorded as reserving, unless the chain already has it
    async fn finish_reservation(&self, tape: ListTape, now: Timestamp) -> Result<ListTape, FunderError> {
        if tape.state != TapeState::Reserving {
            return Ok(tape);
        }
        if self.chain.tape_status(tape.address).await?.is_none() {
            let key = load_key(&tape.key_path, tape.address).await?;
            self.chain
                .reserve(&key, self.tapes.initial_capacity, self.tapes.initial_epochs)
                .await?;
            info!(tape = %tape.address, role = tape.role.as_str(), "reserved a list tape");
        }
        self.records.set_tape_state(tape.address, TapeState::Reserved, now).await?;
        Ok(ListTape {
            state: TapeState::Reserved,
            ..tape
        })
    }

    async fn create_tape(&self, role: TapeRole, now: Timestamp) -> Result<ListTape, FunderError> {
        let (address, path) = self.persist_key().await?;
        Ok(self.records.insert_tape(address, role, path, now).await?)
    }

    /// Write a new tape key durably, before anything records or reserves its tape
    async fn persist_key(&self) -> Result<(Address, PathBuf), FunderError> {
        let key_dir = self.tapes.key_dir.clone();
        let persisted = spawn_blocking(move || persist_new_key(&key_dir))
            .await
            .map_err(|error| FunderError::Task(error.to_string()))?;
        let (address, path) = persisted?;
        info!(tape = %address, key = %path.display(), "persisted a new list tape key");
        Ok((address, path))
    }
}

fn duty(tape: &ListTape, newest: Option<Address>) -> TapeDuty {
    match tape.role {
        TapeRole::Rack => TapeDuty::Rack,
        TapeRole::Blacklist if newest == Some(tape.address) => TapeDuty::Newest,
        TapeRole::Blacklist => TapeDuty::Kept,
    }
}

fn note_rack(
    rack: &ListTape,
    status: &TapeStatus,
    is_short_of_tracks: bool,
    is_expiring: bool,
    report: &mut UpkeepReport,
) {
    if is_short_of_tracks {
        warn!(rack = %rack.address, next_track = status.next_track.0, "blacklist rack is running out of tracks");
        report.is_rack_short_of_tracks = true;
    }
    if is_expiring {
        error!(rack = %rack.address, expiry_epoch = status.expiry_epoch.0, "blacklist rack is about to expire and could not be extended");
        report.is_rack_expiring = true;
    }
}

/// Whether a blacklist tape has left the list, so a missing newest tape needs replacing
fn has_left(tapes: &ListTapes) -> bool {
    tapes
        .blacklists
        .iter()
        .any(|tape| matches!(tape.state, TapeState::Vacating | TapeState::Retired | TapeState::Expired))
}

/// Whether the rack's reservation is confirmed
fn is_rack_ready(tapes: &ListTapes) -> bool {
    tapes
        .rack
        .as_ref()
        .is_some_and(|rack| rack.state == TapeState::Reserved)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use tape_blacklist::batch::HEADER as BLACKLIST_HEADER;
    use tape_blacklist::entry::Entry;
    use tape_core::system::BlacklistEntry;
    use tape_core::types::coin::SOL;
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_rack::format::HEADER as RACK_HEADER;
    use tape_sdk::stream::manifest::MAX_TRACKS_PER_TAPE;
    use tempfile::TempDir;

    use super::*;
    use crate::chain::fake::{FakeChain, FakeTape, Landed, WriteFault};
    use crate::publisher::error::PublishError;
    use crate::publisher::flush::{FlushOutcome, FlushReport};

    const MIB: u64 = 1_024 * 1_024;
    const WINDOW: Duration = Duration::from_secs(10);

    struct Setup {
        dir: TempDir,
        records: Records,
        chain: Arc<FakeChain>,
        funder: Funder<FakeChain>,
        publisher: Publisher<FakeChain>,
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn empty(config: FunderConfig) -> Setup {
        let dir = tempfile::tempdir().expect("temp dir");
        let records = Records::open(&dir.path().join("records.sqlite")).expect("open records");
        let chain = Arc::new(FakeChain::new(EpochNumber(100)));
        let tapes = TapesConfig {
            key_dir: dir.path().join("tapes"),
            initial_capacity: StorageUnits(MIB),
            initial_epochs: EpochNumber(52),
        };
        let funder = Funder::new(records.clone(), Arc::clone(&chain), config, tapes);
        let publisher = Publisher::new(records.clone(), Arc::clone(&chain), WINDOW);
        Setup {
            dir,
            records,
            chain,
            funder,
            publisher,
        }
    }

    /// A rack and `count` blacklist tapes, each declared and named
    async fn created(config: FunderConfig, count: usize) -> Setup {
        let setup = empty(config);
        setup
            .funder
            .create_rack(&setup.publisher, Timestamp(0))
            .await
            .expect("create rack");
        for _ in 1..count {
            add_tape(&setup).await.expect("add tape");
        }
        setup
    }

    async fn add_tape(setup: &Setup) -> Result<Option<Address>, FunderError> {
        setup
            .funder
            .add_tape(&setup.publisher, Timestamp(10), &CancellationToken::new())
            .await
    }

    async fn tapes(setup: &Setup) -> ListTapes {
        setup.records.list_tapes().await.expect("list tapes")
    }

    async fn rack_and_newest(setup: &Setup) -> (Address, Address) {
        let tapes = tapes(setup).await;
        let rack = tapes.rack.as_ref().expect("rack").address;
        (rack, tapes.newest().expect("newest").address)
    }

    fn update(setup: &Setup, tape: Address, change: fn(&mut FakeTape)) {
        change(setup.chain.state().tapes.get_mut(&tape).expect("tape exists"));
    }

    fn reserved(setup: &Setup) -> Vec<Address> {
        setup.chain.state().reserved.clone()
    }

    fn position(setup: &Setup, event: Landed) -> usize {
        let landed = setup.chain.state().landed.clone();
        landed.iter().position(|seen| *seen == event).expect("landed")
    }

    async fn queue(setup: &Setup, byte: u8) {
        setup
            .records
            .queue_operator_decision(Entry::remove(address(byte)), Timestamp(0))
            .await
            .expect("queue decision");
    }

    async fn flush(setup: &Setup, at: i64) -> FlushReport {
        setup
            .publisher
            .flush(Timestamp(at), &CancellationToken::new())
            .await
            .expect("flush")
    }

    async fn run(setup: &Setup, at: i64) -> UpkeepReport {
        setup
            .funder
            .run_once(&setup.publisher, Timestamp(at), &CancellationToken::new())
            .await
            .expect("run upkeep")
    }

    // the rack and first tape are reserved once and declared, the rack names the tape, and a second run is refused
    #[tokio::test]
    async fn create_rack() {
        let setup = empty(FunderConfig::default());

        let created = setup.funder.create_rack(&setup.publisher, Timestamp(0)).await.expect("create rack");
        let again = setup.funder.create_rack(&setup.publisher, Timestamp(1)).await;

        let (rack, tape) = (created.rack.address, created.blacklist.address);
        assert_eq!(reserved(&setup), vec![rack, tape]);
        let record = format!("tape {tape}\n");
        assert_eq!(setup.chain.tracks(rack), vec![(TrackNumber(0), format!("{RACK_HEADER}\n")), (TrackNumber(1), record)]);
        assert_eq!(setup.chain.tracks(tape), vec![(TrackNumber(0), format!("{BLACKLIST_HEADER}\n"))]);
        assert!(matches!(again, Err(FunderError::AlreadyCreated(address)) if address == rack));
        assert!(created.rack.key_path.exists() && created.blacklist.key_path.exists());
    }

    // an interrupted run resumes without reserving or declaring again, and readers see the tape once
    #[tokio::test]
    async fn resumed_setup() {
        let setup = empty(FunderConfig::default());
        let (rack, key_path) = persist_new_key(&setup.dir.path().join("tapes")).expect("persist key");
        setup
            .records
            .insert_tape(rack, TapeRole::Rack, key_path, Timestamp(0))
            .await
            .expect("insert rack");
        let landed = FakeTape::new(StorageUnits(MIB), EpochNumber(152));
        setup.chain.state().tapes.insert(rack, landed);
        setup.chain.state().write_faults.push_back(WriteFault::Lost);

        let stopped = setup.funder.create_rack(&setup.publisher, Timestamp(0)).await;
        let resumed = setup.funder.create_rack(&setup.publisher, Timestamp(1)).await.expect("resume");

        let tape = resumed.blacklist.address;
        assert!(matches!(stopped, Err(FunderError::Publish(PublishError::Write { .. }))));
        assert_eq!(resumed.rack.address, rack);
        assert_eq!(reserved(&setup), vec![tape]);
        assert_eq!(setup.chain.tracks(rack).len(), 2);
        assert_eq!(setup.chain.reached(rack), HashSet::from([rack, tape]));
    }

    // a newest tape short of tracks gets a successor, declared before the rack names it, and decisions publish there
    #[tokio::test]
    async fn new_tape() {
        let config = FunderConfig {
            track_headroom: TrackNumber(MAX_TRACKS_PER_TAPE - 1),
            ..FunderConfig::default()
        };
        let setup = created(config, 1).await;
        let (rack, first) = rack_and_newest(&setup).await;
        queue(&setup, 9).await;

        let report = run(&setup, 100).await;

        let new = report.new_tape.expect("new tape");
        let tapes = tapes(&setup).await;
        let mut states = Vec::new();
        for tape in &tapes.blacklists {
            states.push((tape.address, tape.state, tape.is_named));
        }
        assert_eq!(states, vec![(first, TapeState::Reserved, true), (new, TapeState::Reserved, true)]);
        assert_eq!(reserved(&setup), vec![rack, first, new]);
        let header = position(&setup, Landed::Write { tape: new, track_number: TrackNumber(0) });
        let record = position(&setup, Landed::Write { tape: rack, track_number: TrackNumber(2) });
        assert!(header < record);
        let entry = format!("remove track {}\n", address(9));
        assert_eq!(setup.chain.tracks(new)[1..], [(TrackNumber(1), entry)]);
    }

    // a rack record that lands but is not recorded is found on the next run, and readers see each tape once
    #[tokio::test]
    async fn record_crash() {
        let setup = created(FunderConfig::default(), 1).await;
        let (rack, first) = rack_and_newest(&setup).await;
        setup.chain.state().write_faults.extend([WriteFault::Lands, WriteFault::Lost]);

        let stopped = add_tape(&setup).await;
        let pending = tapes(&setup).await.pending().map(|tape| (tape.is_declared, tape.is_named));
        let resumed = add_tape(&setup).await.expect("resume").expect("tape named");

        assert!(matches!(stopped, Err(FunderError::Publish(PublishError::Write { .. }))));
        assert_eq!(pending, Some((true, false)));
        assert_eq!(setup.chain.tracks(rack).len(), 3);
        assert_eq!(reserved(&setup).len(), 3);
        assert_eq!(setup.chain.reached(rack), HashSet::from([rack, first, resumed]));
    }

    // expiry inside the horizon is extended on the rack and every blacklist tape, old ones included
    #[tokio::test]
    async fn extend_expiry() {
        let setup = created(FunderConfig::default(), 2).await;
        let all: Vec<Address> = tapes(&setup).await.all().map(|tape| tape.address).collect();
        for tape in &all {
            update(&setup, *tape, |fake| fake.expiry_epoch = EpochNumber(103));
        }

        let report = run(&setup, 10).await;

        assert_eq!(report.extended_expiry, all);
        assert!(report.vacated.is_empty());
        let state = setup.chain.state();
        assert_eq!(state.expiry_extensions, all);
        assert!(all.iter().all(|tape| state.tapes[tape].expiry_epoch == EpochNumber(103 + 26)));
    }

    // capacity under the reserve is extended on the rack and the newest tape only
    #[tokio::test]
    async fn extend_capacity() {
        let setup = created(FunderConfig::default(), 2).await;
        let all: Vec<Address> = tapes(&setup).await.all().map(|tape| tape.address).collect();
        for tape in &all {
            update(&setup, *tape, |fake| fake.used = fake.capacity - StorageUnits(1_000));
        }

        let report = run(&setup, 10).await;

        let (rack, newest) = rack_and_newest(&setup).await;
        assert_eq!(report.extended_capacity, vec![rack, newest]);
        assert_eq!(setup.chain.state().capacity_extensions, vec![rack, newest]);
        assert_eq!(report.new_tape, None);
    }

    // a rack short of tracks is reported without adding a tape
    #[tokio::test]
    async fn rack_limit() {
        let setup = created(FunderConfig::default(), 1).await;
        let (rack, _) = rack_and_newest(&setup).await;
        update(&setup, rack, |fake| {
            fake.next_track = TrackNumber(MAX_TRACKS_PER_TAPE - FunderConfig::default().track_headroom.0);
        });

        let report = run(&setup, 10).await;

        assert!(report.is_rack_short_of_tracks);
        assert_eq!(report.new_tape, None);
    }

    // a payer balance under its threshold is reported
    #[tokio::test]
    async fn low_balance() {
        let setup = created(FunderConfig::default(), 1).await;
        setup.chain.state().balances.sol = SOL(1_000);

        let report = run(&setup, 10).await;

        assert_eq!(
            report.low_balances,
            vec![LowBalance::Sol {
                balance: SOL(1_000),
                threshold: FunderConfig::default().sol_warn_below,
            }]
        );
    }

    // an older tape extensions cannot keep past the move horizon has its decisions moved before it leaves the rack
    #[tokio::test]
    async fn expiring() {
        let setup = created(FunderConfig::default(), 1).await;
        let (rack, first) = rack_and_newest(&setup).await;
        queue(&setup, 1).await;
        queue(&setup, 2).await;
        flush(&setup, 100).await;
        let newest = add_tape(&setup).await.expect("add tape").expect("tape named");
        setup.chain.state().is_extension_failing = true;
        update(&setup, first, |fake| fake.expiry_epoch = EpochNumber(102));

        let report = run(&setup, 200).await;
        let moved = flush(&setup, 300).await;

        assert_eq!(report.vacated, vec![first]);
        assert_eq!(moved, FlushReport { written: 1, deleted: 1, outcome: FlushOutcome::Idle });
        let denied = HashSet::from([BlacklistEntry::track(address(1)), BlacklistEntry::track(address(2))]);
        assert_eq!(setup.chain.denied(rack), denied);
        assert_eq!(setup.chain.reached(rack), HashSet::from([rack, newest]));
        let tapes = tapes(&setup).await;
        assert_eq!(tapes.find(first).map(|tape| tape.state), Some(TapeState::Retired));
    }

    // an expired tape stops counting, and its decisions are written again on a live tape
    #[tokio::test]
    async fn expired() {
        let setup = created(FunderConfig::default(), 1).await;
        let (rack, first) = rack_and_newest(&setup).await;
        queue(&setup, 1).await;
        flush(&setup, 100).await;
        add_tape(&setup).await.expect("add tape").expect("tape named");
        update(&setup, first, |fake| fake.expiry_epoch = EpochNumber(100));

        let report = run(&setup, 200).await;
        let lapsed = setup.chain.denied(rack);
        let republished = flush(&setup, 300).await;

        assert_eq!(report.expired, vec![first]);
        assert!(lapsed.is_empty());
        assert_eq!(republished, FlushReport { written: 1, deleted: 1, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.denied(rack), HashSet::from([BlacklistEntry::track(address(1))]));
        let tapes = tapes(&setup).await;
        assert_eq!(tapes.find(first).map(|tape| tape.state), Some(TapeState::Expired));
    }
}
