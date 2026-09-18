//! One publishing pass over the list's tapes

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tape_blacklist::batch::{
    parse as parse_entries, render_first as render_blacklist_header, render_tracks as render_entry_tracks,
};
use tape_blacklist::entry::Entry;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_rack::format::{
    parse as parse_records, render_first as render_rack_header, render_tracks as render_record_tracks,
};
use tape_rack::record::Record;
use tape_sdk::stream::manifest::MAX_TRACKS_PER_TAPE;
use tape_sdk::track::write::SDK_INLINE_RAW_MAX_BYTES;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chain::error::ChainError;
use crate::chain::tape_chain::TapeChain;
use crate::publisher::error::PublishError;
use crate::publisher::reconcile::reconcile;
use crate::publisher::writer::{TrackWriter, WrittenTrack};
use crate::records::rows::{Decision, ListTape, TrackContent, TrackRow};
use crate::records::states::{TapeRole, TrackKind};
use crate::records::store::Records;
use crate::types::ids::DecisionId;
use crate::types::timestamp::Timestamp;

/// Largest track: the SDK's one-transaction inline write
pub const MAX_TRACK_BYTES: usize = SDK_INLINE_RAW_MAX_BYTES;

/// Decisions read per pass; a pass that writes everything loops for more
const DECISIONS_PER_PASS: usize = 512;

const FIRST_TRACK: TrackNumber = TrackNumber(0);

/// Writes the list's tracks and deletes the ones that must go
pub struct Publisher<Chain>
where
    Chain: TapeChain + TrackWriter,
{
    records: Records,
    chain: Arc<Chain>,
    window: Duration,
}

/// Why a flush stopped writing entries
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FlushOutcome {
    /// Nothing left to publish
    #[default]
    Idle,
    /// A partial track waits for its window, or the newest tape is not settled yet
    Waiting,
    /// The newest blacklist tape has no track left
    TapeFull,
    /// No blacklist tape is declared and named yet
    NoTape,
    Cancelled,
}

/// What a flush wrote and deleted
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FlushReport {
    pub written: usize,
    pub deleted: usize,
    pub outcome: FlushOutcome,
}

enum Pass {
    Again,
    Stop(FlushOutcome),
}

impl<Chain> Publisher<Chain>
where
    Chain: TapeChain + TrackWriter,
{
    pub fn new(records: Records, chain: Arc<Chain>, window: Duration) -> Self {
        Self {
            records,
            chain,
            window,
        }
    }

    /// Settle uncertain tapes, write headers, rack records and entries, then delete what must go
    ///
    /// Every track is recorded with its bytes before it is written, and every delete
    /// before it is sent, so a flush stopped anywhere leaves records the next flush
    /// settles against the chain. A track is deleted only once what it names that
    /// should stay is present on another track.
    pub async fn flush(&self, now: Timestamp, cancel: &CancellationToken) -> Result<FlushReport, PublishError> {
        self.records.sweep(now).await?;
        let unsettled = self.reconcile_tapes(now, cancel).await?;

        let mut written = self.retry_writes(&unsettled, now, cancel).await?;
        written += self.write_headers(&unsettled, now, cancel).await?;
        written += self.write_records(&unsettled, now, cancel).await?;
        let (entries, outcome) = self.write_entries(&unsettled, now, cancel).await?;

        self.records.sweep(now).await?;
        let deleted = self.delete_tracks(&unsettled, now, cancel).await?;
        self.records.sweep(now).await?;

        let outcome = match cancel.is_cancelled() {
            true => FlushOutcome::Cancelled,
            false => outcome,
        };
        Ok(FlushReport {
            written: written + entries,
            deleted,
            outcome,
        })
    }

    /// Settle every tape whose records may lag the chain, returning the ones still unsettled
    async fn reconcile_tapes(
        &self,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<HashSet<Address>, PublishError> {
        let tapes = self.records.list_tapes().await?;
        let mut unsettled = HashSet::new();
        for tape in tapes.all() {
            if !tape.is_active() || !tape.is_uncertain() {
                continue;
            }
            // Safe to cancel: settling records only what the chain already shows
            let settling = reconcile(&self.records, self.chain.as_ref(), tape, now);
            match cancel.run_until_cancelled(settling).await {
                Some(Ok(())) => {}
                Some(Err(error)) => {
                    warn!(tape = %tape.address, %error, "settling a blacklist tape against the chain failed");
                    unsettled.insert(tape.address);
                }
                None => {
                    unsettled.insert(tape.address);
                }
            }
        }
        Ok(unsettled)
    }

    /// Write again the planned tracks that did not land
    async fn retry_writes(
        &self,
        unsettled: &HashSet<Address>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<usize, PublishError> {
        let tapes = self.records.list_tapes().await?;
        let mut written = 0;
        for tape in tapes.all() {
            if !tape.is_active() || tape.writing_tracks.0 == 0 || unsettled.contains(&tape.address) {
                continue;
            }
            for track in self.records.writing_tracks(tape.address).await? {
                if cancel.is_cancelled() {
                    return Ok(written);
                }
                if self.send(tape, &track, now, cancel).await?.is_some() {
                    written += 1;
                }
            }
        }
        Ok(written)
    }

    /// Declare every reserved tape nothing has landed on, with its header alone in track 0
    async fn write_headers(
        &self,
        unsettled: &HashSet<Address>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<usize, PublishError> {
        let tapes = self.records.list_tapes().await?;
        let mut written = 0;
        for tape in tapes.all() {
            if !tape.needs_header() || unsettled.contains(&tape.address) || cancel.is_cancelled() {
                continue;
            }
            let header = match tape.role {
                TapeRole::Rack => render_rack_header(&[]),
                TapeRole::Blacklist => render_blacklist_header(&[])?,
            };
            if self.write(tape, TrackContent::Header, header, now, cancel).await?.is_some() {
                info!(tape = %tape.address, role = tape.role.as_str(), "declared a list tape");
                written += 1;
            }
        }
        Ok(written)
    }

    /// Name in the rack every declared blacklist tape no present or planned rack track names
    async fn write_records(
        &self,
        unsettled: &HashSet<Address>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<usize, PublishError> {
        let tapes = self.records.list_tapes().await?;
        let Some(rack) = tapes.rack.as_ref() else {
            return Ok(0);
        };
        if !rack.is_active() || !rack.is_declared || unsettled.contains(&rack.address) {
            return Ok(0);
        }
        let named = self.records.tapes_to_name().await?;
        let mut records = Vec::new();
        for tape in &named {
            records.push(Record::new(*tape));
        }

        let mut next_track = rack.next_track;
        let mut offset = 0;
        let mut written = 0;
        for bytes in render_record_tracks(&records, MAX_TRACK_BYTES)? {
            if cancel.is_cancelled() {
                break;
            }
            if next_track.0 >= MAX_TRACKS_PER_TAPE {
                return Err(PublishError::RackFull(rack.address));
            }
            let count = parse_records(&bytes)?.len();
            let held = named.get(offset..offset + count).ok_or(PublishError::Mismatch)?;
            let content = TrackContent::Records(held.to_vec());
            let Some(landed) = self.write(rack, content, bytes, now, cancel).await? else {
                break;
            };
            info!(rack = %rack.address, named = count, "named blacklist tapes in the rack");
            next_track = TrackNumber(landed.track_number.0.saturating_add(1));
            offset += count;
            written += 1;
        }
        Ok(written)
    }

    /// Write queued decisions on the newest blacklist tape: full tracks now, a partial one once its window passes
    async fn write_entries(
        &self,
        unsettled: &HashSet<Address>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<(usize, FlushOutcome), PublishError> {
        let mut written = 0;
        loop {
            let queued = self.records.queued_decisions(DECISIONS_PER_PASS).await?;
            if queued.is_empty() {
                return Ok((written, FlushOutcome::Idle));
            }
            let tapes = self.records.list_tapes().await?;
            let Some(newest) = tapes.newest() else {
                return Ok((written, FlushOutcome::NoTape));
            };
            if unsettled.contains(&newest.address) {
                return Ok((written, FlushOutcome::Waiting));
            }

            let (pass, count) = self.publish(&queued, newest, now, cancel).await?;
            written += count;
            if let Pass::Stop(outcome) = pass {
                return Ok((written, outcome));
            }
        }
    }

    async fn publish(
        &self,
        queued: &[Decision],
        tape: &ListTape,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<(Pass, usize), PublishError> {
        let entries: Vec<Entry> = queued.iter().map(|decision| decision.entry).collect();
        let tracks = render_entry_tracks(&entries, MAX_TRACK_BYTES)?;
        let total = tracks.len();
        let oldest = queued
            .iter()
            .map(|decision| decision.created_at)
            .min()
            .unwrap_or(now);
        let writable = match now.since(oldest) >= self.window {
            true => total,
            false => total.saturating_sub(1),
        };

        let mut offset = 0;
        let mut next_track = tape.next_track;
        let mut written = 0;
        for bytes in tracks.into_iter().take(writable) {
            if cancel.is_cancelled() {
                return Ok((Pass::Stop(FlushOutcome::Cancelled), written));
            }
            if next_track.0 >= MAX_TRACKS_PER_TAPE {
                return Ok((Pass::Stop(FlushOutcome::TapeFull), written));
            }
            let count = parse_entries(&bytes)?.len();
            let held = queued.get(offset..offset + count).ok_or(PublishError::Mismatch)?;
            let mut decisions: Vec<DecisionId> = Vec::new();
            for decision in held {
                decisions.push(decision.id);
            }

            let content = TrackContent::Entries(decisions);
            let Some(landed) = self.write(tape, content, bytes, now, cancel).await? else {
                return Ok((Pass::Stop(FlushOutcome::Cancelled), written));
            };
            next_track = TrackNumber(landed.track_number.0.saturating_add(1));
            offset += count;
            written += 1;
        }

        let pass = match writable == total {
            true => Pass::Again,
            false => Pass::Stop(FlushOutcome::Waiting),
        };
        Ok((pass, written))
    }

    /// Delete tracks whose kept contents are present elsewhere, with at most one unsettled delete per tape
    async fn delete_tracks(
        &self,
        unsettled: &HashSet<Address>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<usize, PublishError> {
        let tapes = self.records.list_tapes().await?;
        let mut deleted = 0;
        for track in self.records.deletable_tracks().await? {
            let (Some(tape), Some(address)) = (tapes.find(track.tape), track.address) else {
                continue;
            };
            // A second unsettled delete would leave the tape's track count unable to say which landed
            let is_blocked = tape.sent_deletes.0 > 0 && !track.is_delete_sent;
            if is_blocked || unsettled.contains(&tape.address) || cancel.is_cancelled() {
                continue;
            }
            if self.delete(tape, &track, address, now, cancel).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Record a track, then write it
    async fn write(
        &self,
        tape: &ListTape,
        content: TrackContent,
        bytes: Vec<u8>,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<Option<WrittenTrack>, PublishError> {
        let track = self.records.plan_track(tape.address, content, bytes, now).await?;
        self.send(tape, &track, now, cancel).await
    }

    /// Write a recorded track and record where it landed
    async fn send(
        &self,
        tape: &ListTape,
        track: &TrackRow,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<Option<WrittenTrack>, PublishError> {
        // Safe to cancel: the recorded track is found landed, or written again, on the next flush
        let Some(written) = cancel.run_until_cancelled(self.chain.write_inline(tape, &track.bytes)).await else {
            return Ok(None);
        };
        let written = written.map_err(|source| PublishError::Write {
            tape: tape.address,
            source,
        })?;
        self.records.land_track(track.id, written, now).await?;
        if track.kind == TrackKind::Header && written.track_number != FIRST_TRACK {
            return Err(PublishError::HeaderNotFirst {
                tape: tape.address,
                track_number: written.track_number,
            });
        }
        Ok(Some(written))
    }

    /// Delete one track, recording the attempt first so a lost result is settled by the tape's track count
    async fn delete(
        &self,
        tape: &ListTape,
        track: &TrackRow,
        address: Address,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<bool, PublishError> {
        self.records.mark_delete_sent(track.id, now).await?;
        // Safe to cancel: the recorded attempt is settled by the tape's track count on the next flush
        let Some(deleted) = cancel.run_until_cancelled(self.chain.delete_track(tape, address)).await else {
            return Ok(false);
        };
        match deleted {
            Ok(()) => {}
            Err(ChainError::TrackNotFound(_)) => return self.settle_missing(tape, address, now).await,
            Err(source) => return Err(PublishError::Delete { track: address, source }),
        }
        self.records.mark_deleted(vec![track.id], now).await?;
        Ok(true)
    }

    /// No peer holds a track being deleted: an earlier attempt landed, or peers lag; the track count decides
    async fn settle_missing(&self, tape: &ListTape, address: Address, now: Timestamp) -> Result<bool, PublishError> {
        reconcile(&self.records, self.chain.as_ref(), tape, now).await?;
        let is_settled = self.records.sent_deletes(tape.address).await?.is_empty();
        match is_settled {
            true => Ok(true),
            false => Err(PublishError::Delete {
                track: address,
                source: ChainError::TrackNotFound(address),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::PathBuf;

    use rusqlite::Connection;
    use tape_blacklist::batch::HEADER as BLACKLIST_HEADER;
    use tape_core::system::BlacklistEntry;
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_rack::format::HEADER as RACK_HEADER;
    use tempfile::TempDir;

    use super::*;
    use crate::chain::fake::{DeleteFault, FakeChain, Landed, WriteFault};
    use crate::config::settings::{FunderConfig, TapeRuleConfig, TapesConfig};
    use crate::funder::error::FunderError;
    use crate::funder::upkeep::Funder;
    use crate::records::rows::{CategoryScore, CheckAttempt, CheckSettlement, NewReport, Removal, TapeObservation};
    use crate::records::states::{CheckOutcome, TapeState, TargetState};
    use crate::types::category::Category;
    use crate::types::reason::Reason;
    use crate::types::target::Target;
    use crate::worker::content::InputType;
    use crate::worker::tape_rule::TapeRule;

    const WINDOW: Duration = Duration::from_secs(10);
    const MIB: u64 = 1_024 * 1_024;

    struct Setup {
        dir: TempDir,
        records: Records,
        chain: Arc<FakeChain>,
        publisher: Publisher<FakeChain>,
        funder: Funder<FakeChain>,
        rack: Address,
        tape: Address,
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn database(dir: &TempDir) -> PathBuf {
        dir.path().join("records.sqlite")
    }

    /// The service starting on its records and a chain
    fn start(dir: TempDir, chain: Arc<FakeChain>, rack: Address, tape: Address) -> Setup {
        let records = Records::open(&database(&dir)).expect("open records");
        let publisher = Publisher::new(records.clone(), Arc::clone(&chain), WINDOW);
        let tapes = TapesConfig {
            key_dir: dir.path().join("tapes"),
            initial_capacity: StorageUnits(MIB),
            initial_epochs: EpochNumber(52),
        };
        let funder = Funder::new(records.clone(), Arc::clone(&chain), FunderConfig::default(), tapes);
        Setup {
            dir,
            records,
            chain,
            publisher,
            funder,
            rack,
            tape,
        }
    }

    fn empty() -> Setup {
        let dir = tempfile::tempdir().expect("temp dir");
        start(dir, Arc::new(FakeChain::new(EpochNumber(100))), address(0), address(0))
    }

    /// A rack naming one declared blacklist tape
    async fn setup() -> Setup {
        let mut setup = empty();
        let created = setup
            .funder
            .create_rack(&setup.publisher, Timestamp(0))
            .await
            .expect("create rack");
        setup.rack = created.rack.address;
        setup.tape = created.blacklist.address;
        setup
    }

    /// The service stopped and started again on the same records and chain
    fn restart(setup: Setup) -> Setup {
        start(setup.dir, setup.chain, setup.rack, setup.tape)
    }

    async fn queue(setup: &Setup, byte: u8, at: i64) {
        setup
            .records
            .queue_operator_decision(Entry::remove(address(byte)), Timestamp(at))
            .await
            .expect("queue decision");
    }

    async fn restore(setup: &Setup, byte: u8, at: i64) {
        setup
            .records
            .restore_target(Target::track(address(byte)), Timestamp(at))
            .await
            .expect("restore");
    }

    async fn flush(setup: &Setup, at: i64) -> Result<FlushReport, PublishError> {
        setup.publisher.flush(Timestamp(at), &CancellationToken::new()).await
    }

    async fn add_tape(setup: &Setup) -> Address {
        setup
            .funder
            .add_tape(&setup.publisher, Timestamp(10), &CancellationToken::new())
            .await
            .expect("add tape")
            .expect("tape named")
    }

    fn removed(bytes: &[u8]) -> HashSet<BlacklistEntry> {
        let mut targets = HashSet::new();
        for byte in bytes {
            targets.insert(BlacklistEntry::track(address(*byte)));
        }
        targets
    }

    fn lines(bytes: &[u8]) -> String {
        let mut text = String::new();
        for byte in bytes {
            text.push_str(&format!("remove track {}\n", address(*byte)));
        }
        text
    }

    fn count(setup: &Setup, sql: &str) -> u64 {
        let connection = Connection::open(database(&setup.dir)).expect("open database");
        connection.query_row(sql, [], |row| row.get(0)).expect("count")
    }

    fn position(setup: &Setup, event: Landed) -> usize {
        let landed = setup.chain.state().landed.clone();
        landed.iter().position(|seen| *seen == event).expect("landed")
    }

    // the rack and blacklist tape hold only their header in track 0, and a later rack track names the tape
    #[tokio::test]
    async fn headers() {
        let setup = setup().await;

        let rack = setup.chain.tracks(setup.rack);
        let tape = setup.chain.tracks(setup.tape);

        let record = format!("tape {}\n", setup.tape);
        assert_eq!(rack, vec![(TrackNumber(0), format!("{RACK_HEADER}\n")), (TrackNumber(1), record)]);
        assert_eq!(tape, vec![(TrackNumber(0), format!("{BLACKLIST_HEADER}\n"))]);
        assert_eq!(setup.chain.reached(setup.rack), HashSet::from([setup.rack, setup.tape]));
    }

    // a blacklist tape whose header has not landed is not named, and is named once it lands
    #[tokio::test]
    async fn declared_first() {
        let setup = empty();
        setup.chain.state().write_faults.extend([WriteFault::Lands, WriteFault::Refused]);

        let refused = setup.funder.create_rack(&setup.publisher, Timestamp(0)).await;
        let rack = setup.records.list_tapes().await.expect("tapes").rack.expect("rack").address;
        let before = setup.chain.tracks(rack);
        let created = setup
            .funder
            .create_rack(&setup.publisher, Timestamp(1))
            .await
            .expect("resume");

        let tape = created.blacklist.address;
        assert!(matches!(refused, Err(FunderError::Publish(PublishError::Write { .. }))));
        assert_eq!(before, vec![(TrackNumber(0), format!("{RACK_HEADER}\n"))]);
        let header = position(&setup, Landed::Write { tape, track_number: TrackNumber(0) });
        let record = position(&setup, Landed::Write { tape: rack, track_number: TrackNumber(1) });
        assert!(header < record);
        assert_eq!(setup.chain.reached(rack), HashSet::from([rack, tape]));
    }

    // decisions pack in order into tracks after the header, within the inline limit
    #[tokio::test]
    async fn batching() {
        let setup = setup().await;
        let bytes: Vec<u8> = (1..=30).collect();
        for byte in &bytes {
            queue(&setup, *byte, 0).await;
        }

        let report = flush(&setup, 100).await.expect("flush");

        let tracks = setup.chain.tracks(setup.tape);
        assert_eq!(report, FlushReport { written: tracks.len() - 1, deleted: 0, outcome: FlushOutcome::Idle });
        assert!(tracks.len() > 2);
        let mut text = String::new();
        for (_, track) in &tracks[1..] {
            assert!(track.len() <= MAX_TRACK_BYTES);
            text.push_str(track);
        }
        assert_eq!(text, lines(&bytes));
        assert_eq!(setup.chain.denied(setup.rack), removed(&bytes));
        assert_eq!(count(&setup, "SELECT COUNT(*) FROM decisions WHERE track IS NULL"), 0);
    }

    // a partial track waits for the window, then publishes
    #[tokio::test]
    async fn window() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;

        let early = flush(&setup, 9).await.expect("early flush");
        let due = flush(&setup, 10).await.expect("due flush");

        assert_eq!(early, FlushReport { written: 0, deleted: 0, outcome: FlushOutcome::Waiting });
        assert_eq!(due, FlushReport { written: 1, deleted: 0, outcome: FlushOutcome::Idle });
    }

    // a full track publishes before the window while the rest waits
    #[tokio::test]
    async fn full_batch() {
        let setup = setup().await;
        for byte in 1..=20 {
            queue(&setup, byte, 0).await;
        }

        let report = flush(&setup, 1).await.expect("flush");

        assert_eq!(report, FlushReport { written: 1, deleted: 0, outcome: FlushOutcome::Waiting });
        let tracks = setup.chain.tracks(setup.tape);
        let first = parse_entries(tracks[1].1.as_bytes()).expect("parse").len() as u64;
        assert_eq!(count(&setup, "SELECT COUNT(*) FROM decisions WHERE track IS NULL"), 20 - first);
    }

    // tracks hold only entries: no report, reason, note or score
    #[tokio::test]
    async fn private_fields() {
        let setup = setup().await;
        let track = address(3);
        let report = NewReport {
            target: Target::track(track),
            reason: Reason::Hate,
            note: Some("note-SECRET-context".to_string()),
            idempotency_key: "idempotency-key-0001".to_string(),
            reporter_hash: "reporter-hash-SECRET".to_string(),
        };
        setup.records.submit_report(report, Timestamp(0)).await.expect("submit");
        let job = setup.records.next_due_job(Timestamp(0)).await.expect("due").expect("job");
        let mut attempt = CheckAttempt::new(job.id, track, CheckOutcome::Remove, Timestamp(0));
        attempt.input_type = Some(InputType::Text);
        attempt.detail = Some("flagged: hate".to_string());
        attempt.categories = vec![CategoryScore {
            result_index: 0,
            category: "hate".to_string(),
            flagged: Some(true),
            score: Some(0.987_654),
            applied_input_types: Some(vec!["text".to_string()]),
        }];
        let settlement = CheckSettlement {
            job,
            attempt,
            state: TargetState::Removed,
            tape: Some(address(90)),
            removal: Some(Removal {
                chunks: Vec::new(),
                categories: vec![Category::Hate],
            }),
        };
        let rule = TapeRule::new(&TapeRuleConfig::default());
        setup.records.settle_check(settlement, rule).await.expect("settle");

        flush(&setup, 100).await.expect("flush");

        let tracks = setup.chain.tracks(setup.tape);
        assert_eq!(tracks.len(), 2);
        let text = &tracks[1].1;
        assert_eq!(*text, lines(&[3]));
        for private in ["note-SECRET", "reporter-hash", "idempotency", "0.98", "flagged"] {
            assert!(!text.contains(private), "track leaks `{private}`");
        }
    }

    // restoring a target writes the other entries of its track into a new track, then deletes the old one
    #[tokio::test]
    async fn rewrite() {
        let setup = setup().await;
        for byte in 1..=3 {
            queue(&setup, byte, 0).await;
        }
        flush(&setup, 100).await.expect("publish");
        restore(&setup, 2, 101).await;

        let report = flush(&setup, 102).await.expect("rewrite");

        assert_eq!(report, FlushReport { written: 1, deleted: 1, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.denied(setup.rack), removed(&[1, 3]));
        let tracks = setup.chain.tracks(setup.tape);
        assert_eq!(tracks[1..], [(TrackNumber(2), lines(&[1, 3]))]);
        let written = position(&setup, Landed::Write { tape: setup.tape, track_number: TrackNumber(2) });
        let deleted = position(&setup, Landed::Delete { tape: setup.tape, track_number: TrackNumber(1) });
        assert!(written < deleted);
    }

    // restoring the only entry of a track deletes it without writing anything
    #[tokio::test]
    async fn restore_alone() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;
        flush(&setup, 100).await.expect("publish");
        restore(&setup, 1, 101).await;

        let report = flush(&setup, 102).await.expect("delete");

        assert_eq!(report, FlushReport { written: 0, deleted: 1, outcome: FlushOutcome::Idle });
        assert!(setup.chain.denied(setup.rack).is_empty());
        assert_eq!(setup.chain.tracks(setup.tape).len(), 1);
    }

    // a restart after the rewrite lands and before the old track is deleted deletes it once, allowing nothing in between
    #[tokio::test]
    async fn restart_between() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;
        queue(&setup, 2, 0).await;
        flush(&setup, 100).await.expect("publish");
        restore(&setup, 2, 101).await;
        setup.chain.state().delete_faults.push_back(DeleteFault::Refused);
        let stopped = flush(&setup, 102).await;
        let between = setup.chain.denied(setup.rack);
        let setup = restart(setup);

        let report = flush(&setup, 200).await.expect("flush after restart");

        assert!(matches!(stopped, Err(PublishError::Delete { .. })));
        assert_eq!(between, removed(&[1, 2]));
        assert_eq!(report, FlushReport { written: 0, deleted: 1, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.denied(setup.rack), removed(&[1]));
        let tracks = setup.chain.tracks(setup.tape);
        assert_eq!(tracks[1..], [(TrackNumber(2), lines(&[1]))]);
    }

    // a delete that landed but was not recorded is settled by the tape's track count, not sent again
    #[tokio::test]
    async fn lost_delete() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;
        queue(&setup, 2, 0).await;
        flush(&setup, 100).await.expect("publish");
        restore(&setup, 2, 101).await;
        setup.chain.state().delete_faults.push_back(DeleteFault::Lost);
        let stopped = flush(&setup, 102).await;
        let setup = restart(setup);

        let report = flush(&setup, 200).await.expect("flush after restart");

        assert!(matches!(stopped, Err(PublishError::Delete { .. })));
        assert_eq!(report, FlushReport { written: 0, deleted: 0, outcome: FlushOutcome::Idle });
        assert_eq!(count(&setup, "SELECT COUNT(*) FROM tracks WHERE state = 'deleting'"), 0);
        let deletes = setup.chain.state().landed.iter().filter(|event| matches!(event, Landed::Delete { .. })).count();
        assert_eq!(deletes, 1);
        assert_eq!(setup.chain.denied(setup.rack), removed(&[1]));
    }

    // a write that landed but was not recorded is found by its key after a restart, not written again
    #[tokio::test]
    async fn lost_write() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;
        queue(&setup, 2, 0).await;
        setup.chain.state().write_faults.push_back(WriteFault::Lost);
        let stopped = flush(&setup, 100).await;
        let setup = restart(setup);

        let report = flush(&setup, 200).await.expect("flush after restart");

        assert!(matches!(stopped, Err(PublishError::Write { .. })));
        assert_eq!(report, FlushReport { written: 0, deleted: 0, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.tracks(setup.tape)[1..], [(TrackNumber(1), lines(&[1, 2]))]);
        assert_eq!(count(&setup, "SELECT COUNT(*) FROM tracks WHERE kind = 'entries'"), 1);
        assert_eq!(count(&setup, "SELECT COUNT(*) FROM entries"), 2);
        assert_eq!(setup.chain.denied(setup.rack), removed(&[1, 2]));
    }

    // a write that landed twice has its copy deleted, leaving one track per decision
    #[tokio::test]
    async fn doubled_write() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;
        queue(&setup, 2, 0).await;
        setup.chain.state().write_faults.push_back(WriteFault::Doubled);

        let first = flush(&setup, 100).await.expect("first flush");
        let second = flush(&setup, 200).await.expect("second flush");

        assert_eq!(first, FlushReport { written: 1, deleted: 0, outcome: FlushOutcome::Idle });
        assert_eq!(second, FlushReport { written: 0, deleted: 1, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.tracks(setup.tape)[1..], [(TrackNumber(2), lines(&[1, 2]))]);
        assert_eq!(setup.chain.denied(setup.rack), removed(&[1, 2]));
    }

    // a write that did not land is written again from its record, its decisions held once
    #[tokio::test]
    async fn refused_write() {
        let setup = setup().await;
        queue(&setup, 1, 0).await;
        queue(&setup, 2, 0).await;
        setup.chain.state().write_faults.push_back(WriteFault::Refused);

        let stopped = flush(&setup, 100).await;
        let report = flush(&setup, 200).await.expect("flush again");

        assert!(matches!(stopped, Err(PublishError::Write { .. })));
        assert_eq!(report, FlushReport { written: 1, deleted: 0, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.tracks(setup.tape)[1..], [(TrackNumber(1), lines(&[1, 2]))]);
        assert_eq!(count(&setup, "SELECT COUNT(*) FROM entries"), 2);
    }

    // entries stop once the newest tape has no track left
    #[tokio::test]
    async fn tape_full() {
        let setup = setup().await;
        let observation = TapeObservation {
            next_track: TrackNumber(MAX_TRACKS_PER_TAPE),
            expiry_epoch: EpochNumber(152),
            capacity: StorageUnits(MIB),
            used: StorageUnits(0),
        };
        setup.records.observe_tape(setup.tape, observation, Timestamp(0)).await.expect("observe");
        queue(&setup, 1, 0).await;

        let report = flush(&setup, 100).await.expect("flush");

        assert_eq!(report, FlushReport { written: 0, deleted: 0, outcome: FlushOutcome::TapeFull });
        assert_eq!(setup.chain.tracks(setup.tape).len(), 1);
    }

    // entries go only to the newest named tape, never an older one or one not yet named
    #[tokio::test]
    async fn newest_only() {
        let setup = setup().await;
        let newest = add_tape(&setup).await;
        let pending = address(202);
        setup
            .records
            .insert_tape(pending, TapeRole::Blacklist, PathBuf::from("pending.json"), Timestamp(0))
            .await
            .expect("pending tape");
        for byte in 1..=30 {
            queue(&setup, byte, 0).await;
        }

        let report = flush(&setup, 100).await.expect("flush");

        assert_eq!(report.outcome, FlushOutcome::Idle);
        assert_eq!(setup.chain.tracks(setup.tape).len(), 1);
        assert!(setup.chain.tracks(newest).len() > 2);
        assert!(setup.chain.tracks(pending).is_empty());
    }

    // a vacating tape's decisions are written on the newest tape before the rack stops naming it
    #[tokio::test]
    async fn vacate() {
        let setup = setup().await;
        for byte in 1..=3 {
            queue(&setup, byte, 0).await;
        }
        flush(&setup, 100).await.expect("publish");
        let newest = add_tape(&setup).await;
        setup
            .records
            .set_tape_state(setup.tape, TapeState::Vacating, Timestamp(150))
            .await
            .expect("vacate");

        let report = flush(&setup, 200).await.expect("move");

        assert_eq!(report, FlushReport { written: 1, deleted: 1, outcome: FlushOutcome::Idle });
        assert_eq!(setup.chain.denied(setup.rack), removed(&[1, 2, 3]));
        assert_eq!(setup.chain.reached(setup.rack), HashSet::from([setup.rack, newest]));
        assert_eq!(setup.chain.tracks(newest)[1..], [(TrackNumber(1), lines(&[1, 2, 3]))]);
        let tapes = setup.records.list_tapes().await.expect("tapes");
        assert_eq!(tapes.find(setup.tape).map(|tape| tape.state), Some(TapeState::Retired));
    }
}
