//! What the background passes saw, which readiness and metrics answer from

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tape_core::types::coin::{SOL, TAPE};
use tape_core::types::EpochNumber;

use crate::config::settings::{FunderConfig, PublisherConfig};
use crate::funder::upkeep::UpkeepFacts;
use crate::observe::error::ObserveError;
use crate::observe::metrics::Metrics;
use crate::observe::ready::Readiness;
use crate::records::rows::StatusSummary;
use crate::records::states::{CheckOutcome, TapeState};
use crate::types::timestamp::Timestamp;

/// How often the service records what its records and the chain say
pub const OBSERVE_INTERVAL: Duration = Duration::from_secs(15);

/// Intervals a pass may miss before what it records counts as stale
const STALE_PASSES: u32 = 3;

/// Batch windows a decision may wait before the publisher counts as stuck
const QUEUE_STALL_WINDOWS: u32 = 30;

/// A background pass of the running service
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PassName {
    /// Writes to the records and reads what they say
    Observe,
    /// Reads the current epoch, on the observe tick
    Chain,
    Publish,
    Upkeep,
    Worker,
}

/// Windows a pass may go unrecorded, how long a decision may wait, and the payer's thresholds
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Covers the observe pass and the chain probe that shares its tick
    pub observe_window: Duration,
    pub publish_window: Duration,
    pub upkeep_window: Duration,
    /// A queued decision waiting longer than this leaves the publisher unready
    pub queue_stall: Duration,
    pub sol_warn_below: SOL,
    pub tape_warn_below: TAPE,
}

/// When a pass last succeeded, and what its last run failed with
#[derive(Clone, Debug, Default)]
pub struct Pass {
    pub last_success: Option<Instant>,
    pub error: Option<String>,
    /// Runs that failed since the last success
    pub failures: u64,
}

/// What the records showed an observe pass
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Observation {
    /// The rack is reserved and its header is present
    pub has_rack: bool,
    /// A blacklist tape is declared and named, so decisions have somewhere to go
    pub has_tape: bool,
    pub queued_decisions: u64,
    /// How long the oldest queued decision has waited
    pub queue_age: Duration,
    pub pending_jobs: u64,
    pub failed_jobs: u64,
}

/// Everything the passes recorded
#[derive(Clone, Debug, Default)]
pub struct Watched {
    pub observe: Pass,
    pub chain: Pass,
    pub publish: Pass,
    pub upkeep: Pass,
    pub worker: Pass,
    pub observed: Option<Observation>,
    /// The epoch the chain probe last read
    pub epoch: Option<EpochNumber>,
    pub upkept: Option<UpkeepFacts>,
}

/// How a submitted report ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportOutcome {
    Accepted,
    Duplicate,
    /// Over its caller's limit, acknowledged without recording
    Limited,
    /// The body or target was malformed
    Rejected,
    /// The records refused it
    Failed,
}

/// What every pass records into, and what `/v1/ready` and `/metrics` read
pub struct Observer {
    limits: Limits,
    metrics: Metrics,
    watched: Mutex<Watched>,
}

impl PassName {
    pub const ALL: [Self; 5] = [
        Self::Observe,
        Self::Chain,
        Self::Publish,
        Self::Upkeep,
        Self::Worker,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Chain => "chain",
            Self::Publish => "publish",
            Self::Upkeep => "upkeep",
            Self::Worker => "worker",
        }
    }
}

impl Limits {
    pub fn new(publisher: &PublisherConfig, funder: &FunderConfig) -> Self {
        let publish_interval = publisher.poll_interval.max(publisher.batch_window);
        Self {
            observe_window: OBSERVE_INTERVAL.saturating_mul(STALE_PASSES),
            publish_window: publish_interval.saturating_mul(STALE_PASSES),
            upkeep_window: funder.interval.saturating_mul(STALE_PASSES),
            queue_stall: publisher.batch_window.saturating_mul(QUEUE_STALL_WINDOWS),
            sol_warn_below: funder.sol_warn_below,
            tape_warn_below: funder.tape_warn_below,
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::new(&PublisherConfig::default(), &FunderConfig::default())
    }
}

impl Pass {
    fn record(&mut self, outcome: Result<(), String>, now: Instant) {
        match outcome {
            Ok(()) => {
                self.last_success = Some(now);
                self.error = None;
                self.failures = 0;
            }
            Err(error) => {
                self.error = Some(error);
                self.failures = self.failures.saturating_add(1);
            }
        }
    }

    /// Whether the pass succeeded, and nothing has failed since
    pub fn is_ok(&self) -> bool {
        self.last_success.is_some() && self.error.is_none()
    }

    /// Time since the last success; `None` when no pass has succeeded yet
    pub fn age(&self, now: Instant) -> Option<Duration> {
        self.last_success
            .map(|success| now.saturating_duration_since(success))
    }
}

impl Observation {
    pub fn new(summary: &StatusSummary, now: Timestamp) -> Self {
        let has_rack = summary
            .tapes
            .rack
            .as_ref()
            .is_some_and(|rack| rack.state == TapeState::Reserved && rack.is_declared);
        let queue_age = match summary.oldest_queued {
            Some(oldest) => now.since(oldest),
            None => Duration::ZERO,
        };
        Self {
            has_rack,
            has_tape: summary.tapes.newest().is_some(),
            queued_decisions: summary.queued_decisions,
            queue_age,
            pending_jobs: summary.pending_jobs,
            failed_jobs: summary.failed_jobs,
        }
    }
}

impl Watched {
    pub fn pass(&self, name: PassName) -> &Pass {
        match name {
            PassName::Observe => &self.observe,
            PassName::Chain => &self.chain,
            PassName::Publish => &self.publish,
            PassName::Upkeep => &self.upkeep,
            PassName::Worker => &self.worker,
        }
    }
}

impl ReportOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Duplicate => "duplicate",
            Self::Limited => "limited",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }
}

impl Observer {
    pub fn try_new(limits: Limits) -> Result<Self, ObserveError> {
        Ok(Self {
            metrics: Metrics::try_new(&limits)?,
            limits,
            watched: Mutex::new(Watched::default()),
        })
    }

    /// An observe pass: the records took a write, and said this
    pub fn observed(&self, observed: Result<Observation, String>) {
        let now = Instant::now();
        let mut watched = self.lock();
        match observed {
            Ok(observation) => {
                self.metrics.observed(&observation);
                watched.observed = Some(observation);
                watched.observe.record(Ok(()), now);
            }
            Err(error) => watched.observe.record(Err(error), now),
        }
    }

    /// The chain probe of an observe pass: one epoch read, recorded apart from the records
    pub fn probed(&self, probed: Result<EpochNumber, String>) {
        let now = Instant::now();
        let mut watched = self.lock();
        match probed {
            Ok(epoch) => {
                watched.epoch = Some(epoch);
                watched.chain.record(Ok(()), now);
            }
            Err(error) => watched.chain.record(Err(error), now),
        }
    }

    /// A publishing pass
    pub fn published(&self, published: Result<(), String>) {
        let now = Instant::now();
        if published.is_err() {
            self.metrics.publish_failed();
        }
        self.lock().publish.record(published, now);
    }

    /// An upkeep pass, which read the chain before anything else
    pub fn upkept(&self, upkept: Result<UpkeepFacts, String>) {
        let now = Instant::now();
        let mut watched = self.lock();
        match upkept {
            Ok(facts) => {
                self.metrics.upkept(&facts);
                watched.upkept = Some(facts);
                watched.upkeep.record(Ok(()), now);
            }
            Err(error) => watched.upkeep.record(Err(error), now),
        }
    }

    /// A worker pass over the jobs that were due
    pub fn worked(&self, worked: Result<(), String>) {
        let now = Instant::now();
        self.lock().worker.record(worked, now);
    }

    pub fn reported(&self, outcome: ReportOutcome) {
        self.metrics.reported(outcome);
    }

    pub fn checked(&self, outcome: CheckOutcome) {
        self.metrics.checked(outcome);
    }

    /// Every check and its state, as of `now`
    pub fn readiness(&self, now: Instant) -> Readiness {
        let watched = self.lock().clone();
        Readiness::new(&watched, &self.limits, now)
    }

    /// The Prometheus text, with each pass aged as of `now`
    pub fn render(&self, now: Instant) -> String {
        let watched = self.lock().clone();
        for name in PassName::ALL {
            self.metrics.passed(name, watched.pass(name), now);
        }
        self.metrics.render()
    }

    fn lock(&self) -> MutexGuard<'_, Watched> {
        self.watched.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::records::store::Records;

    // an observe pass writes its probe row and reads back a list with nothing on it
    #[tokio::test]
    async fn observed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("records.sqlite");
        let records = Records::open(&path).expect("open records");

        let summary = records.observe(Timestamp(100)).await.expect("observe");
        let observation = Observation::new(&summary, Timestamp(100));

        let connection = Connection::open(&path).expect("open database");
        let written: i64 = connection
            .query_row("SELECT written_at FROM probe", [], |row| row.get(0))
            .expect("probe row");
        assert_eq!(written, 100);
        assert!(!observation.has_rack);
        assert!(!observation.has_tape);
        assert_eq!(observation.queue_age, Duration::ZERO);
    }
}
