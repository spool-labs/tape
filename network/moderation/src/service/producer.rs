//! The one task that writes to chain: publishing on a short tick, upkeep on a long one

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::tape_chain::TapeChain;
use crate::funder::upkeep::Funder;
use crate::observe::state::{Observation, Observer, OBSERVE_INTERVAL};
use crate::publisher::flush::{FlushOutcome, Publisher};
use crate::publisher::writer::TrackWriter;
use crate::records::store::Records;
use crate::types::timestamp::Timestamp;

/// How often the producer publishes and runs upkeep
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerIntervals {
    pub publish: Duration,
    pub upkeep: Duration,
}

/// Publishing and upkeep share a task, so the producer is the only writer of its tapes
pub struct Producer<Chain>
where
    Chain: TapeChain + TrackWriter,
{
    publisher: Publisher<Chain>,
    funder: Funder<Chain>,
    chain: Arc<Chain>,
    records: Records,
    observer: Arc<Observer>,
}

impl<Chain> Producer<Chain>
where
    Chain: TapeChain + TrackWriter,
{
    pub fn new(
        publisher: Publisher<Chain>,
        funder: Funder<Chain>,
        chain: Arc<Chain>,
        records: Records,
        observer: Arc<Observer>,
    ) -> Self {
        Self {
            publisher,
            funder,
            chain,
            records,
            observer,
        }
    }

    pub async fn run(&self, intervals: ProducerIntervals, cancel: CancellationToken) {
        let mut publish = interval(intervals.publish);
        publish.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut upkeep = interval(intervals.upkeep);
        upkeep.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut observe = interval(OBSERVE_INTERVAL);
        observe.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // Safe: waiting on cancellation holds no state
                () = cancel.cancelled() => return,
                // Safe: a tick holds no state; a dropped one waits for the next
                _ = publish.tick() => self.publish_decisions(&cancel).await,
                // Safe: a tick holds no state; a dropped one waits for the next
                _ = upkeep.tick() => self.upkeep_tapes(&cancel).await,
                // Safe: a tick holds no state; a dropped one waits for the next
                _ = observe.tick() => self.observe().await,
            }
        }
    }

    async fn publish_decisions(&self, cancel: &CancellationToken) {
        let report = match self.publisher.flush(Timestamp::now(), cancel).await {
            Ok(report) => {
                self.observer.published(Ok(()));
                report
            }
            Err(error) => {
                warn!(%error, "publishing the blacklist failed");
                self.observer.published(Err(error.to_string()));
                return;
            }
        };
        if report.written > 0 || report.deleted > 0 {
            info!(written = report.written, deleted = report.deleted, "published blacklist tracks");
        }
        match report.outcome {
            FlushOutcome::TapeFull => self.add_tape(cancel).await,
            FlushOutcome::NoTape => debug!("decisions wait for a named blacklist tape"),
            FlushOutcome::Idle | FlushOutcome::Waiting | FlushOutcome::Cancelled => {}
        }
    }

    async fn upkeep_tapes(&self, cancel: &CancellationToken) {
        match self.funder.run_once(&self.publisher, Timestamp::now(), cancel).await {
            Ok(report) => {
                self.observer.upkept(Ok(report.facts));
                debug!(?report, "blacklist tape upkeep ran");
            }
            Err(error) => {
                warn!(%error, "blacklist tape upkeep failed");
                self.observer.upkept(Err(error.to_string()));
            }
        }
    }

    /// Take a write on the records, then read the epoch: what readiness and metrics answer from
    ///
    /// The two are recorded apart, so a dead RPC leaves the database check alone
    /// and an unwritable database leaves the chain check alone.
    async fn observe(&self) {
        let now = Timestamp::now();
        let observed = match self.records.observe(now).await {
            Ok(summary) => Ok(Observation::new(&summary, now)),
            Err(error) => {
                warn!(%error, "reading the records for readiness failed");
                Err(error.to_string())
            }
        };
        self.observer.observed(observed);

        let probed = match self.chain.current_epoch().await {
            Ok(epoch) => Ok(epoch),
            Err(error) => {
                warn!(%error, "reading the current epoch failed");
                Err(error.to_string())
            }
        };
        self.observer.probed(probed);
    }

    async fn add_tape(&self, cancel: &CancellationToken) {
        if let Err(error) = self.funder.add_tape(&self.publisher, Timestamp::now(), cancel).await {
            warn!(%error, "adding a blacklist tape failed");
        }
    }
}
