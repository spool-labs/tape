//! The one task that writes to chain: publishing on a short tick, upkeep on a long one

use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::tape_chain::TapeChain;
use crate::funder::upkeep::Funder;
use crate::publisher::flush::{FlushOutcome, Publisher};
use crate::publisher::writer::TrackWriter;
use crate::types::timestamp::Timestamp;

/// How often the producer publishes and runs upkeep
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerIntervals {
    pub publish: Duration,
    pub upkeep: Duration,
}

/// Publishing and upkeep share a task, so the producer is the only writer of its tapes
pub async fn run_producer<Chain>(
    publisher: Publisher<Chain>,
    funder: Funder<Chain>,
    intervals: ProducerIntervals,
    cancel: CancellationToken,
) where
    Chain: TapeChain + TrackWriter,
{
    let mut publish = interval(intervals.publish);
    publish.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut upkeep = interval(intervals.upkeep);
    upkeep.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Safe: waiting on cancellation holds no state
            () = cancel.cancelled() => return,
            // Safe: a tick holds no state; a dropped one waits for the next
            _ = publish.tick() => publish_decisions(&publisher, &funder, &cancel).await,
            // Safe: a tick holds no state; a dropped one waits for the next
            _ = upkeep.tick() => upkeep_tapes(&publisher, &funder, &cancel).await,
        }
    }
}

async fn publish_decisions<Chain>(publisher: &Publisher<Chain>, funder: &Funder<Chain>, cancel: &CancellationToken)
where
    Chain: TapeChain + TrackWriter,
{
    let report = match publisher.flush(Timestamp::now(), cancel).await {
        Ok(report) => report,
        Err(error) => {
            warn!(%error, "publishing the blacklist failed");
            return;
        }
    };
    if report.written > 0 || report.deleted > 0 {
        info!(written = report.written, deleted = report.deleted, "published blacklist tracks");
    }
    match report.outcome {
        FlushOutcome::TapeFull => add_tape(publisher, funder, cancel).await,
        FlushOutcome::NoTape => debug!("decisions wait for a named blacklist tape"),
        FlushOutcome::Idle | FlushOutcome::Waiting | FlushOutcome::Cancelled => {}
    }
}

async fn upkeep_tapes<Chain>(publisher: &Publisher<Chain>, funder: &Funder<Chain>, cancel: &CancellationToken)
where
    Chain: TapeChain + TrackWriter,
{
    match funder.run_once(publisher, Timestamp::now(), cancel).await {
        Ok(report) => debug!(?report, "blacklist tape upkeep ran"),
        Err(error) => warn!(%error, "blacklist tape upkeep failed"),
    }
}

async fn add_tape<Chain>(publisher: &Publisher<Chain>, funder: &Funder<Chain>, cancel: &CancellationToken)
where
    Chain: TapeChain + TrackWriter,
{
    if let Err(error) = funder.add_tape(publisher, Timestamp::now(), cancel).await {
        warn!(%error, "adding a blacklist tape failed");
    }
}
