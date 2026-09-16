//! What `/v1/ready` answers: one check per thing the service needs to do its job

use std::time::{Duration, Instant};

use serde::Serialize;

use crate::observe::state::{Limits, Pass, Watched};

/// Publishing passes that may fail in a row before the publisher counts as stuck
const STUCK_FAILURES: u64 = 3;

/// Every check, and whether the service can do its job
#[derive(Clone, Debug, Serialize)]
pub struct Readiness {
    #[serde(rename = "ready")]
    pub is_ready: bool,
    pub checks: Checks,
}

/// One check per thing a deploy gate cares about
#[derive(Clone, Debug, Serialize)]
pub struct Checks {
    pub chain: Check,
    pub database: Check,
    pub list: Check,
    pub publisher: Check,
    pub payer: Check,
}

/// A check and why it stands where it does
#[derive(Clone, Debug, Serialize)]
pub struct Check {
    #[serde(rename = "ready")]
    pub is_ready: bool,
    pub detail: String,
}

impl Readiness {
    pub fn new(watched: &Watched, limits: &Limits, now: Instant) -> Self {
        let checks = Checks {
            chain: chain(watched, limits, now),
            database: database(watched, limits, now),
            list: list(watched, limits, now),
            publisher: publisher(watched, limits, now),
            payer: payer(watched, limits, now),
        };
        Self {
            is_ready: checks.is_ready(),
            checks,
        }
    }
}

impl Checks {
    fn is_ready(&self) -> bool {
        let checks = [
            &self.chain,
            &self.database,
            &self.list,
            &self.publisher,
            &self.payer,
        ];
        checks.iter().all(|check| check.is_ready)
    }
}

impl Check {
    fn ready(detail: String) -> Self {
        Self {
            is_ready: true,
            detail,
        }
    }

    fn unready(detail: String) -> Self {
        Self {
            is_ready: false,
            detail,
        }
    }
}

/// The chain is reachable: the probe on the observe tick read the epoch inside the window
fn chain(watched: &Watched, limits: &Limits, now: Instant) -> Check {
    if let Some(reason) = stale(&watched.chain, limits.observe_window, now) {
        return Check::unready(format!("no chain read: {reason}"));
    }
    let epoch = match watched.epoch {
        Some(epoch) => epoch.0,
        None => 0,
    };
    Check::ready(format!(
        "epoch {epoch}, read {}s ago{}",
        seconds(&watched.chain, now),
        failure(&watched.chain)
    ))
}

/// The records are writable: an observe pass wrote its probe row inside the window
fn database(watched: &Watched, limits: &Limits, now: Instant) -> Check {
    match stale(&watched.observe, limits.observe_window, now) {
        Some(reason) => Check::unready(format!("no write: {reason}")),
        None => Check::ready(format!(
            "written {}s ago{}",
            seconds(&watched.observe, now),
            failure(&watched.observe)
        )),
    }
}

/// The list exists: the rack is declared and a blacklist tape takes decisions
fn list(watched: &Watched, limits: &Limits, now: Instant) -> Check {
    if let Some(reason) = stale(&watched.observe, limits.observe_window, now) {
        return Check::unready(format!("list not read: {reason}"));
    }
    let Some(observed) = watched.observed else {
        return Check::unready("list not read yet".to_string());
    };
    if !observed.has_rack {
        return Check::unready("no declared rack; run create-rack".to_string());
    }
    if !observed.has_tape {
        return Check::unready("no named blacklist tape to write to".to_string());
    }
    Check::ready("rack declared, newest blacklist tape named".to_string())
}

/// The publisher is not stuck: it passed recently, its writes land, and nothing waits too long
fn publisher(watched: &Watched, limits: &Limits, now: Instant) -> Check {
    if let Some(reason) = stale(&watched.publish, limits.publish_window, now) {
        return Check::unready(format!("no publishing pass: {reason}"));
    }
    if watched.publish.failures >= STUCK_FAILURES {
        return Check::unready(format!(
            "{} publishing passes failed in a row{}",
            watched.publish.failures,
            failure(&watched.publish)
        ));
    }
    let Some(observed) = watched.observed else {
        return Check::ready(format!("published {}s ago", seconds(&watched.publish, now)));
    };
    if observed.queue_age > limits.queue_stall {
        return Check::unready(format!(
            "a decision has waited {}s, over the {}s window",
            observed.queue_age.as_secs(),
            limits.queue_stall.as_secs()
        ));
    }
    Check::ready(format!(
        "published {}s ago, {} decisions queued{}",
        seconds(&watched.publish, now),
        observed.queued_decisions,
        failure(&watched.publish)
    ))
}

/// The payer is funded: both balances are above the funder's warning thresholds
fn payer(watched: &Watched, limits: &Limits, now: Instant) -> Check {
    if let Some(reason) = stale(&watched.upkeep, limits.upkeep_window, now) {
        return Check::unready(format!("no balance read: {reason}"));
    }
    let Some(balances) = watched.upkept.and_then(|facts| facts.balances) else {
        return Check::unready("payer balances not read".to_string());
    };
    if balances.sol < limits.sol_warn_below {
        return Check::unready(format!(
            "{} under the {} threshold",
            balances.sol, limits.sol_warn_below
        ));
    }
    if balances.tape < limits.tape_warn_below {
        return Check::unready(format!(
            "{} under the {} threshold",
            balances.tape, limits.tape_warn_below
        ));
    }
    Check::ready(format!("{}, {}", balances.sol, balances.tape))
}

/// Why what a pass recorded does not count: it never succeeded, or its success is too old
fn stale(pass: &Pass, window: Duration, now: Instant) -> Option<String> {
    let Some(age) = pass.age(now) else {
        return Some(match &pass.error {
            Some(error) => format!("no pass has succeeded yet: {error}"),
            None => "no pass has run yet".to_string(),
        });
    };
    let is_stale = age > window;
    is_stale.then(|| {
        format!(
            "last succeeded {}s ago, over the {}s window{}",
            age.as_secs(),
            window.as_secs(),
            failure(pass)
        )
    })
}

/// What to add to a detail when the last run failed, though an earlier one still counts
fn failure(pass: &Pass) -> String {
    match &pass.error {
        Some(error) => format!(", last run failed: {error}"),
        None => String::new(),
    }
}

fn seconds(pass: &Pass, now: Instant) -> u64 {
    pass.age(now).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use tape_core::types::coin::{SOL, TAPE};
    use tape_core::types::{EpochNumber, TrackNumber};

    use super::*;
    use crate::chain::tape_chain::PayerBalances;
    use crate::funder::upkeep::UpkeepFacts;
    use crate::observe::state::{Observation, Observer};

    const EPOCH: EpochNumber = EpochNumber(1_274);

    fn observer() -> Observer {
        Observer::try_new(Limits::default()).expect("observer")
    }

    fn observation() -> Observation {
        Observation {
            has_rack: true,
            has_tape: true,
            queued_decisions: 0,
            queue_age: Duration::ZERO,
            pending_jobs: 0,
            failed_jobs: 0,
        }
    }

    fn facts() -> UpkeepFacts {
        UpkeepFacts {
            balances: Some(PayerBalances {
                sol: SOL(2 * SOL::SCALE),
                tape: TAPE(100 * TAPE::SCALE),
            }),
            rack_epochs_left: Some(EpochNumber(40)),
            newest_epochs_left: Some(EpochNumber(40)),
            newest_free_tracks: Some(TrackNumber(30_000)),
        }
    }

    /// Every pass has run and everything it saw is healthy
    fn healthy() -> Observer {
        let observer = observer();
        observer.observed(Ok(observation()));
        observer.probed(Ok(EPOCH));
        observer.published(Ok(()));
        observer.upkept(Ok(facts()));
        observer
    }

    fn succeeded(at: Instant) -> Pass {
        Pass {
            last_success: Some(at),
            error: None,
            failures: 0,
        }
    }

    // nothing is ready before the passes have run, and each check says so
    #[test]
    fn unrun() {
        let readiness = observer().readiness(Instant::now());

        assert!(!readiness.is_ready);
        let checks = [
            readiness.checks.chain,
            readiness.checks.database,
            readiness.checks.list,
            readiness.checks.publisher,
            readiness.checks.payer,
        ];
        for check in checks {
            assert!(!check.is_ready);
            assert!(check.detail.contains("no pass has run yet"), "{}", check.detail);
        }
    }

    // every pass healthy answers ready
    #[test]
    fn ready() {
        let readiness = healthy().readiness(Instant::now());

        assert!(readiness.is_ready, "{:?}", readiness.checks);
    }

    // an epoch read that failed leaves the chain check unready, with the error, and the rest ready
    #[test]
    fn chain_unread() {
        let observer = observer();
        observer.observed(Ok(observation()));
        observer.published(Ok(()));
        observer.upkept(Ok(facts()));
        observer.probed(Err("connection refused".to_string()));

        let readiness = observer.readiness(Instant::now());

        assert!(!readiness.checks.chain.is_ready);
        assert!(readiness.checks.chain.detail.contains("connection refused"));
        assert!(readiness.checks.database.is_ready);
        assert!(readiness.checks.list.is_ready);
        assert!(readiness.checks.payer.is_ready);
    }

    // a chain probe older than its window turns the chain check unready on its own
    #[test]
    fn chain_stale() {
        let limits = Limits::default();
        let read_at = Instant::now();
        let now = read_at + limits.observe_window + Duration::from_secs(1);
        let watched = Watched {
            observe: succeeded(now),
            chain: succeeded(read_at),
            publish: succeeded(now),
            upkeep: succeeded(now),
            worker: succeeded(now),
            observed: Some(observation()),
            epoch: Some(EPOCH),
            upkept: Some(facts()),
        };

        let readiness = Readiness::new(&watched, &limits, now);

        assert!(!readiness.is_ready);
        assert!(!readiness.checks.chain.is_ready);
        assert!(readiness.checks.chain.detail.contains("over the"));
        assert!(readiness.checks.database.is_ready);
        assert!(readiness.checks.list.is_ready);
        assert!(readiness.checks.payer.is_ready);
    }

    // a records write that failed leaves the database and list checks unready, with the error
    #[test]
    fn database_unwritable() {
        let observer = observer();
        observer.probed(Ok(EPOCH));
        observer.published(Ok(()));
        observer.upkept(Ok(facts()));
        observer.observed(Err("attempt to write a readonly database".to_string()));

        let readiness = observer.readiness(Instant::now());

        assert!(!readiness.checks.database.is_ready);
        assert!(readiness.checks.database.detail.contains("readonly"));
        assert!(!readiness.checks.list.is_ready);
        assert!(readiness.checks.chain.is_ready);
    }

    // a list with no blacklist tape to write to is not ready
    #[test]
    fn list_missing() {
        let observer = healthy();
        observer.observed(Ok(Observation {
            has_tape: false,
            ..observation()
        }));

        let readiness = observer.readiness(Instant::now());

        assert!(!readiness.is_ready);
        assert!(!readiness.checks.list.is_ready);
        assert_eq!(readiness.checks.list.detail, "no named blacklist tape to write to");
    }

    // a rack that is not declared yet is not ready
    #[test]
    fn rack_missing() {
        let observer = healthy();
        observer.observed(Ok(Observation {
            has_rack: false,
            ..observation()
        }));

        let readiness = observer.readiness(Instant::now());

        assert!(readiness.checks.list.detail.contains("create-rack"));
    }

    // decisions waiting past the queue window, and writes that keep failing, stop the publisher
    #[test]
    fn publisher_stuck() {
        let limits = Limits::default();
        let waiting = healthy();
        waiting.observed(Ok(Observation {
            queue_age: limits.queue_stall + Duration::from_secs(1),
            queued_decisions: 4,
            ..observation()
        }));
        let failing = healthy();
        for _ in 0..STUCK_FAILURES {
            failing.published(Err("blockhash not found".to_string()));
        }

        let waited = waiting.readiness(Instant::now());
        let failed = failing.readiness(Instant::now());

        assert!(!waited.checks.publisher.is_ready);
        assert!(waited.checks.publisher.detail.contains("has waited"));
        assert!(!failed.checks.publisher.is_ready);
        assert!(failed.checks.publisher.detail.contains("blockhash not found"));
    }

    // a payer under either threshold is not ready
    #[test]
    fn payer_low() {
        let limits = Limits::default();
        let observer = healthy();
        observer.upkept(Ok(UpkeepFacts {
            balances: Some(PayerBalances {
                sol: SOL(limits.sol_warn_below.0 - 1),
                tape: TAPE(limits.tape_warn_below.0 - 1),
            }),
            ..facts()
        }));

        let readiness = observer.readiness(Instant::now());

        assert!(!readiness.is_ready);
        assert!(readiness.checks.payer.detail.contains("under the"));
    }

    // a ready service turns unready once its passes go stale
    #[test]
    fn staleness() {
        let limits = Limits::default();
        let observer = healthy();
        let now = Instant::now();

        let fresh = observer.readiness(now);
        let stale = observer.readiness(now + limits.upkeep_window + Duration::from_secs(1));

        assert!(fresh.is_ready);
        assert!(!stale.is_ready);
        assert!(!stale.checks.database.is_ready);
        assert!(stale.checks.database.detail.contains("over the"));
        assert!(!stale.checks.payer.is_ready);
    }
}
