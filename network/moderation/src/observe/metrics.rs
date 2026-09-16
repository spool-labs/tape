//! Prometheus metrics in an own registry, rendered on demand

use std::time::Instant;

use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};

use crate::funder::upkeep::UpkeepFacts;
use crate::observe::error::ObserveError;
use crate::observe::state::{Limits, Observation, Pass, PassName, ReportOutcome};
use crate::records::states::CheckOutcome;

/// Every metric name starts with this
const NAMESPACE: &str = "tape_moderation";

/// What a gauge reads before the pass that sets it has succeeded
const UNKNOWN: i64 = -1;

/// Everything `/metrics` exports
pub struct Metrics {
    registry: Registry,
    payer_sol: IntGauge,
    payer_sol_warn_below: IntGauge,
    payer_tape: IntGauge,
    payer_tape_warn_below: IntGauge,
    rack_epochs_to_expiry: IntGauge,
    blacklist_epochs_to_expiry: IntGauge,
    blacklist_free_tracks: IntGauge,
    decisions_queued: IntGauge,
    decision_queue_age: IntGauge,
    jobs_pending: IntGauge,
    jobs_failed: IntGauge,
    publish_failures: IntCounter,
    reports: IntCounterVec,
    checks: IntCounterVec,
    pass_ok: IntGaugeVec,
    pass_age: IntGaugeVec,
}

impl Metrics {
    /// Register every metric, with the payer's thresholds set from the config
    pub fn try_new(limits: &Limits) -> Result<Self, ObserveError> {
        let registry = Registry::new();
        let metrics = Self {
            payer_sol: gauge(&registry, "payer_sol_lamports", "Payer SOL balance in lamports")?,
            payer_sol_warn_below: gauge(
                &registry,
                "payer_sol_warn_below_lamports",
                "Lamports the payer's SOL balance warns below",
            )?,
            payer_tape: gauge(&registry, "payer_tape_flux", "Payer TAPE balance in flux")?,
            payer_tape_warn_below: gauge(&registry, "payer_tape_warn_below_flux", "Flux the payer's TAPE balance warns below")?,
            rack_epochs_to_expiry: gauge(&registry, "rack_epochs_to_expiry", "Epochs until the rack expires")?,
            blacklist_epochs_to_expiry: gauge(
                &registry,
                "blacklist_epochs_to_expiry",
                "Epochs until the newest blacklist tape expires",
            )?,
            blacklist_free_tracks: gauge(&registry, "blacklist_free_tracks", "Free track slots on the newest blacklist tape")?,
            decisions_queued: gauge(&registry, "decisions_queued", "Decisions no track holds yet")?,
            decision_queue_age: gauge(&registry, "decision_queue_age_seconds", "How long the oldest queued decision has waited")?,
            jobs_pending: gauge(&registry, "jobs_pending", "Check and listing jobs still to run")?,
            jobs_failed: gauge(&registry, "jobs_failed", "Jobs that ran out of attempts")?,
            publish_failures: counter(&registry, "publish_failures_total", "Publishing passes that failed")?,
            reports: counter_vec(&registry, "reports_total", "Reports submitted, by what the API did with each", "outcome")?,
            checks: counter_vec(&registry, "checks_total", "Checks run, by verdict", "verdict")?,
            pass_ok: gauge_vec(&registry, "pass_ok", "Whether a background pass last succeeded", "pass")?,
            pass_age: gauge_vec(&registry, "pass_age_seconds", "How long ago a background pass last succeeded", "pass")?,
            registry,
        };
        metrics.unset();
        metrics.payer_sol_warn_below.set(count(limits.sol_warn_below.lamports()));
        metrics.payer_tape_warn_below.set(count(limits.tape_warn_below.flux()));
        Ok(metrics)
    }

    /// Every gauge reads `UNKNOWN` until the pass that sets it has succeeded
    fn unset(&self) {
        let gauges = [
            &self.payer_sol,
            &self.payer_tape,
            &self.rack_epochs_to_expiry,
            &self.blacklist_epochs_to_expiry,
            &self.blacklist_free_tracks,
            &self.decisions_queued,
            &self.decision_queue_age,
            &self.jobs_pending,
            &self.jobs_failed,
        ];
        for gauge in gauges {
            gauge.set(UNKNOWN);
        }
    }

    /// What the records showed an observe pass
    pub fn observed(&self, observation: &Observation) {
        self.decisions_queued.set(count(observation.queued_decisions));
        self.decision_queue_age.set(count(observation.queue_age.as_secs()));
        self.jobs_pending.set(count(observation.pending_jobs));
        self.jobs_failed.set(count(observation.failed_jobs));
    }

    /// What the chain showed an upkeep pass
    pub fn upkept(&self, facts: &UpkeepFacts) {
        if let Some(balances) = facts.balances {
            self.payer_sol.set(count(balances.sol.lamports()));
            self.payer_tape.set(count(balances.tape.flux()));
        }
        set(&self.rack_epochs_to_expiry, facts.rack_epochs_left.map(|left| left.0));
        set(&self.blacklist_epochs_to_expiry, facts.newest_epochs_left.map(|left| left.0));
        set(&self.blacklist_free_tracks, facts.newest_free_tracks.map(|free| free.0));
    }

    pub fn publish_failed(&self) {
        self.publish_failures.inc();
    }

    pub fn reported(&self, outcome: ReportOutcome) {
        self.reports.with_label_values(&[outcome.as_str()]).inc();
    }

    pub fn checked(&self, outcome: CheckOutcome) {
        self.checks.with_label_values(&[outcome.as_str()]).inc();
    }

    /// Age and state of one pass, as of this scrape
    pub fn passed(&self, name: PassName, pass: &Pass, now: Instant) {
        let age = match pass.age(now) {
            Some(age) => count(age.as_secs()),
            None => UNKNOWN,
        };
        self.pass_age.with_label_values(&[name.as_str()]).set(age);
        self.pass_ok.with_label_values(&[name.as_str()]).set(i64::from(pass.is_ok()));
    }

    pub fn render(&self) -> String {
        let mut text = String::new();
        let _encoded = TextEncoder::new().encode_utf8(&self.registry.gather(), &mut text);
        text
    }
}

fn gauge(registry: &Registry, name: &str, help: &str) -> Result<IntGauge, ObserveError> {
    let gauge = IntGauge::with_opts(options(name, help))?;
    registry.register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

fn gauge_vec(registry: &Registry, name: &str, help: &str, label: &str) -> Result<IntGaugeVec, ObserveError> {
    let gauge = IntGaugeVec::new(options(name, help), &[label])?;
    registry.register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

fn counter(registry: &Registry, name: &str, help: &str) -> Result<IntCounter, ObserveError> {
    let counter = IntCounter::with_opts(options(name, help))?;
    registry.register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn counter_vec(registry: &Registry, name: &str, help: &str, label: &str) -> Result<IntCounterVec, ObserveError> {
    let counter = IntCounterVec::new(options(name, help), &[label])?;
    registry.register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn options(name: &str, help: &str) -> Opts {
    Opts::new(name, help).namespace(NAMESPACE)
}

/// A gauge whose value is not known yet reads `UNKNOWN`, not zero
fn set(gauge: &IntGauge, value: Option<u64>) {
    match value {
        Some(value) => gauge.set(count(value)),
        None => gauge.set(UNKNOWN),
    }
}

fn count(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tape_core::types::coin::{SOL, TAPE};
    use tape_core::types::{EpochNumber, TrackNumber};

    use super::*;
    use crate::chain::tape_chain::PayerBalances;
    use crate::observe::state::Observer;

    fn observer() -> Observer {
        Observer::try_new(Limits::default()).expect("observer")
    }

    // the payer's balances and the thresholds they are judged against render as gauges
    #[test]
    fn balances() {
        let limits = Limits::default();
        let observer = observer();
        observer.upkept(Ok(UpkeepFacts {
            balances: Some(PayerBalances {
                sol: SOL(3 * SOL::SCALE),
                tape: TAPE(7 * TAPE::SCALE),
            }),
            rack_epochs_left: Some(EpochNumber(40)),
            newest_epochs_left: Some(EpochNumber(12)),
            newest_free_tracks: Some(TrackNumber(30_000)),
        }));

        let text = observer.render(Instant::now());

        assert!(text.contains("tape_moderation_payer_sol_lamports 3000000000"), "{text}");
        assert!(text.contains("tape_moderation_payer_tape_flux 7000000000"), "{text}");
        assert!(
            text.contains(&format!(
                "tape_moderation_payer_sol_warn_below_lamports {}",
                limits.sol_warn_below.lamports()
            )),
            "{text}"
        );
        assert!(
            text.contains(&format!(
                "tape_moderation_payer_tape_warn_below_flux {}",
                limits.tape_warn_below.flux()
            )),
            "{text}"
        );
        assert!(text.contains("tape_moderation_rack_epochs_to_expiry 40"), "{text}");
        assert!(text.contains("tape_moderation_blacklist_epochs_to_expiry 12"), "{text}");
        assert!(text.contains("tape_moderation_blacklist_free_tracks 30000"), "{text}");
    }

    // passes report their state and age, and a pass that never ran reports an unknown age
    #[test]
    fn passes() {
        let observer = observer();
        observer.published(Err("blockhash not found".to_string()));
        observer.observed(Ok(Observation {
            has_rack: true,
            has_tape: true,
            queued_decisions: 5,
            queue_age: Duration::from_secs(9),
            pending_jobs: 2,
            failed_jobs: 1,
        }));
        let now = Instant::now();

        let text = observer.render(now + Duration::from_secs(4));

        assert!(text.contains(r#"tape_moderation_pass_ok{pass="observe"} 1"#), "{text}");
        assert!(text.contains(r#"tape_moderation_pass_age_seconds{pass="observe"} 4"#), "{text}");
        assert!(text.contains(r#"tape_moderation_pass_ok{pass="publish"} 0"#), "{text}");
        assert!(text.contains(r#"tape_moderation_pass_age_seconds{pass="worker"} -1"#), "{text}");
        assert!(text.contains("tape_moderation_publish_failures_total 1"), "{text}");
        assert!(text.contains("tape_moderation_decisions_queued 5"), "{text}");
        assert!(text.contains("tape_moderation_decision_queue_age_seconds 9"), "{text}");
        assert!(text.contains("tape_moderation_jobs_pending 2"), "{text}");
        assert!(text.contains("tape_moderation_jobs_failed 1"), "{text}");
    }

    // reports and checks count by what became of each
    #[test]
    fn outcomes() {
        let observer = observer();
        observer.reported(ReportOutcome::Accepted);
        observer.reported(ReportOutcome::Accepted);
        observer.reported(ReportOutcome::Rejected);
        observer.checked(CheckOutcome::Remove);

        let text = observer.render(Instant::now());

        assert!(text.contains(r#"tape_moderation_reports_total{outcome="accepted"} 2"#), "{text}");
        assert!(text.contains(r#"tape_moderation_reports_total{outcome="rejected"} 1"#), "{text}");
        assert!(text.contains(r#"tape_moderation_checks_total{verdict="remove"} 1"#), "{text}");
    }
}
