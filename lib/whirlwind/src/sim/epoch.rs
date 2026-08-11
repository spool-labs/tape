//! The continuous multi-epoch Whirlwind engine.
//!
//! One persistent scoreboard and one timeline are created once and threaded
//! through every round of every epoch. Each epoch places the current committee at
//! real cities and runs a challenge round for every spool position. A round is
//! scheduled in Solana slots, fixed at epoch start; its entropy block is the first
//! slot in the round's span to produce a finalized block, and if the span produces
//! none the round is void and counts against nobody. Each non-void round draws a
//! byte-weighted sub-leaf sample, builds a real two-level merkle proof, converts
//! the injected one-way latency to slots against the proof deadline, and forms a
//! real BLS certificate once the threshold sign. At the boundary a cause-free
//! q-of-n eviction vote driven only by local scores signs and verifies a real BLS
//! eviction certificate that excludes targets from the next committee, where a
//! fresh honest node takes each freed slice position. The scoreboard is never
//! reset across a boundary, which is what makes the run continuous.
//!
//! The premise under test is that a spool must answer faster than it could fetch
//! discarded data from its peers, so the proof deadline is the discriminator: an
//! offline or selective spool stops delivering and a colluder cannot forge a
//! quorum, while whether a fetch or reconstruct free-rider is caught depends on
//! how the proof deadline slots compare with its fetch surcharge.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;

use anyhow::{bail, ensure, Result};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::encoding::ClayParams;
use tape_core::types::{EpochNumber, NodeId, StorageUnits};
use tape_crypto::hash::Hash;

use crate::crypto::{aggregate, eviction_message, keypair, sign, verify_certificate};
use crate::network::simulated::ping_data::PingData;
use crate::network::simulated::Group;
use crate::sim::latency::Costs;
use crate::sim::node::{designate_victim, willing_to_evict, Node};
use crate::sim::round::{run_round, RelayPolicy};
use crate::sim::schedule::{ProofDeadline, Schedule};
use crate::sim::scoreboard::{EvictionReason, EvictionRule, Scoreboard};
use crate::sim::timeline::{Attestation, ChallengeRecord, Event, NodeRef, Timeline};
use crate::sim::track::SpoolHoldings;
use crate::spool::{AGREEMENT_THRESHOLD, LEAF_COUNT};
use crate::types::{GroupPosition, RoundNumber, SlotCount};

pub use crate::sim::node::{Behavior, BehaviorConfig};

/// Static group identifier for this single-group simulation.
pub const GROUP_ID: u64 = 0;

/// How much real BLS to run, trading fidelity for wall-clock. Eviction BLS is
/// always real regardless of this setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlsMode {
    Full,
    Sampled,
    Off,
}

impl BlsMode {
    /// Short label for reports.
    pub fn label(&self) -> &'static str {
        match self {
            BlsMode::Full => "full",
            BlsMode::Sampled => "sampled",
            BlsMode::Off => "off",
        }
    }
}

/// How many of each behavior make up the initial committee.
#[derive(Clone, Copy, Debug)]
pub struct Roster {
    pub honest: usize,
    pub nearby_fetch: usize,
    pub reconstruct: usize,
    pub selective: usize,
    pub offline: usize,
    pub colluders: usize,
}

impl Default for Roster {
    fn default() -> Self {
        Self {
            honest: 14,
            nearby_fetch: 1,
            reconstruct: 1,
            selective: 1,
            offline: 1,
            colluders: 2,
        }
    }
}

impl Roster {
    /// One of each singleton adversary, the colluder bloc, honest filling the rest
    pub fn with_colluders(group_size: usize, colluders: usize) -> Self {
        let base = Self::default();
        let adversaries = base.nearby_fetch + base.reconstruct + base.selective + base.offline;
        Self {
            honest: group_size.saturating_sub(adversaries + colluders),
            colluders,
            ..base
        }
    }

    /// Total members described by the roster.
    pub fn total(&self) -> usize {
        self.honest
            + self.nearby_fetch
            + self.reconstruct
            + self.selective
            + self.offline
            + self.colluders
    }

    /// Expand into per-position behavior and bloc assignments.
    ///
    /// The colluders form one bloc. Its first member is the free-rider that never
    /// holds a valid proof; the rest prove honestly but withhold as verifiers.
    fn expand(&self, group_size: usize) -> Result<Vec<(Behavior, Option<u32>)>> {
        if self.total() != group_size {
            bail!("roster totals {} but group size is {}", self.total(), group_size);
        }
        let mut slots = Vec::with_capacity(group_size);
        for _ in 0..self.honest {
            slots.push((Behavior::Honest, None));
        }
        for _ in 0..self.nearby_fetch {
            slots.push((Behavior::NearbyFetch, None));
        }
        for _ in 0..self.reconstruct {
            slots.push((Behavior::Reconstruct, None));
        }
        for _ in 0..self.selective {
            slots.push((Behavior::Selective, None));
        }
        for _ in 0..self.offline {
            slots.push((Behavior::Offline, None));
        }
        for index in 0..self.colluders {
            let behavior = if index == 0 {
                Behavior::CollusiveFreeRider
            } else {
                Behavior::Honest
            };
            slots.push((behavior, Some(0)));
        }
        Ok(slots)
    }
}

/// Everything needed to run a continuous multi-epoch simulation.
#[derive(Clone)]
pub struct EpochConfig {
    /// Number of epochs to run back to back.
    pub epochs: u64,
    /// Challenge rounds within each epoch.
    pub rounds_per_epoch: u64,
    /// Committee size, held constant across boundaries.
    pub group_size: usize,
    /// Attestation and eviction quorum.
    pub threshold: usize,
    /// Payload size of the largest coded track in the shared holdings.
    pub blob_bytes: usize,
    /// Slot schedule and block production model.
    pub schedule: Schedule,
    /// Round trip to a nearby metro edge for a fetch free-rider.
    pub edge_rtt_ms: f64,
    /// How far a proof travels after the challenged owner sends it.
    pub relay: RelayPolicy,
    /// How much real BLS to run for certificates.
    pub bls_mode: BlsMode,
    /// Base seed for placement, behavior, and block production draws.
    pub seed: u64,
    /// Composition of the initial committee.
    pub roster: Roster,
    /// Tunables for the time-varying behaviors.
    pub behavior: BehaviorConfig,
    /// Local rule that fires an eviction vote.
    pub eviction: EvictionRule,
    /// Whether to emit per-pair score events into the timeline.
    pub verbose: bool,
}

impl EpochConfig {
    /// Enforce the paper's parameter relations, with f pinned by q = n - f
    pub fn validate(&self) -> Result<()> {
        self.schedule.validate()?;
        ensure!(
            self.rounds_per_epoch > 0,
            "an epoch with no rounds challenges nobody, so the run would report a clean sheet it never earned",
        );
        let params = ClayParams::default();
        let (n, q) = (self.group_size, self.threshold);
        let (k, d) = (params.k() as usize, params.d() as usize);
        ensure!(0 < q && q <= n, "threshold q={q} outside 1..=n for n={n}");
        let f = n - q;
        ensure!(k + f <= q, "no certificate margin: k={k} plus f={f} exceeds q={q}");
        ensure!(2 * q > n + f, "quorums do not intersect: 2q={} at most n+f={}", 2 * q, n + f);
        ensure!(k <= d && d < n, "repair helpers d={d} outside {k}..{n}");
        Ok(())
    }
}

impl Default for EpochConfig {
    fn default() -> Self {
        let rounds_per_epoch = 48;
        Self {
            epochs: 12,
            rounds_per_epoch,
            group_size: LEAF_COUNT,
            threshold: AGREEMENT_THRESHOLD,
            blob_bytes: 1_000_000,
            schedule: default_schedule(),
            edge_rtt_ms: 20.0,
            relay: RelayPolicy::Flood,
            bls_mode: BlsMode::Full,
            seed: 1,
            roster: Roster::default(),
            behavior: BehaviorConfig {
                selective_deliver_probability: 0.4,
                offline_after_round: RoundNumber::new(rounds_per_epoch / 3),
            },
            eviction: EvictionRule::default(),
            verbose: false,
        }
    }
}

/// The default slot schedule: a round every 150 slots (60 s at the real Solana
/// slot time), a four-slot entropy span, a two-slot proof deadline, and a
/// confirmation one slot after production.
pub fn default_schedule() -> Schedule {
    Schedule {
        slot_ms: 400.0,
        round_interval_slots: SlotCount::new(150),
        span_slots: SlotCount::new(4),
        proof_deadline: ProofDeadline::Slots(SlotCount::new(2)),
        confirmation_slots: SlotCount::new(1),
        attestation_window_slots: SlotCount::new(4),
        certificate_gossip_slots: SlotCount::new(2),
        block_production_probability: 0.95,
    }
}

/// One node's excluded-from-next-committee decision.
#[derive(Clone, Copy)]
pub struct Eviction {
    pub node: NodeId,
    pub behavior: Behavior,
    pub reason: EvictionReason,
}

/// What one epoch produced.
pub struct EpochSummary {
    pub epoch: EpochNumber,
    pub rounds: u64,
    pub certificates: u64,
    pub gaps: u64,
    pub voids: u64,
    pub evicted: Vec<Eviction>,
    pub joined: Vec<NodeId>,
    pub committee: Vec<NodeId>,
}

impl EpochSummary {
    /// Fraction of challenged round-and-target pairs that certified.
    pub fn certificate_rate(&self) -> f64 {
        let total = self.certificates + self.gaps;
        if total == 0 {
            return 0.0;
        }
        self.certificates as f64 / total as f64
    }

    /// One-line stream summary: epoch, certs, gaps, voids, cert percent, boundary.
    pub fn stream_line(&self) -> String {
        format!(
            "epoch {:>4}  certs {:>5}  gaps {:>4}  voids {:>3}  cert {:>5.1}%  {}",
            self.epoch.as_u64(),
            self.certificates,
            self.gaps,
            self.voids,
            self.certificate_rate() * 100.0,
            epoch_boundary_line(self),
        )
    }
}

/// One node's lifetime across the whole run.
#[derive(Clone, Copy, Serialize)]
pub struct NodeOutcome {
    pub id: NodeId,
    pub behavior: Behavior,
    pub bloc: Option<u32>,
    pub joined_epoch: EpochNumber,
    pub evicted_epoch: Option<EpochNumber>,
    pub reason: Option<EvictionReason>,
    pub final_rate: Option<f64>,
}

/// The full result of a multi-epoch run, with an inherent printer.
pub struct SimulationReport {
    pub config: EpochConfig,
    pub costs: Costs,
    pub deadline_ms: f64,
    pub per_epoch: Vec<EpochSummary>,
    pub outcomes: Vec<NodeOutcome>,
    pub final_committee: Vec<NodeRef>,
    pub cert_count: u64,
    pub gap_count: u64,
    pub void_count: u64,
    pub scoreboard: Scoreboard,
    pub timeline: Timeline,
}

/// One epoch's eviction votes, aggregated and verified at the boundary.
struct EvictionLedger {
    partials: HashMap<NodeId, Vec<Voter>>,
    reasons: HashMap<NodeId, EvictionReason>,
}

/// One real eviction-proposal partial signature.
struct Voter {
    pubkey: BlsPubkey,
    signature: BlsSignature,
}

impl EvictionLedger {
    fn new() -> Self {
        Self {
            partials: HashMap::new(),
            reasons: HashMap::new(),
        }
    }

    fn record(
        &mut self,
        target: NodeId,
        pubkey: BlsPubkey,
        signature: BlsSignature,
        reason: EvictionReason,
    ) {
        self.partials
            .entry(target)
            .or_default()
            .push(Voter { pubkey, signature });
        self.reasons.entry(target).or_insert(reason);
    }

    /// Aggregate and verify each target that reached the quorum.
    fn finalize(&self, threshold: usize, epoch: u64, group: u64) -> Result<BTreeMap<NodeId, EvictionReason>> {
        let mut decided = BTreeMap::new();
        for (target, voters) in &self.partials {
            if voters.len() < threshold {
                continue;
            }
            let message = eviction_message(epoch, group, target.as_u64());
            let signatures: Vec<BlsSignature> = voters.iter().map(|voter| voter.signature).collect();
            let pubkeys: Vec<BlsPubkey> = voters.iter().map(|voter| voter.pubkey).collect();
            let aggregate_signature = aggregate(&signatures)?;
            if verify_certificate(&aggregate_signature, &message, &pubkeys) {
                if let Some(reason) = self.reasons.get(target) {
                    decided.insert(*target, *reason);
                }
            }
        }
        Ok(decided)
    }
}

/// Run the continuous multi-epoch simulation and return its report.
///
/// This measures the crypto and reconstruction costs once on this machine and
/// then runs the engine against them.
pub fn simulate_epochs(data: &PingData, config: &EpochConfig) -> Result<SimulationReport> {
    let costs = Costs::measure(config.threshold, config.blob_bytes, config.group_size)?;
    simulate_epochs_with_costs(data, config, costs)
}

/// Run the engine against a caller-supplied cost model.
///
/// The public entry measures the costs first; tests and multi-configuration
/// callers such as the deadline sweep measure once and inject the model so each
/// run does not pay the one-time measurement.
pub fn simulate_epochs_with_costs(
    data: &PingData,
    config: &EpochConfig,
    costs: Costs,
) -> Result<SimulationReport> {
    let mut sim = Simulation::new(data, config, costs)?;
    let mut per_epoch: Vec<EpochSummary> = Vec::with_capacity(config.epochs as usize);
    for _ in 0..config.epochs {
        per_epoch.push(sim.step_epoch()?);
    }
    Ok(sim.into_report(per_epoch))
}

/// A continuous multi-epoch run advanced one epoch at a time.
pub struct Simulation<'a> {
    data: &'a PingData,
    config: EpochConfig,
    costs: Costs,
    holdings: SpoolHoldings,
    rng: SmallRng,
    committee: Vec<Node>,
    served: Vec<Node>,
    next_node_id: u64,
    scoreboard: Scoreboard,
    timeline: Timeline,
    next_epoch_index: u64,
    cert_count: u64,
    gap_count: u64,
    void_count: u64,
}

impl<'a> Simulation<'a> {
    /// Set up the shared holdings, initial committee, costs, and scoreboard.
    pub fn new(data: &'a PingData, config: &EpochConfig, costs: Costs) -> Result<Self> {
        config.validate()?;
        let mut rng = SmallRng::seed_from_u64(config.seed);
        let holdings = SpoolHoldings::build(config.blob_bytes, config.group_size)?;
        let committee = build_initial_committee(data, config, &mut rng)?;
        let next_node_id = config.group_size as u64;
        let served = committee.clone();
        Ok(Self {
            data,
            config: config.clone(),
            costs,
            holdings,
            rng,
            committee,
            served,
            next_node_id,
            scoreboard: Scoreboard::new(),
            timeline: Timeline::new(config.verbose),
            next_epoch_index: 0,
            cert_count: 0,
            gap_count: 0,
            void_count: 0,
        })
    }

    /// Run one epoch and return its summary.
    pub fn step_epoch(&mut self) -> Result<EpochSummary> {
        let epoch = EpochNumber(self.next_epoch_index);
        let next_epoch = EpochNumber(self.next_epoch_index + 1);
        let server_ids: Vec<usize> = self.committee.iter().map(|node| node.server_id).collect();
        let group = Group::place(self.data, &server_ids)?;
        self.timeline
            .push_epoch_started(epoch, committee_refs(&self.committee, self.data));

        let deadline_ms = self.config.schedule.proof_deadline_ms();
        let mut epoch_certs = 0u64;
        let mut epoch_gaps = 0u64;
        let mut epoch_voids = 0u64;

        for raw_round in 0..self.config.rounds_per_epoch {
            let round_index = RoundNumber::new(raw_round);
            let entropy = match self.config.schedule.entropy_block(
                self.config.seed,
                epoch,
                GROUP_ID,
                round_index,
                self.config.rounds_per_epoch,
            ) {
                Some(block) => block,
                None => {
                    // The span produced no finalized block, so the round is void
                    // and counts against no spool.
                    epoch_voids += 1;
                    self.void_count += 1;
                    continue;
                }
            };
            let seed = hex_hash(&entropy.hash);
            for target_index in 0..self.committee.len() {
                let target_position = GroupPosition::new(target_index);
                let outcome = run_round(
                    &self.committee,
                    &group,
                    &self.holdings,
                    &self.costs,
                    &entropy,
                    epoch,
                    round_index,
                    target_position,
                    &self.config,
                )?;
                let target_id = self.committee[target_index].id;
                let mut witnesses = 0usize;
                let mut attestations = Vec::with_capacity(self.committee.len().saturating_sub(1));
                for (observer_index, (observer, observation)) in
                    self.committee.iter().zip(outcome.observers.iter()).enumerate()
                {
                    if observer_index == target_index {
                        continue;
                    }
                    if observation.witnessed {
                        witnesses += 1;
                    }
                    attestations.push(Attestation {
                        observer: observer.id,
                        arrival_ms: observation.arrival_ms,
                        witnessed: observation.witnessed,
                        signed: observation.signed,
                    });
                }
                // Every owner knows the certificate long before the boundary acts.
                for observer in self.committee.iter() {
                    self.scoreboard
                        .record(observer.id, target_id, outcome.certificate_formed);
                }
                if outcome.certificate_formed {
                    epoch_certs += 1;
                    self.cert_count += 1;
                } else {
                    epoch_gaps += 1;
                    self.gap_count += 1;
                }
                self.timeline.push_challenge(ChallengeRecord {
                    epoch,
                    round: round_index,
                    target: target_id,
                    target_position,
                    seed: seed.clone(),
                    entropy_slot: entropy.slot,
                    deadline_ms,
                    ready_ms: outcome.ready_ms,
                    witnesses,
                    signer_count: outcome.signer_count,
                    certified: outcome.certificate_formed,
                    certificate_ms: outcome.certificate_ms,
                    attestations,
                });
                if let Some(formed_ms) = outcome.certificate_ms {
                    self.timeline.push_certificate(
                        epoch,
                        round_index,
                        target_id,
                        outcome.signer_count,
                        formed_ms,
                    );
                }
            }
        }

        if self.config.verbose {
            emit_scores(&self.scoreboard, epoch, &self.committee, &mut self.timeline);
        }

        let (evicted, joined) = run_boundary(
            self.data,
            &self.config,
            epoch,
            next_epoch,
            &self.scoreboard,
            &mut self.committee,
            &mut self.served,
            &mut self.next_node_id,
            &mut self.timeline,
            &mut self.rng,
        )?;

        self.next_epoch_index += 1;

        Ok(EpochSummary {
            epoch,
            rounds: self.config.rounds_per_epoch,
            certificates: epoch_certs,
            gaps: epoch_gaps,
            voids: epoch_voids,
            evicted,
            joined,
            committee: self.committee.iter().map(|node| node.id).collect(),
        })
    }

    /// Epochs fully completed so far.
    pub fn epochs_completed(&self) -> u64 {
        self.next_epoch_index
    }

    /// Certificates formed across the whole run so far.
    pub fn cert_count(&self) -> u64 {
        self.cert_count
    }

    /// Gaps across the whole run so far.
    pub fn gap_count(&self) -> u64 {
        self.gap_count
    }

    /// Void rounds across the whole run so far.
    pub fn void_count(&self) -> u64 {
        self.void_count
    }

    /// The proof deadline in milliseconds.
    pub fn deadline_ms(&self) -> f64 {
        self.config.schedule.proof_deadline_ms()
    }

    /// The current committee as identity and behavior pairs.
    pub fn survivors(&self) -> Vec<(NodeId, Behavior)> {
        self.committee.iter().map(|node| (node.id, node.behavior)).collect()
    }

    /// Consume the run and assemble the full multi-epoch report.
    fn into_report(self, per_epoch: Vec<EpochSummary>) -> SimulationReport {
        let final_committee = committee_refs(&self.committee, self.data);
        let outcomes = build_outcomes(&self.served, &per_epoch, &self.scoreboard);
        let deadline_ms = self.config.schedule.proof_deadline_ms();
        SimulationReport {
            config: self.config,
            costs: self.costs,
            deadline_ms,
            per_epoch,
            outcomes,
            final_committee,
            cert_count: self.cert_count,
            gap_count: self.gap_count,
            void_count: self.void_count,
            scoreboard: self.scoreboard,
            timeline: self.timeline,
        }
    }
}

/// Stream one summary line per epoch until the cap is reached or the process is
/// interrupted.
pub fn run_continuous(data: &PingData, config: &EpochConfig, cap: Option<u64>) -> Result<()> {
    let costs = Costs::measure(config.threshold, config.blob_bytes, config.group_size)?;
    let mut sim = Simulation::new(data, config, costs)?;
    print_stream_header(&sim, config, cap);

    let mut stdout = std::io::stdout();
    let mut total_evicted = 0u64;
    let mut total_joined = 0u64;
    // A cap of none runs to a huge bound so an interrupt is what ends the loop.
    let bound = cap.unwrap_or(u64::MAX);
    for _ in 0..bound {
        let summary = sim.step_epoch()?;
        total_evicted += summary.evicted.len() as u64;
        total_joined += summary.joined.len() as u64;
        if !emit_line(&mut stdout, &summary.stream_line())? {
            return Ok(());
        }
    }
    print_stream_tally(&sim, total_evicted, total_joined);
    Ok(())
}

/// Write one streamed line and flush, reporting whether the reader is still there.
fn emit_line(stdout: &mut std::io::Stdout, line: &str) -> Result<bool> {
    use std::io::Write as _;
    match writeln!(stdout, "{line}").and_then(|()| stdout.flush()) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// Print a one-line header describing the continuous run before the stream.
fn print_stream_header(sim: &Simulation, config: &EpochConfig, cap: Option<u64>) {
    let cap_label = match cap {
        Some(count) => format!("cap={count}"),
        None => "cap=none, interrupt to stop".to_string(),
    };
    let schedule = &config.schedule;
    println!(
        "whirlwind continuous run  n={} q={}  round every {} slots ({})  rounds/epoch={}  bls={}  deadline={} ({:.0} ms)  {}",
        config.group_size,
        config.threshold,
        schedule.round_interval_slots,
        human_seconds(schedule.round_interval_ms()),
        config.rounds_per_epoch,
        config.bls_mode.label(),
        schedule.proof_deadline.label(),
        sim.deadline_ms(),
        cap_label,
    );
}

/// Print a short tally when the stream stops cleanly at its cap.
fn print_stream_tally(sim: &Simulation, evicted: u64, joined: u64) {
    let certs = sim.cert_count();
    let gaps = sim.gap_count();
    let voids = sim.void_count();
    let total = certs + gaps;
    let rate = if total == 0 {
        0.0
    } else {
        certs as f64 / total as f64 * 100.0
    };
    println!(
        "stopped after {} epochs  {} certs  {} gaps  {} voids  {:.1}% overall  evicted {}  joined {}",
        sim.epochs_completed(),
        certs,
        gaps,
        voids,
        rate,
        evicted,
        joined,
    );
    for behavior in [Behavior::NearbyFetch, Behavior::Reconstruct] {
        if let Some((id, _)) = sim.survivors().iter().find(|(_, other)| *other == behavior) {
            println!(
                "  {} free-rider node {} still in committee, never voted out",
                behavior.label(),
                id.as_u64(),
            );
        }
    }
}

/// Run the cause-free eviction vote and reassign freed positions.
#[allow(clippy::too_many_arguments)]
fn run_boundary(
    data: &PingData,
    config: &EpochConfig,
    epoch: EpochNumber,
    next_epoch: EpochNumber,
    scoreboard: &Scoreboard,
    committee: &mut [Node],
    served: &mut Vec<Node>,
    next_node_id: &mut u64,
    timeline: &mut Timeline,
    rng: &mut SmallRng,
) -> Result<(Vec<Eviction>, Vec<NodeId>)> {
    let victim = designate_victim(committee);
    let mut ledger = EvictionLedger::new();
    for observer in committee.iter() {
        for target in committee.iter() {
            if observer.id == target.id {
                continue;
            }
            let score_reason = scoreboard
                .score(observer.id, target.id)
                .and_then(|score| score.eviction_fires(&config.eviction));
            if let Some(reason) = willing_to_evict(observer, target, score_reason, victim) {
                let message = eviction_message(epoch.as_u64(), GROUP_ID, target.id.as_u64());
                let signature = sign(&observer.secret_key, &message)?;
                ledger.record(target.id, observer.public_key, signature, reason);
                timeline.push_eviction_proposed(epoch, observer.id, target.id, reason);
            }
        }
    }

    let decided = ledger.finalize(config.threshold, epoch.as_u64(), GROUP_ID)?;

    let mut evicted = Vec::new();
    let mut joined = Vec::new();
    for (target_id, reason) in &decided {
        let Some(position) = committee.iter().position(|node| node.id == *target_id) else {
            continue;
        };
        let behavior = committee[position].behavior;
        evicted.push(Eviction {
            node: *target_id,
            behavior,
            reason: *reason,
        });
        timeline.push_eviction(epoch, next_epoch, *target_id, behavior, *reason);

        let server_id = sample_unused_city(data, committee, rng)?;
        let replacement = Node::spawn_honest(NodeId(*next_node_id), server_id, next_epoch)?;
        *next_node_id += 1;
        joined.push(replacement.id);
        served.push(replacement);
        let (city, country, latitude, longitude) = locate(data, server_id);
        timeline.push_join(next_epoch, replacement.id, city, country, latitude, longitude);
        committee[position] = replacement;
    }

    Ok((evicted, joined))
}

/// Build the epoch-zero committee: expand the roster, mint keypairs, and draw
/// distinct real cities from the dataset.
fn build_initial_committee(
    data: &PingData,
    config: &EpochConfig,
    rng: &mut SmallRng,
) -> Result<Vec<Node>> {
    let slots = config.roster.expand(config.group_size)?;
    let mut servers: Vec<usize> = data.servers().iter().map(|server| server.id).collect();
    if servers.len() < config.group_size {
        bail!("dataset has {} cities, need {}", servers.len(), config.group_size);
    }
    servers.shuffle(rng);
    let mut committee = Vec::with_capacity(config.group_size);
    for (index, (behavior, bloc)) in slots.into_iter().enumerate() {
        let (secret_key, public_key) = keypair()?;
        committee.push(Node {
            id: NodeId(index as u64),
            behavior,
            bloc,
            secret_key,
            public_key,
            server_id: servers[index],
            joined_epoch: EpochNumber(0),
        });
    }
    Ok(committee)
}

/// Draw a real city not already used by the current committee.
fn sample_unused_city(data: &PingData, committee: &[Node], rng: &mut SmallRng) -> Result<usize> {
    let used: HashSet<usize> = committee.iter().map(|node| node.server_id).collect();
    let mut candidates: Vec<usize> = data
        .servers()
        .iter()
        .map(|server| server.id)
        .filter(|id| !used.contains(id))
        .collect();
    if candidates.is_empty() {
        bail!("dataset exhausted, no free city for a replacement node");
    }
    candidates.sort_unstable();
    let pick = rng.gen_range(0..candidates.len());
    Ok(candidates[pick])
}

/// City, country, and coordinates for a server id, with a placeholder fallback.
fn locate(data: &PingData, server_id: usize) -> (String, String, f64, f64) {
    data.servers()
        .iter()
        .find(|server| server.id == server_id)
        .map(|server| {
            (
                server.location.clone(),
                server.country.clone(),
                server.latitude,
                server.longitude,
            )
        })
        .unwrap_or_else(|| (format!("server {server_id}"), String::new(), 0.0, 0.0))
}

/// Build node references for the current committee, resolving each real city.
fn committee_refs(committee: &[Node], data: &PingData) -> Vec<NodeRef> {
    committee
        .iter()
        .map(|node| {
            let (city, country, latitude, longitude) = locate(data, node.server_id);
            NodeRef {
                id: node.id,
                behavior: node.behavior,
                city,
                country,
                latitude,
                longitude,
            }
        })
        .collect()
}

/// Lowercase hex of a round's entropy block hash, the label for the log.
fn hex_hash(hash: &Hash) -> String {
    let mut out = String::with_capacity(hash.as_ref().len() * 2);
    for byte in hash.as_ref() {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Emit one score reading per observer and target in the current committee.
fn emit_scores(scoreboard: &Scoreboard, epoch: EpochNumber, committee: &[Node], timeline: &mut Timeline) {
    for observer in committee {
        for target in committee {
            if observer.id == target.id {
                continue;
            }
            if let Some(score) = scoreboard.score(observer.id, target.id) {
                timeline.push_score(
                    epoch,
                    observer.id,
                    target.id,
                    score.opportunities,
                    score.successes,
                );
            }
        }
    }
}

/// Assemble lifetime outcomes for every node that ever served.
fn build_outcomes(
    served: &[Node],
    per_epoch: &[EpochSummary],
    scoreboard: &Scoreboard,
) -> Vec<NodeOutcome> {
    let mut evicted_index: HashMap<NodeId, (EpochNumber, EvictionReason)> = HashMap::new();
    for summary in per_epoch {
        for eviction in &summary.evicted {
            evicted_index.insert(eviction.node, (summary.epoch, eviction.reason));
        }
    }
    served
        .iter()
        .map(|node| {
            let (evicted_epoch, reason) = match evicted_index.get(&node.id) {
                Some((epoch, reason)) => (Some(*epoch), Some(*reason)),
                None => (None, None),
            };
            NodeOutcome {
                id: node.id,
                behavior: node.behavior,
                bloc: node.bloc,
                joined_epoch: node.joined_epoch,
                evicted_epoch,
                reason,
                final_rate: scoreboard.certified_rate(node.id),
            }
        })
        .collect()
}

impl SimulationReport {
    /// Print the multi-epoch summary in the crate's plain-line voice.
    pub fn print(&self) {
        let config = &self.config;
        let schedule = &config.schedule;
        println!("whirlwind multi-epoch simulation");
        println!(
            "  group n={} q={}  epochs={} rounds/epoch={}  bls={}  blob={}",
            config.group_size,
            config.threshold,
            config.epochs,
            config.rounds_per_epoch,
            config.bls_mode.label(),
            StorageUnits(config.blob_bytes as u64).human(),
        );
        println!(
            "  measured costs  proof {:.3} verify {:.3} sign {:.3} aggregate {:.3} cert {:.3} decode {:.3} ms, helpers {}",
            self.costs.proof_gen_ms,
            self.costs.verify_ms,
            self.costs.sign_ms,
            self.costs.aggregate_ms,
            self.costs.cert_verify_ms,
            self.costs.decode_ms,
            self.costs.helper_count,
        );
        println!(
            "  round every {} slots ({})  round width {} slots  epoch spans {} of simulated time",
            schedule.round_interval_slots,
            human_seconds(schedule.round_interval_ms()),
            schedule.round_width_slots(),
            human_seconds(config.rounds_per_epoch as f64 * schedule.round_interval_ms()),
        );
        println!(
            "  slot {:.0} ms  span {} slots  proof deadline {} ({:.0} ms)  confirmation {} slots  attest window {} slots  cert gossip {} slots  block prob {:.2}",
            schedule.slot_ms,
            schedule.span_slots,
            schedule.proof_deadline.label(),
            self.deadline_ms,
            schedule.confirmation_slots,
            schedule.attestation_window_slots,
            schedule.certificate_gossip_slots,
            schedule.block_production_probability,
        );
        println!();

        println!("per epoch");
        println!("  {:>5}  {:>5}  {:>5}  {:>5}  {:>6}  boundary", "epoch", "certs", "gaps", "voids", "certpc");
        for summary in &self.per_epoch {
            let boundary = epoch_boundary_line(summary);
            println!(
                "  {:>5}  {:>5}  {:>5}  {:>5}  {:>5.0}%  {}",
                summary.epoch.as_u64(),
                summary.certificates,
                summary.gaps,
                summary.voids,
                summary.certificate_rate() * 100.0,
                boundary,
            );
        }
        println!();

        println!("nodes");
        for outcome in &self.outcomes {
            let bloc = match outcome.bloc {
                Some(bloc) => format!("bloc {bloc}"),
                None => "solo".to_string(),
            };
            let status = match outcome.evicted_epoch.zip(outcome.reason) {
                Some((epoch, reason)) => {
                    format!("evicted e{} ({})", epoch.as_u64(), reason.label())
                }
                None => "survived".to_string(),
            };
            let rate = match outcome.final_rate {
                Some(rate) => format!("{rate:.2}"),
                None => "n/a".to_string(),
            };
            println!(
                "  {:>3}  {:<20} {:<6} joined e{:<2} {:<28} rate {}",
                outcome.id.as_u64(),
                outcome.behavior.label(),
                bloc,
                outcome.joined_epoch.as_u64(),
                status,
                rate,
            );
        }
        println!();

        println!("final committee");
        for node in &self.final_committee {
            println!("  {:>3}  {:<20} {}", node.id.as_u64(), node.behavior.label(), node.city);
        }
        println!();

        self.print_headlines();
    }

    fn print_headlines(&self) {
        println!("headlines");
        for behavior in [Behavior::NearbyFetch, Behavior::Reconstruct] {
            if let Some(free_rider) = self
                .outcomes
                .iter()
                .find(|outcome| outcome.behavior == behavior)
            {
                let rate = free_rider.final_rate.unwrap_or(0.0);
                match free_rider.evicted_epoch {
                    Some(epoch) => println!(
                        "  {} free-rider node {} evicted at epoch {} (rate {:.2})",
                        behavior.label(),
                        free_rider.id.as_u64(),
                        epoch.as_u64(),
                        rate,
                    ),
                    None => println!(
                        "  {} free-rider node {} survived all {} epochs uncaught (rate {:.2}, never voted out)",
                        behavior.label(),
                        free_rider.id.as_u64(),
                        self.config.epochs,
                        rate,
                    ),
                }
            }
        }
        for behavior in [Behavior::Offline, Behavior::Selective] {
            if let Some(outcome) = self
                .outcomes
                .iter()
                .find(|outcome| outcome.behavior == behavior && outcome.evicted_epoch.is_some())
            {
                if let (Some(epoch), Some(reason)) = (outcome.evicted_epoch, outcome.reason) {
                    println!(
                        "  {} node {} evicted at epoch {} ({})",
                        behavior.label(),
                        outcome.id.as_u64(),
                        epoch.as_u64(),
                        reason.label(),
                    );
                }
            }
        }
        if let Some(free_rider) = self
            .outcomes
            .iter()
            .find(|outcome| outcome.behavior == Behavior::CollusiveFreeRider)
        {
            let certified = self.timeline.events().iter().any(|event| {
                matches!(
                    event,
                    Event::Certificate { target, .. } if *target == free_rider.id
                )
            });
            let status = match free_rider.evicted_epoch {
                Some(epoch) => format!("evicted at epoch {}", epoch.as_u64()),
                None => "still present".to_string(),
            };
            println!(
                "  colluder bloc never reached quorum: no certificate formed for the free-rider that never proved (certified={}, {})",
                certified, status,
            );
        }
        let bls_note = match self.config.bls_mode {
            BlsMode::Full => {
                "every certificate and every eviction were real bls, aggregated and verified each round"
            }
            BlsMode::Sampled => {
                "eviction bls and one sampled certificate per round were real bls; other certificates are modeled by signer count"
            }
            BlsMode::Off => {
                "eviction bls was real bls; certificates are modeled by signer count"
            }
        };
        println!();
        println!("measured vs modeled");
        println!("  crypto phase costs and the clay decode were measured once and applied as fixed ms");
        println!("  {bls_note}");
        println!("  the two-level sub-leaf commitment and every sub-leaf proof are real merkle; a free-rider proof invalidity is a modeled boolean");
        println!("  the proof deadline is a fixed slot count from block production and the one-way latency is converted to slots against it");
    }
}

/// One-line boundary summary of evictions and joins for an epoch.
fn epoch_boundary_line(summary: &EpochSummary) -> String {
    if summary.evicted.is_empty() {
        return "-".to_string();
    }
    let parts: Vec<String> = summary
        .evicted
        .iter()
        .map(|eviction| {
            format!(
                "-{} node {} ({})",
                eviction.behavior.label(),
                eviction.node.as_u64(),
                eviction.reason.label(),
            )
        })
        .collect();
    let joined: Vec<String> = summary
        .joined
        .iter()
        .map(|id| format!("+node {}", id.as_u64()))
        .collect();
    format!("{}  {}", parts.join(", "), joined.join(", "))
}

/// Human-readable duration for the report, in the largest fitting unit.
fn human_seconds(ms: f64) -> String {
    let seconds = ms / 1000.0;
    if seconds < 90.0 {
        format!("{seconds:.0} s")
    } else if seconds < 5400.0 {
        format!("{:.1} min", seconds / 60.0)
    } else {
        format!("{:.1} h", seconds / 3600.0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::OnceLock;

    use super::*;
    use crate::sim::schedule::Schedule;
    use crate::sim::track::SpoolHoldings;

    /// Load the real ping dataset once and share it across every test.
    fn ping_data() -> &'static PingData {
        static DATA: OnceLock<PingData> = OnceLock::new();
        DATA.get_or_init(|| PingData::load().expect("load ping dataset"))
    }

    /// A fixed cost model close to the measured one, so tests skip the one-time
    /// measurement and stay fast and deterministic.
    fn test_costs() -> Costs {
        Costs {
            proof_gen_ms: 0.12,
            verify_ms: 0.03,
            sign_ms: 2.0,
            aggregate_ms: 24.0,
            cert_verify_ms: 170.0,
            decode_ms: 0.6,
            helper_count: 16,
        }
    }

    /// A small but faithful multi-epoch config for tests.
    fn fast_config() -> EpochConfig {
        EpochConfig {
            epochs: 3,
            rounds_per_epoch: 24,
            blob_bytes: 60_000,
            bls_mode: BlsMode::Off,
            behavior: BehaviorConfig {
                selective_deliver_probability: 0.4,
                offline_after_round: RoundNumber::new(8),
            },
            ..EpochConfig::default()
        }
    }

    fn find_behavior(report: &SimulationReport, behavior: Behavior) -> NodeOutcome {
        report
            .outcomes
            .iter()
            .copied()
            .find(|outcome| outcome.behavior == behavior)
            .expect("behavior present in outcomes")
    }

    #[test]
    fn offline_evicted() {
        let report = simulate_epochs_with_costs(ping_data(), &fast_config(), test_costs()).expect("run epochs");

        let offline = find_behavior(&report, Behavior::Offline);
        assert!(offline.evicted_epoch.is_some());

        for behavior in [Behavior::NearbyFetch, Behavior::Reconstruct] {
            assert!(find_behavior(&report, behavior).evicted_epoch.is_none());
        }

        // Every replacement that joined at a boundary is honest.
        for outcome in &report.outcomes {
            if outcome.joined_epoch.as_u64() > 0 {
                assert_eq!(outcome.behavior, Behavior::Honest);
            }
        }
    }

    #[test]
    fn relay_beats_withholding() {
        let mut direct = fast_config();
        direct.relay = RelayPolicy::Direct;
        let caught = simulate_epochs_with_costs(ping_data(), &direct, test_costs()).expect("run epochs");
        assert!(find_behavior(&caught, Behavior::Selective).evicted_epoch.is_some());

        // One hop is enough: peers it skipped receive the proof the long way.
        let mut relayed = fast_config();
        relayed.relay = RelayPolicy::SingleHop;
        let survives = simulate_epochs_with_costs(ping_data(), &relayed, test_costs()).expect("run epochs");
        assert!(find_behavior(&survives, Behavior::Selective).evicted_epoch.is_none());
    }

    #[test]
    fn scoreboard_persists() {
        let config = fast_config();
        let report = simulate_epochs_with_costs(ping_data(), &config, test_costs()).expect("run epochs");

        // Two original honest survivors accumulate opportunities across every epoch.
        let opportunities = report
            .scoreboard
            .score(NodeId(0), NodeId(1))
            .expect("original pair scored")
            .opportunities;
        assert!(opportunities > config.rounds_per_epoch);

        // A newcomer admitted at a boundary has fewer, proving no reset.
        let newcomer = report
            .outcomes
            .iter()
            .find(|outcome| outcome.joined_epoch.as_u64() > 0)
            .expect("a replacement joined");
        let newcomer_opportunities = report
            .scoreboard
            .score(newcomer.id, NodeId(0))
            .map(|score| score.opportunities)
            .unwrap_or(0);
        assert!(newcomer_opportunities < opportunities);
    }

    #[test]
    fn no_self_scores() {
        let report = simulate_epochs_with_costs(ping_data(), &fast_config(), test_costs()).expect("run epochs");
        for (observer, target, _) in report.scoreboard.iter() {
            assert_ne!(observer, target);
        }
    }

    #[test]
    fn void_rounds() {
        // Force a heavy void rate with an unreliable span, then assert voids never
        // produce a certificate or a gap and never touch the scoreboard.
        let mut config = fast_config();
        config.schedule = Schedule {
            block_production_probability: 0.2,
            span_slots: SlotCount::new(1),
            ..config.schedule
        };
        let report = simulate_epochs_with_costs(ping_data(), &config, test_costs()).expect("run epochs");
        assert!(report.void_count > 0, "expected some void rounds");

        let mut challenged = 0u64;
        for epoch in &report.per_epoch {
            challenged += epoch.certificates + epoch.gaps;
            // Every round is either challenged for all positions or void.
            let non_void = epoch.rounds - epoch.voids;
            assert_eq!(epoch.certificates + epoch.gaps, non_void * config.group_size as u64);
        }
        assert!(challenged > 0);
    }

    #[test]
    fn real_certificate() {
        let data = ping_data();
        let costs = test_costs();
        let mut config = fast_config();
        config.bls_mode = BlsMode::Full;
        // A generous deadline so an honest target clears every observer.
        config.schedule.proof_deadline = ProofDeadline::Slots(SlotCount::new(6));

        let mut rng = SmallRng::seed_from_u64(config.seed);
        let committee = build_initial_committee(data, &config, &mut rng).expect("build committee");
        let server_ids: Vec<usize> = committee.iter().map(|node| node.server_id).collect();
        let group = Group::place(data, &server_ids).expect("place group");
        let holdings = SpoolHoldings::build(config.blob_bytes, config.group_size).expect("build holdings");
        let entropy = config
            .schedule
            .entropy_block(config.seed, EpochNumber(0), GROUP_ID, RoundNumber::new(0), config.rounds_per_epoch)
            .expect("a produced entropy block");

        let honest_position = committee
            .iter()
            .position(|node| node.behavior == Behavior::Honest && node.bloc.is_none())
            .expect("an unaligned honest position");
        let honest = run_round(
            &committee, &group, &holdings, &costs, &entropy, EpochNumber(0),
            RoundNumber::new(0), GroupPosition::new(honest_position), &config,
        )
        .expect("run the honest round");
        assert!(honest.signer_count >= config.threshold);
        assert!(honest.certificate_formed);

        // A free-rider target gathers only its bloc, below the threshold, forming
        // no certificate whatever the deadline.
        let free_position = committee
            .iter()
            .position(|node| node.behavior == Behavior::CollusiveFreeRider)
            .expect("a free-rider position");
        let free = run_round(
            &committee, &group, &holdings, &costs, &entropy, EpochNumber(0),
            RoundNumber::new(0), GroupPosition::new(free_position), &config,
        )
        .expect("run the free-rider round");
        assert!(!free.certificate_formed);
        assert!(free.signer_count < config.threshold);
    }

    #[test]
    fn free_rider_evicted() {
        let config = fast_config();
        let report = simulate_epochs_with_costs(ping_data(), &config, test_costs()).expect("run epochs");
        let free_rider = find_behavior(&report, Behavior::CollusiveFreeRider);
        assert!(free_rider.evicted_epoch.is_some());

        for event in report.timeline.events() {
            match event {
                Event::Certificate { target, .. } => assert_ne!(*target, free_rider.id),
                Event::Challenge(record) if record.target == free_rider.id => {
                    assert!(!record.certified)
                }
                _ => {}
            }
        }
    }

    #[test]
    fn bloc_cannot_evict() {
        let mut config = fast_config();
        config.roster = Roster {
            honest: 10,
            nearby_fetch: 1,
            reconstruct: 1,
            selective: 1,
            offline: 1,
            colluders: 6,
        };
        let report = simulate_epochs_with_costs(ping_data(), &config, test_costs()).expect("run epochs");

        // The victim is the lowest-position honest solo node, id 0.
        let victim = NodeId(0);
        let proposed = report
            .timeline
            .events()
            .iter()
            .filter(|event| matches!(event, Event::EvictionProposed { target, .. } if *target == victim))
            .count();
        assert!(proposed > 0, "bloc never voted against the victim");

        for event in report.timeline.events() {
            if let Event::Eviction { target, .. } = event {
                assert_ne!(*target, victim);
            }
        }
        assert!(report.final_committee.iter().any(|node| node.id == victim));
        let victim_outcome = report.outcomes.iter().find(|outcome| outcome.id == victim).expect("the victim outcome");
        assert!(victim_outcome.evicted_epoch.is_none());
    }

    #[test]
    fn committee_size() {
        let config = fast_config();
        let report = simulate_epochs_with_costs(ping_data(), &config, test_costs()).expect("run epochs");
        for summary in &report.per_epoch {
            assert_eq!(summary.committee.len(), config.group_size);
            let mut seen = HashSet::new();
            for id in &summary.committee {
                assert!(seen.insert(*id), "duplicate id in committee");
            }
        }
    }

    #[test]
    fn deterministic_run() {
        let first = simulate_epochs_with_costs(ping_data(), &fast_config(), test_costs()).expect("run epochs");
        let second = simulate_epochs_with_costs(ping_data(), &fast_config(), test_costs()).expect("run epochs");
        assert_eq!(first.per_epoch.len(), second.per_epoch.len());
        for (left, right) in first.per_epoch.iter().zip(second.per_epoch.iter()) {
            assert_eq!(left.certificates, right.certificates);
            assert_eq!(left.gaps, right.gaps);
            assert_eq!(left.voids, right.voids);
            assert_eq!(left.committee, right.committee);
            assert_eq!(left.joined, right.joined);
            let left_evicted: Vec<(NodeId, EvictionReason)> =
                left.evicted.iter().map(|eviction| (eviction.node, eviction.reason)).collect();
            let right_evicted: Vec<(NodeId, EvictionReason)> =
                right.evicted.iter().map(|eviction| (eviction.node, eviction.reason)).collect();
            assert_eq!(left_evicted, right_evicted);
        }
        let left_json = serde_json::to_string(&first.timeline.events()).expect("serialize timeline");
        let right_json = serde_json::to_string(&second.timeline.events()).expect("serialize timeline");
        assert_eq!(left_json, right_json);
    }

    #[test]
    fn parameter_relations() {
        // The default n=20 q=14 satisfies every relation with Clay k=7 d=16.
        assert!(fast_config().validate().is_ok());

        // q=10 pins f=10, so k+f=17 exceeds q and the certificate margin is gone.
        let mut low = fast_config();
        low.threshold = 10;
        assert!(low.validate().is_err());

        // q above n is rejected outright.
        let mut high = fast_config();
        high.threshold = 21;
        assert!(high.validate().is_err());

        // The engine refuses to construct on a violating config.
        assert!(Simulation::new(ping_data(), &low, test_costs()).is_err());
    }
}
