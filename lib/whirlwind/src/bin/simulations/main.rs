//! Whirlwind simulations: the latency histogram and the multi-epoch engine.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

use whirlwind::network::simulated::ping_data::PingData;
use whirlwind::sim::epoch::{
    run_continuous, simulate_epochs, simulate_epochs_with_costs, Behavior, BehaviorConfig, BlsMode,
    EpochConfig, Roster,
};
use whirlwind::sim::export;
use whirlwind::sim::latency::{simulate, Costs};
use whirlwind::sim::plot;
use whirlwind::sim::project::{Preset, Projection, RpcModel, ScaleParams};
use whirlwind::sim::schedule::{cadence_for_epoch, ProofDeadline, Schedule, MAINNET_CADENCE_SLOTS};
use whirlwind::sim::round::RelayPolicy;
use whirlwind::sim::scoreboard::EvictionRule;
use whirlwind::spool::{AGREEMENT_THRESHOLD, LEAF_COUNT};
use whirlwind::types::{RoundNumber, SlotCount};

/// Default output path for the animation export, under the crate's viz directory.
const DEFAULT_EXPORT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/viz/whirlwind-run.json");

#[derive(Parser)]
#[command(name = "whirlwind-sim", about = "Whirlwind latency histogram and epoch engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Render the per-position latency histogram from real ping data.
    Histogram(HistogramArgs),
    /// Run the continuous multi-epoch challenge and eviction simulation.
    Epoch(EpochArgs),
    /// Run a multi-epoch simulation and export its event log as JSON.
    Export(ExportArgs),
    /// Project the measured per-round behavior to a full mainnet epoch.
    Project(ProjectArgs),
    /// Sweep the proof deadline and report who certifies and who is evicted.
    Sweep(SweepArgs),
}

/// The engine knobs every epoch-driving command shares: group shape, slot
/// schedule timing, adversary tuning, and the base seed.
#[derive(Parser)]
struct EngineArgs {
    /// Spool owners in the group, one committee position each.
    #[arg(long, default_value_t = LEAF_COUNT)]
    group_size: usize,
    /// Attestation and eviction quorum.
    #[arg(long, default_value_t = AGREEMENT_THRESHOLD)]
    threshold: usize,
    /// Solana slot time in milliseconds.
    #[arg(long, default_value_t = 400)]
    slot_ms: u64,
    /// Slots between round starts, derived from the epoch length when unset
    #[arg(long)]
    round_interval_slots: Option<u64>,
    /// Slots in each round's entropy span.
    #[arg(long, default_value_t = 4)]
    span_slots: u64,
    /// Slots after production at which signing opens on confirmation.
    #[arg(long, default_value_t = 1)]
    confirmation_slots: u64,
    /// Slots the signature period stays open after confirmation.
    #[arg(long, default_value_t = 4)]
    attestation_window_slots: u64,
    /// Slots for an aggregated certificate to gossip to the group.
    #[arg(long, default_value_t = 2)]
    certificate_gossip_slots: u64,
    /// Probability a scheduled slot produces a finalized block.
    #[arg(long, default_value_t = 0.95)]
    block_production_probability: f64,
    /// Round trip to a nearby metro edge for a fetch free-rider.
    #[arg(long, default_value_t = 20.0)]
    edge_rtt_ms: f64,
    /// How far a proof travels after the challenged owner sends it.
    #[arg(long, value_enum, default_value_t = RelayArg::Flood)]
    relay: RelayArg,
    /// Probability a selective prover delivers to a given observer.
    #[arg(long, default_value_t = 0.4)]
    selective_deliver: f64,
    /// Members of the colluding bloc, the rest of the roster filling around it.
    #[arg(long, default_value_t = 2)]
    colluders: usize,
    /// Base seed for placement, behavior, and block production draws.
    #[arg(long, default_value_t = 1)]
    seed: u64,
}

impl EngineArgs {
    /// The slot schedule at the given proof deadline
    fn schedule(&self, proof_deadline: ProofDeadline, epoch_slots: Option<u64>) -> Schedule {
        let mut schedule = Schedule {
            slot_ms: self.slot_ms as f64,
            round_interval_slots: SlotCount::new(MAINNET_CADENCE_SLOTS),
            span_slots: SlotCount::new(self.span_slots),
            proof_deadline,
            confirmation_slots: SlotCount::new(self.confirmation_slots),
            attestation_window_slots: SlotCount::new(self.attestation_window_slots),
            certificate_gossip_slots: SlotCount::new(self.certificate_gossip_slots),
            block_production_probability: self.block_production_probability,
        };
        let interval = match (self.round_interval_slots, epoch_slots) {
            (Some(explicit), _) => explicit,
            (None, Some(slots)) => cadence_for_epoch(slots, schedule.round_width_slots()),
            (None, None) => MAINNET_CADENCE_SLOTS,
        };
        schedule.round_interval_slots = SlotCount::new(interval);
        schedule
    }

    /// The full engine config for one command's run shape
    fn config(&self, shape: RunShape) -> EpochConfig {
        EpochConfig {
            epochs: shape.epochs,
            rounds_per_epoch: shape.rounds_per_epoch,
            group_size: self.group_size,
            threshold: self.threshold,
            blob_bytes: shape.blob_bytes,
            schedule: self.schedule(shape.proof_deadline, shape.epoch_slots),
            edge_rtt_ms: self.edge_rtt_ms,
            relay: RelayPolicy::from(self.relay),
            bls_mode: shape.bls_mode,
            seed: self.seed,
            roster: Roster::with_colluders(self.group_size, self.colluders),
            behavior: BehaviorConfig {
                selective_deliver_probability: self.selective_deliver,
                offline_after_round: RoundNumber::new(shape.rounds_per_epoch / 3),
            },
            eviction: EvictionRule::default(),
            verbose: shape.verbose,
        }
    }
}

/// What varies per command around the shared engine knobs
struct RunShape {
    proof_deadline: ProofDeadline,
    epochs: u64,
    rounds_per_epoch: u64,
    blob_bytes: usize,
    bls_mode: BlsMode,
    verbose: bool,
    epoch_slots: Option<u64>,
}

#[derive(Parser)]
struct HistogramArgs {
    /// Spool owners in the group, one committee position each.
    #[arg(long, default_value_t = LEAF_COUNT)]
    group_size: usize,
    /// Attestation quorum, marked on the chart.
    #[arg(long, default_value_t = AGREEMENT_THRESHOLD)]
    threshold: usize,
    /// Random placements averaged into the curve.
    #[arg(long, default_value_t = 500)]
    rounds: usize,
    /// Solana slot time, which fixes where the schedulable deadlines are drawn.
    #[arg(long, default_value_t = 400)]
    slot_ms: u64,
    /// Payload the measured proof and decode costs are taken against.
    #[arg(long, default_value_t = 1_000_000)]
    blob_bytes: usize,
    /// Round trip to a nearby metro edge for the fetch free-rider curve.
    #[arg(long, default_value_t = 20.0)]
    edge_rtt_ms: f64,
    /// How far a proof travels after the challenged owner sends it.
    #[arg(long, value_enum, default_value_t = RelayArg::Flood)]
    relay: RelayArg,
    /// Path the rendered histogram is written to.
    #[arg(long, default_value = "whirlwind-latency.png")]
    out: String,
}

#[derive(Parser)]
struct EpochArgs {
    /// Stream one line per epoch and run until interrupted. Implied by epochs 0.
    #[arg(long)]
    continuous: bool,
    /// Epochs to run back to back.
    #[arg(long, default_value_t = 12)]
    epochs: u64,
    /// Rounds simulated per epoch. The run is a sample of a real-cadence
    /// timeline, not a full mainnet epoch, which would be 10,080 rounds.
    #[arg(long, default_value_t = 48)]
    rounds_per_epoch: u64,
    /// Payload size of the largest coded track in the shared holdings.
    #[arg(long, default_value_t = 1_000_000)]
    blob_bytes: usize,
    /// Slots after the entropy block by which a proof must arrive.
    #[arg(long, default_value_t = 2)]
    proof_deadline_slots: u64,
    /// How much real BLS to run for certificates.
    #[arg(long, value_enum, default_value_t = BlsModeArg::Full)]
    bls_mode: BlsModeArg,
    /// Shared engine knobs.
    #[command(flatten)]
    engine: EngineArgs,
}

/// Export defaults to a shorter run than the full report so the JSON stays a
/// friendly size for a browser animation while still crossing several epoch
/// boundaries. Certificate BLS defaults off because the animation reads the
/// modeled formation timing, not real aggregate bytes; eviction BLS stays real.
#[derive(Parser)]
struct ExportArgs {
    /// Epochs to run back to back.
    #[arg(long, default_value_t = 4)]
    epochs: u64,
    /// Rounds simulated per epoch.
    #[arg(long, default_value_t = 12)]
    rounds_per_epoch: u64,
    /// Payload size of the largest coded track in the shared holdings.
    #[arg(long, default_value_t = 256_000)]
    blob_bytes: usize,
    /// Slots after the entropy block by which a proof must arrive.
    #[arg(long, default_value_t = 2)]
    proof_deadline_slots: u64,
    /// How much real BLS to run for certificates.
    #[arg(long, value_enum, default_value_t = BlsModeArg::Off)]
    bls_mode: BlsModeArg,
    /// Path the run document is written to.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Shared engine knobs.
    #[command(flatten)]
    engine: EngineArgs,
}

/// The projection runs a short one-group measurement then scales it. The
/// measurement knobs size that run; the scale knobs size the network the
/// measured per-round behavior is extrapolated onto.
#[derive(Parser)]
struct ProjectArgs {
    /// Epoch length preset, overridden by explicit epoch-seconds.
    #[arg(long, value_enum, default_value_t = PresetArg::Mainnet)]
    preset: PresetArg,
    /// Explicit epoch duration in seconds, overriding the preset.
    #[arg(long)]
    epoch_seconds: Option<u64>,
    /// Number of spool groups in the network, defaulting to the preset's genesis count.
    #[arg(long)]
    spool_groups: Option<u64>,
    /// Which rpc assumption to headline in the report.
    #[arg(long, value_enum, default_value_t = RpcModelArg::Both)]
    rpc_model: RpcModelArg,
    /// Epochs to run in the representative measurement.
    #[arg(long, default_value_t = 6)]
    measure_epochs: u64,
    /// Rounds per epoch in the representative measurement.
    #[arg(long, default_value_t = 24)]
    measure_rounds_per_epoch: u64,
    /// Payload bytes fixing the measured proof size.
    #[arg(long, default_value_t = 1_000_000)]
    blob_bytes: usize,
    /// Slots after the entropy block by which a proof must arrive.
    #[arg(long, default_value_t = 2)]
    proof_deadline_slots: u64,
    /// Shared engine knobs.
    #[command(flatten)]
    engine: EngineArgs,
}

/// The sweep re-runs the engine at several proof deadlines against one measured
/// cost model, regenerating the timing headline as an artifact: which behaviors
/// certify and survive at whole-slot deadlines, and what a sub-slot clock would
/// catch. Sub-slot values are counterfactual; the real slot clock cannot
/// schedule them.
#[derive(Parser)]
struct SweepArgs {
    /// Epochs to run at each deadline.
    #[arg(long, default_value_t = 3)]
    epochs: u64,
    /// Rounds simulated per epoch.
    #[arg(long, default_value_t = 24)]
    rounds_per_epoch: u64,
    /// Payload size of the largest coded track in the shared holdings.
    #[arg(long, default_value_t = 1_000_000)]
    blob_bytes: usize,
    /// Counterfactual sub-slot deadlines in milliseconds after production.
    #[arg(long, value_delimiter = ',', default_values_t = vec![100.0, 200.0, 300.0])]
    deadline_ms: Vec<f64>,
    /// Whole-slot deadlines the real clock can schedule.
    #[arg(long, value_delimiter = ',', default_values_t = vec![1, 2, 3])]
    deadline_slots: Vec<u64>,
    /// How much real BLS to run for certificates.
    #[arg(long, value_enum, default_value_t = BlsModeArg::Off)]
    bls_mode: BlsModeArg,
    /// Shared engine knobs.
    #[command(flatten)]
    engine: EngineArgs,
}

#[derive(Clone, Copy, ValueEnum)]
enum PresetArg {
    Mainnet,
    Devnet,
    Localnet,
    Simnet,
}

impl From<PresetArg> for Preset {
    fn from(value: PresetArg) -> Self {
        match value {
            PresetArg::Mainnet => Preset::Mainnet,
            PresetArg::Devnet => Preset::Devnet,
            PresetArg::Localnet => Preset::Localnet,
            PresetArg::Simnet => Preset::Simnet,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum RpcModelArg {
    OwnIngestor,
    ExternalRpc,
    Both,
}

impl From<RpcModelArg> for RpcModel {
    fn from(value: RpcModelArg) -> Self {
        match value {
            RpcModelArg::OwnIngestor => RpcModel::OwnIngestor,
            RpcModelArg::ExternalRpc => RpcModel::ExternalRpc,
            RpcModelArg::Both => RpcModel::Both,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum RelayArg {
    Direct,
    SingleHop,
    Flood,
}

impl From<RelayArg> for RelayPolicy {
    fn from(value: RelayArg) -> Self {
        match value {
            RelayArg::Direct => RelayPolicy::Direct,
            RelayArg::SingleHop => RelayPolicy::SingleHop,
            RelayArg::Flood => RelayPolicy::Flood,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum BlsModeArg {
    Full,
    Sampled,
    Off,
}

impl From<BlsModeArg> for BlsMode {
    fn from(value: BlsModeArg) -> Self {
        match value {
            BlsModeArg::Full => BlsMode::Full,
            BlsModeArg::Sampled => BlsMode::Sampled,
            BlsModeArg::Off => BlsMode::Off,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Histogram(args) => run_histogram(args),
        Command::Epoch(args) => run_epoch(args),
        Command::Export(args) => run_export(args),
        Command::Project(args) => run_project(args),
        Command::Sweep(args) => run_sweep(args),
    }
}

/// Resolve the epoch length from an optional explicit second count over a preset.
fn resolve_epoch_seconds(preset: PresetArg, explicit: Option<u64>) -> u64 {
    explicit.unwrap_or_else(|| Preset::from(preset).epoch_seconds())
}

/// Resolve the group count the same way, so a preset projects onto its own
/// network rather than a mainnet-at-full-width one.
fn resolve_spool_groups(preset: PresetArg, explicit: Option<u64>) -> u64 {
    explicit.unwrap_or_else(|| Preset::from(preset).spool_groups())
}

fn run_histogram(args: HistogramArgs) -> Result<()> {
    println!("measuring crypto costs on this machine");
    let costs = Costs::measure(args.threshold, args.blob_bytes, args.group_size)?;
    println!(
        "  proof {:.4} verify {:.4} sign {:.4} aggregate {:.4} cert {:.4} decode {:.4} ms, helpers {}",
        costs.proof_gen_ms,
        costs.verify_ms,
        costs.sign_ms,
        costs.aggregate_ms,
        costs.cert_verify_ms,
        costs.decode_ms,
        costs.helper_count
    );

    println!("loading ping dataset");
    let data = PingData::load()?;
    println!("  {} servers", data.servers().len());

    println!("simulating {} placements", args.rounds);
    let curve = simulate(
        &data,
        &costs,
        args.group_size,
        args.threshold,
        args.rounds,
        args.edge_rtt_ms,
        RelayPolicy::from(args.relay),
    );
    let honest = curve.honest_total_ms();
    let mid = curve.group_size / 2;
    println!(
        "  at 50% of group: honest {:.1} ms, reconstruct free-rider {:.1} ms, nearby free-rider {:.1} ms",
        honest[mid], curve.freerider_total_ms[mid], curve.nearby_total_ms[mid]
    );

    plot::render(&curve, &args.out, args.slot_ms as f64)?;
    println!("wrote {}", args.out);
    Ok(())
}

fn run_epoch(args: EpochArgs) -> Result<()> {
    let config = args.engine.config(RunShape {
        proof_deadline: ProofDeadline::Slots(SlotCount::new(args.proof_deadline_slots)),
        epochs: args.epochs,
        rounds_per_epoch: args.rounds_per_epoch,
        blob_bytes: args.blob_bytes,
        bls_mode: BlsMode::from(args.bls_mode),
        verbose: false,
        epoch_slots: None,
    });

    let continuous = args.continuous || args.epochs == 0;
    print_signature_estimate(&config, (!continuous).then_some(config.epochs));

    println!("loading ping dataset");
    let data = PingData::load()?;
    println!("  {} servers", data.servers().len());
    println!();

    if continuous {
        // Epochs zero means run forever; a positive count is a clean stopping cap.
        let cap = (args.epochs != 0).then_some(args.epochs);
        run_continuous(&data, &config, cap)?;
    } else {
        let report = simulate_epochs(&data, &config)?;
        report.print();
    }
    Ok(())
}

fn run_export(args: ExportArgs) -> Result<()> {
    let config = args.engine.config(RunShape {
        proof_deadline: ProofDeadline::Slots(SlotCount::new(args.proof_deadline_slots)),
        epochs: args.epochs,
        rounds_per_epoch: args.rounds_per_epoch,
        blob_bytes: args.blob_bytes,
        bls_mode: BlsMode::from(args.bls_mode),
        verbose: true,
        epoch_slots: None,
    });

    print_signature_estimate(&config, Some(config.epochs));

    println!("loading ping dataset");
    let data = PingData::load()?;
    println!("  {} servers", data.servers().len());
    println!();

    let report = simulate_epochs(&data, &config)?;
    let out = args
        .out
        .unwrap_or_else(|| PathBuf::from(DEFAULT_EXPORT_PATH));
    export::write_run_json(&report, &out)?;
    println!(
        "wrote {} ({} events, {} certificates, {} gaps)",
        out.display(),
        report.timeline.len(),
        report.cert_count,
        report.gap_count,
    );
    Ok(())
}

fn run_project(args: ProjectArgs) -> Result<()> {
    let epoch_seconds = resolve_epoch_seconds(args.preset, args.epoch_seconds);
    let spool_groups = resolve_spool_groups(args.preset, args.spool_groups);
    let epoch_slots = epoch_seconds.saturating_mul(1_000) / args.engine.slot_ms.max(1);
    // Certificate bytes come from the real signature type, so a live aggregate
    // would only cost wall clock here.
    let measure = args.engine.config(RunShape {
        proof_deadline: ProofDeadline::Slots(SlotCount::new(args.proof_deadline_slots)),
        epochs: args.measure_epochs,
        rounds_per_epoch: args.measure_rounds_per_epoch,
        blob_bytes: args.blob_bytes,
        bls_mode: BlsMode::Off,
        verbose: false,
        epoch_slots: Some(epoch_slots),
    });
    let interval = measure.schedule.round_interval_slots.as_u64();
    let scale = ScaleParams {
        preset: args.epoch_seconds.is_none().then(|| Preset::from(args.preset)),
        epoch_seconds,
        slot_ms: args.engine.slot_ms,
        round_interval_slots: interval,
        spool_groups,
        groups_from_preset: args.spool_groups.is_none(),
        rpc_model: RpcModel::from(args.rpc_model),
    };
    println!(
        "measuring one group over {} epochs x {} rounds, then scaling to {} groups",
        args.measure_epochs, args.measure_rounds_per_epoch, spool_groups,
    );
    println!("loading ping dataset");
    let data = PingData::load()?;
    println!("  {} servers", data.servers().len());
    println!();

    let projection = Projection::compute(&data, &measure, &scale)?;
    projection.print();
    Ok(())
}

fn run_sweep(args: SweepArgs) -> Result<()> {
    println!("measuring crypto costs on this machine");
    let costs = Costs::measure(args.engine.threshold, args.blob_bytes, args.engine.group_size)?;
    println!("loading ping dataset");
    let data = PingData::load()?;
    println!("  {} servers", data.servers().len());
    println!();

    let mut deadlines: Vec<ProofDeadline> = args
        .deadline_ms
        .iter()
        .map(|ms| ProofDeadline::CounterfactualMs(*ms))
        .collect();
    deadlines.extend(
        args.deadline_slots
            .iter()
            .map(|slots| ProofDeadline::Slots(SlotCount::new(*slots))),
    );

    println!(
        "deadline sweep  n={} q={}  {} epochs x {} rounds  bls={}",
        args.engine.group_size,
        args.engine.threshold,
        args.epochs,
        args.rounds_per_epoch,
        BlsMode::from(args.bls_mode).label(),
    );
    println!(
        "  {:<20}  {:>6}  {:>12}  {:>12}  {:>11}  {:>11}  {:>11}  {:>9}",
        "deadline", "certs", "nearby-fetch", "reconstruct", "selective", "offline", "colluder", "honest-ev",
    );

    for deadline in deadlines {
        let config = args.engine.config(RunShape {
            proof_deadline: deadline,
            epochs: args.epochs,
            rounds_per_epoch: args.rounds_per_epoch,
            blob_bytes: args.blob_bytes,
            bls_mode: BlsMode::from(args.bls_mode),
            verbose: false,
            epoch_slots: None,
        });
        let report = simulate_epochs_with_costs(&data, &config, costs)?;

        let cell = |behavior: Behavior| -> String {
            report
                .outcomes
                .iter()
                .find(|outcome| outcome.behavior == behavior)
                .map(|outcome| match outcome.evicted_epoch {
                    Some(epoch) => format!("evicted e{}", epoch.as_u64()),
                    None => "survives".to_string(),
                })
                .unwrap_or_else(|| "-".to_string())
        };
        let honest_evicted = report
            .outcomes
            .iter()
            .filter(|outcome| outcome.behavior == Behavior::Honest && outcome.evicted_epoch.is_some())
            .count();
        let challenged = report.cert_count + report.gap_count;
        let rate = if challenged == 0 {
            0.0
        } else {
            report.cert_count as f64 / challenged as f64 * 100.0
        };
        println!(
            "  {:<20}  {:>5.1}%  {:>12}  {:>12}  {:>11}  {:>11}  {:>11}  {:>9}",
            deadline.label(),
            rate,
            cell(Behavior::NearbyFetch),
            cell(Behavior::Reconstruct),
            cell(Behavior::Selective),
            cell(Behavior::Offline),
            cell(Behavior::CollusiveFreeRider),
            honest_evicted,
        );
    }
    println!();
    println!("sub-slot rows are counterfactual: the real slot clock cannot schedule them");
    Ok(())
}

/// Certifying targets per round for the chosen BLS mode.
fn certifying_targets(config: &EpochConfig) -> f64 {
    match config.bls_mode {
        BlsMode::Full => config.group_size as f64,
        BlsMode::Sampled => 1.0,
        BlsMode::Off => 0.0,
    }
}

/// Print the certificate BLS wall-clock estimate before a run: the total over a
/// bounded run, or per epoch for a continuous one whose length is unbounded.
fn print_signature_estimate(config: &EpochConfig, epochs: Option<u64>) {
    let per_epoch =
        config.threshold as f64 * certifying_targets(config) * config.rounds_per_epoch as f64;
    match epochs {
        Some(epochs) => {
            println!(
                "epoch run: {} epochs x {} rounds, n={} q={}, bls={}",
                epochs,
                config.rounds_per_epoch,
                config.group_size,
                config.threshold,
                config.bls_mode.label(),
            );
            println!(
                "  estimated certificate signatures: {:.0} (times measured sign cost for wall-clock)",
                per_epoch * epochs as f64,
            );
        }
        None => {
            println!(
                "continuous epoch run: {} rounds/epoch, n={} q={}, bls={}",
                config.rounds_per_epoch,
                config.group_size,
                config.threshold,
                config.bls_mode.label(),
            );
            println!("  estimated certificate signatures per epoch: {per_epoch:.0}");
        }
    }
}
