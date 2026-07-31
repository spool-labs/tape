//! whirlwind: measures the Whirlwind storage challenge mechanism from the
//! TAPEDRIVE draft (section 8) against the implemented slicer, merkle, and BLS code.
//!
//! The harness runs the honest round end to end and measures its real cost, then
//! runs the same round against adversaries and against the threshold edge cases.
//! Where the mechanism holds, the numbers show it; where a claim fails, the
//! failure is attributable to the spec.

use anyhow::Result;
use clap::{Parser, Subcommand};

use whirlwind::probes::commitment::CommitmentReport;
use whirlwind::probes::detection::DetectionReport;
use whirlwind::probes::grinding::GrindingReport;
use whirlwind::probes::lifecycle::LifecycleReport;
use whirlwind::probes::quorum::QuorumReport;
use whirlwind::probes::reconstruct::ReconstructReport;
use whirlwind::report;
use whirlwind::spool::{Spool, LEAF_COUNT};

const DEFAULT_RTTS_MS: [f64; 4] = [1.0, 10.0, 50.0, 100.0];
const DEFAULT_NET_MBPS: f64 = 1_000.0;
const DEFAULT_DISK_MBPS: f64 = 5_000.0;

#[derive(Parser)]
#[command(name = "whirlwind", about = "Measure the Whirlwind storage challenge mechanism")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Probe what the registered commitment commits to and the response readings.
    Commitment {
        #[arg(long, default_value_t = 4_000_000)]
        blob_bytes: usize,
    },
    /// Measure the cost of faking one sampled unit via real Clay repair.
    Reconstruct {
        #[arg(long, default_value_t = 1_000_000)]
        blob_bytes: usize,
        #[arg(long, default_value_t = 1_024)]
        sub_leaf_bytes: usize,
        #[arg(long, default_value_t = DEFAULT_NET_MBPS)]
        net_mbps: f64,
        #[arg(long, default_value_t = DEFAULT_DISK_MBPS)]
        disk_mbps: f64,
    },
    /// Simulate detection of a silently discarded fraction.
    Detection {
        #[arg(long, default_value_t = 0.01)]
        discard_fraction: f64,
        #[arg(long, default_value_t = 60)]
        cadence_secs: u64,
        #[arg(long, default_value_t = 100_000)]
        trials: u64,
    },
    /// Simulate entropy-block grinding by a colluding producer.
    Grinding {
        #[arg(long, default_value_t = 0.9)]
        discard_fraction: f64,
        #[arg(long, default_value_t = 100_000)]
        trials: u64,
    },
    /// Analyze q-signature collection under one slow honest peer.
    Quorum {
        #[arg(long, default_value_t = LEAF_COUNT)]
        group_size: usize,
        #[arg(long, default_value_t = 6)]
        byzantine: usize,
    },
    /// Measure the honest round's real per-phase compute cost.
    Lifecycle {
        #[arg(long, default_value_t = 1_000_000)]
        blob_bytes: usize,
        #[arg(long, default_value_t = 14)]
        threshold: usize,
        #[arg(long, default_value_t = 101)]
        iterations: usize,
    },
    /// Run every probe with default parameters.
    All,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Commitment { blob_bytes } => run_commitment(blob_bytes),
        Command::Reconstruct {
            blob_bytes,
            sub_leaf_bytes,
            net_mbps,
            disk_mbps,
        } => run_reconstruct(blob_bytes, sub_leaf_bytes, net_mbps, disk_mbps),
        Command::Detection {
            discard_fraction,
            cadence_secs,
            trials,
        } => {
            report::print_detection(&DetectionReport::simulate(discard_fraction, cadence_secs, trials));
            Ok(())
        }
        Command::Grinding {
            discard_fraction,
            trials,
        } => {
            report::print_grinding(&GrindingReport::simulate(discard_fraction, trials));
            Ok(())
        }
        Command::Quorum {
            group_size,
            byzantine,
        } => {
            report::print_quorum(&QuorumReport::analyze(group_size, byzantine));
            Ok(())
        }
        Command::Lifecycle {
            blob_bytes,
            threshold,
            iterations,
        } => run_lifecycle(blob_bytes, threshold, iterations),
        Command::All => run_all(),
    }
}

fn run_lifecycle(blob_bytes: usize, threshold: usize, iterations: usize) -> Result<()> {
    let spool = Spool::build(blob_bytes)?;
    report::print_spool(&spool);
    report::print_lifecycle(&LifecycleReport::measure(&spool, threshold, iterations)?);
    Ok(())
}

fn run_commitment(blob_bytes: usize) -> Result<()> {
    let spool = Spool::build(blob_bytes)?;
    report::print_spool(&spool);
    report::print_commitment(&CommitmentReport::measure(&spool)?);
    Ok(())
}

fn run_reconstruct(blob_bytes: usize, sub_leaf_bytes: usize, net_mbps: f64, disk_mbps: f64) -> Result<()> {
    let spool = Spool::build(blob_bytes)?;
    report::print_spool(&spool);
    let reconstruct =
        ReconstructReport::measure(&spool, sub_leaf_bytes, &DEFAULT_RTTS_MS, net_mbps, disk_mbps)?;
    report::print_reconstruct(&reconstruct);
    Ok(())
}

fn run_all() -> Result<()> {
    run_lifecycle(1_000_000, 14, 101)?;
    run_commitment(4_000_000)?;
    run_reconstruct(1_000_000, 1_024, DEFAULT_NET_MBPS, DEFAULT_DISK_MBPS)?;
    report::print_detection(&DetectionReport::simulate(0.01, 60, 100_000));
    report::print_grinding(&GrindingReport::simulate(0.9, 100_000));
    report::print_quorum(&QuorumReport::analyze(LEAF_COUNT, 6));
    Ok(())
}
