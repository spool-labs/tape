//! JSON export of one continuous multi-epoch run.
//!
//! The document wraps run-wide metadata, every node's lifetime, and the ordered
//! event log the engine already produced. Metadata carries the parameters and the
//! once-measured crypto costs so a viewer can interpret every millisecond; the
//! node list gives each identity's join, eviction, and final certified rate; the
//! events carry the continuous timeline of epochs, challenge rounds with per-peer
//! attestation arrivals, certificates, evictions, joins, and score readings.

use std::fs::{create_dir_all, File};
use std::io::BufWriter;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::sim::epoch::{NodeOutcome, SimulationReport};
use crate::sim::latency::Costs;
use crate::sim::scoreboard::EvictionRule;
use crate::sim::timeline::Event;

/// The full run document written to disk.
#[derive(Serialize)]
struct RunDocument<'a> {
    meta: Meta<'a>,
    nodes: &'a [NodeOutcome],
    events: &'a [Event],
}

/// Run-wide parameters and once-measured costs, enough to read the event log.
#[derive(Serialize)]
struct Meta<'a> {
    group_size: usize,
    threshold: usize,
    epochs: u64,
    rounds_per_epoch: u64,
    blob_bytes: usize,
    deadline_ms: f64,
    slot_ms: f64,
    round_interval_slots: u64,
    proof_deadline: String,
    confirmation_slots: u64,
    edge_rtt_ms: f64,
    bls_mode: &'a str,
    seed: u64,
    certificates: u64,
    gaps: u64,
    eviction_rule: EvictionRule,
    costs: &'a Costs,
}

/// Write the run as one pretty JSON document, creating the parent directory when
/// it does not yet exist.
pub fn write_run_json(report: &SimulationReport, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
    }
    let document = RunDocument {
        meta: Meta {
            group_size: report.config.group_size,
            threshold: report.config.threshold,
            epochs: report.config.epochs,
            rounds_per_epoch: report.config.rounds_per_epoch,
            blob_bytes: report.config.blob_bytes,
            deadline_ms: report.deadline_ms,
            slot_ms: report.config.schedule.slot_ms,
            round_interval_slots: report.config.schedule.round_interval_slots.as_u64(),
            proof_deadline: report.config.schedule.proof_deadline.label(),
            confirmation_slots: report.config.schedule.confirmation_slots.as_u64(),
            edge_rtt_ms: report.config.edge_rtt_ms,
            bls_mode: report.config.bls_mode.label(),
            seed: report.config.seed,
            certificates: report.cert_count,
            gaps: report.gap_count,
            eviction_rule: report.config.eviction,
            costs: &report.costs,
        },
        nodes: &report.outcomes,
        events: report.timeline.events(),
    };
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &document).context("write run json")?;
    Ok(())
}
