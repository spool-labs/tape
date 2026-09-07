//! Mainnet-scale projection from a short measured run.
//!
//! The continuous engine models one spool group over a few epochs. This module
//! runs that engine as a representative sample, reads back the per-round
//! certificate rate, the eviction rate, and the per-round attestation counts,
//! then extrapolates those measured quantities to a full mainnet epoch across
//! every spool group. Challenges are off chain, so they scale as a signal count
//! and add no transactions; only evictions that land and the epoch-lifecycle
//! transactions touch the chain. Byte sizes come from the real proof, signature,
//! and certificate representations, with every assumption named.

use anyhow::Result;
use tape_core::bls::BlsSignature;
use tape_core::types::StorageUnits;

use crate::network::simulated::ping_data::PingData;
use crate::probes::commitment::CommitmentReport;
use crate::sim::epoch::{simulate_epochs_with_costs, EpochConfig};
use crate::sim::latency::Costs;
use crate::sim::schedule::TARGET_ROUNDS_PER_EPOCH;
use crate::sim::timeline::Event;
use crate::sim::track::SpoolHoldings;
use crate::spool::Spool;

/// Global epoch-lifecycle transactions per epoch: advance, commit, assignment.
const GLOBAL_LIFECYCLE_TXNS: u64 = 3;

/// Header on one wire attestation beside its signature: signer index and round
/// tag. Assumed minimal since the round coordinates are shared context.
const ATTESTATION_HEADER_BYTES: usize = 8;

/// A canonical epoch length, either a named preset or an explicit second count.
#[derive(Clone, Copy, Debug)]
pub enum Preset {
    Mainnet,
    Devnet,
    Localnet,
    Simnet,
}

impl Preset {
    /// Epoch duration in seconds for this preset, from the genesis config.
    pub fn epoch_seconds(&self) -> u64 {
        match self {
            Preset::Mainnet => 604_800,
            Preset::Devnet => 3_600,
            Preset::Localnet => 100,
            Preset::Simnet => 20,
        }
    }

    /// Spool groups this preset starts with, from the genesis config.
    ///
    /// Devnet inherits localnet, and simnet inherits mainnet, so the two pairs
    /// match. The count only ever rises, and only by operator vote, so this is a
    /// floor rather than the width a mature network runs at.
    pub fn spool_groups(&self) -> u64 {
        match self {
            Preset::Mainnet => 1,
            Preset::Devnet => 5,
            Preset::Localnet => 5,
            Preset::Simnet => 1,
        }
    }

    /// Short label for the report.
    pub fn label(&self) -> &'static str {
        match self {
            Preset::Mainnet => "mainnet",
            Preset::Devnet => "devnet",
            Preset::Localnet => "localnet",
            Preset::Simnet => "simnet",
        }
    }
}

/// Which RPC assumption a node runs under for entropy ingestion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcModel {
    OwnIngestor,
    ExternalRpc,
    Both,
}

impl RpcModel {
    /// Short label for the report.
    pub fn label(&self) -> &'static str {
        match self {
            RpcModel::OwnIngestor => "own-ingestor",
            RpcModel::ExternalRpc => "external-rpc",
            RpcModel::Both => "both",
        }
    }
}

/// The network shape and cadence the measured run is projected onto.
#[derive(Clone, Copy, Debug)]
pub struct ScaleParams {
    /// Preset the epoch length came from, or none for an explicit second count.
    pub preset: Option<Preset>,
    /// Epoch duration in seconds.
    pub epoch_seconds: u64,
    /// Solana slot time in milliseconds.
    pub slot_ms: u64,
    /// Slots between challenge rounds, setting the per-group cadence.
    pub round_interval_slots: u64,
    /// Number of spool groups in the network.
    pub spool_groups: u64,
    /// Whether that count came from the preset rather than being asked for.
    pub groups_from_preset: bool,
    /// Which RPC assumption to headline in the report.
    pub rpc_model: RpcModel,
}

impl ScaleParams {
    /// Total slots in one epoch at this slot time.
    pub fn epoch_slots(&self) -> u64 {
        self.epoch_seconds.saturating_mul(1_000) / self.slot_ms.max(1)
    }

    /// Challenge rounds per group per epoch, epoch slots over the cadence.
    pub fn rounds_per_epoch(&self) -> u64 {
        self.epoch_slots() / self.round_interval_slots.max(1)
    }

    /// Label for the epoch length, the preset name or an explicit second count.
    pub fn epoch_label(&self) -> String {
        match self.preset {
            Some(preset) => preset.label().to_string(),
            None => format!("{}s", self.epoch_seconds),
        }
    }
}

/// One node's measured and projected footprint, ready to print.
///
/// The measured block is read straight from a short representative run; the
/// projected block is that measurement scaled analytically to a mainnet epoch
/// and to every spool group.
pub struct Projection {
    /// Epoch length label, a preset name or an explicit second count.
    pub epoch_label: String,
    /// Epoch duration in seconds.
    pub epoch_seconds: u64,
    /// Solana slot time in milliseconds.
    pub slot_ms: u64,
    /// Slots between challenge rounds.
    pub round_interval_slots: u64,
    /// Total slots in one epoch.
    pub epoch_slots: u64,
    /// Number of spool groups in the network.
    pub groups: u64,
    /// Whether that count came from the preset rather than being asked for.
    pub groups_from_preset: bool,
    /// Committee size of one group.
    pub group_size: usize,
    /// Total network nodes, one per spool position.
    pub total_nodes: u64,
    /// Which RPC assumption is headlined.
    pub rpc_model: RpcModel,
    /// Epochs run in the representative measurement.
    pub measure_epochs: u64,
    /// Rounds per epoch run in the representative measurement.
    pub measure_rounds_per_epoch: u64,
    /// Total rounds observed in the measurement.
    pub measure_rounds: u64,
    /// Certificates formed per round in one group, measured.
    pub certs_per_round: f64,
    /// Fraction of challenges that certified, measured.
    pub cert_fraction: f64,
    /// Attestations gathered per round in one group, measured.
    pub attestations_per_round: f64,
    /// Evictions that landed per epoch in one group, measured.
    pub evictions_per_epoch_per_group: f64,
    /// Possession response bytes on the wire: the sampled sub-leaf plus both
    /// merkle paths, which is what a prover must actually send.
    pub proof_bytes: usize,
    /// Whole-slice response bytes, the alternative reading, measured.
    pub full_slice_bytes: usize,
    /// One attestation on the wire, signature plus a small header.
    pub attestation_bytes: usize,
    /// One certificate, aggregate signature plus the group signer bitmap.
    pub certificate_bytes: usize,
    /// Rounds per epoch for one group, from the cadence.
    pub rounds_per_epoch: u64,
    /// Rounds per epoch across all groups.
    pub rounds_per_epoch_all_groups: u64,
    /// Certificates per epoch in one group.
    pub certs_per_epoch_per_group: f64,
    /// Certificates per epoch network-wide, an off-chain signal count.
    pub certs_per_epoch_network: f64,
    /// Global lifecycle transactions per epoch: advance, commit, assignment.
    pub lifecycle_global: u64,
    /// Snapshot-commitment transactions per epoch, one per group.
    pub lifecycle_snapshots: u64,
    /// All epoch-lifecycle transactions per epoch.
    pub lifecycle_total: u64,
    /// External-rpc block-hash reads per epoch network-wide.
    pub rpc_external_per_epoch: f64,
    /// Proof bytes per node per epoch.
    pub proof_bytes_node_epoch: f64,
    /// Attestation bytes per node per epoch.
    pub attestation_bytes_node_epoch: f64,
    /// Proof bytes network-wide per epoch.
    pub proof_bytes_network_epoch: f64,
    /// Attestation bytes network-wide per epoch.
    pub attestation_bytes_network_epoch: f64,
}

impl Projection {
    /// Run the representative measurement and scale it to the given network.
    ///
    /// The measurement config drives the real one-group engine; blob bytes fix
    /// the proof size and the roster fixes the adversary mix that produces the
    /// measured eviction rate. Certificate byte sizes come from the real BLS
    /// signature type regardless of the measurement bls mode.
    pub fn compute(data: &PingData, measure: &EpochConfig, scale: &ScaleParams) -> Result<Self> {
        let costs = Costs::measure(measure.threshold, measure.blob_bytes, measure.group_size)?;
        Self::compute_with_costs(data, measure, scale, costs)
    }

    /// Scale a measured run against a caller-supplied cost model.
    ///
    /// The public entry measures the crypto costs first; tests inject a fixed
    /// model so the projection does not pay the one-time measurement.
    pub fn compute_with_costs(
        data: &PingData,
        measure: &EpochConfig,
        scale: &ScaleParams,
        costs: Costs,
    ) -> Result<Self> {
        let report = simulate_epochs_with_costs(data, measure, costs)?;

        let measure_rounds = measure.epochs.saturating_mul(measure.rounds_per_epoch);
        let rounds = measure_rounds.max(1) as f64;
        let total_challenges = report.cert_count + report.gap_count;
        let certs_per_round = report.cert_count as f64 / rounds;
        let cert_fraction = if total_challenges == 0 {
            0.0
        } else {
            report.cert_count as f64 / total_challenges as f64
        };

        let mut total_attestations = 0u64;
        for event in report.timeline.events() {
            if let Event::Challenge(record) = event {
                total_attestations += record.signer_count as u64;
            }
        }
        let attestations_per_round = total_attestations as f64 / rounds;

        let mut evictions = 0u64;
        for summary in &report.per_epoch {
            evictions += summary.evicted.len() as u64;
        }
        let evictions_per_epoch_per_group = evictions as f64 / measure.epochs.max(1) as f64;

        // A possession response is the real two-level proof: the sampled sub-leaf
        // bytes plus the path to its slice root and on to the commitment. Costing
        // it as a leaf hash and one path would price the cached-proof response the
        // commitment probe shows a free-rider can replay without the bytes.
        let proof_bytes = SpoolHoldings::coded_response_bytes();
        let spool = Spool::build(measure.blob_bytes)?;
        let commitment = CommitmentReport::measure(&spool)?;
        let full_slice_bytes = commitment.full_slice_bytes;

        let signature_bytes = BlsSignature::size();
        let attestation_bytes = signature_bytes + ATTESTATION_HEADER_BYTES;
        let bitmap_bytes = measure.group_size.div_ceil(8);
        let certificate_bytes = signature_bytes + bitmap_bytes;

        let group_size = measure.group_size;
        let groups = scale.spool_groups;
        let total_nodes = groups.saturating_mul(group_size as u64);
        let rounds_per_epoch = scale.rounds_per_epoch();
        let rounds_per_epoch_all_groups = rounds_per_epoch.saturating_mul(groups);

        // Every position is challenged each round, so a round holds group_size
        // proof fan-outs; each prover broadcasts to the other group_size minus one.
        let proofs_per_round = (group_size as f64) * (group_size.saturating_sub(1) as f64);
        let proof_bytes_per_round = proofs_per_round * proof_bytes as f64;
        let attestation_bytes_per_round = attestations_per_round * attestation_bytes as f64;

        let rounds_scale = rounds_per_epoch as f64;
        let proof_bytes_group_epoch = proof_bytes_per_round * rounds_scale;
        let attestation_bytes_group_epoch = attestation_bytes_per_round * rounds_scale;
        let proof_bytes_network_epoch = proof_bytes_group_epoch * groups as f64;
        let attestation_bytes_network_epoch = attestation_bytes_group_epoch * groups as f64;
        let node_divisor = total_nodes.max(1) as f64;
        let proof_bytes_node_epoch = proof_bytes_network_epoch / node_divisor;
        let attestation_bytes_node_epoch = attestation_bytes_network_epoch / node_divisor;

        let certs_per_epoch_per_group = certs_per_round * rounds_scale;
        let certs_per_epoch_network = certs_per_epoch_per_group * groups as f64;

        // Eviction transactions are deliberately not projected. The measured rate
        // is the one-time purge of the seeded adversary roster, not a steady
        // state, so scaling it to an epoch of any length reports a constant as a
        // rate. Lifecycle is the only on-chain cost this run can honestly claim.
        let lifecycle_global = GLOBAL_LIFECYCLE_TXNS;
        let lifecycle_snapshots = groups;
        let lifecycle_total = lifecycle_global + lifecycle_snapshots;

        // Own-ingestor reads no block over rpc per round; external-rpc reads one
        // finalized block hash per round per node.
        let rpc_external_per_epoch = rounds_per_epoch as f64 * total_nodes as f64;

        Ok(Self {
            epoch_label: scale.epoch_label(),
            epoch_seconds: scale.epoch_seconds,
            slot_ms: scale.slot_ms,
            round_interval_slots: scale.round_interval_slots,
            epoch_slots: scale.epoch_slots(),
            groups,
            groups_from_preset: scale.groups_from_preset,
            group_size,
            total_nodes,
            rpc_model: scale.rpc_model,
            measure_epochs: measure.epochs,
            measure_rounds_per_epoch: measure.rounds_per_epoch,
            measure_rounds,
            certs_per_round,
            cert_fraction,
            attestations_per_round,
            evictions_per_epoch_per_group,
            proof_bytes,
            full_slice_bytes,
            attestation_bytes,
            certificate_bytes,
            rounds_per_epoch,
            rounds_per_epoch_all_groups,
            certs_per_epoch_per_group,
            certs_per_epoch_network,
            lifecycle_global,
            lifecycle_snapshots,
            lifecycle_total,
            rpc_external_per_epoch,
            proof_bytes_node_epoch,
            attestation_bytes_node_epoch,
            proof_bytes_network_epoch,
            attestation_bytes_network_epoch,
        })
    }

    /// Print the projection in the crate's plain-line voice.
    pub fn print(&self) {
        println!("whirlwind {} projection", self.epoch_label);
        println!(
            "  epoch {} ({} s)  slot {} ms  round every {} slots  epoch slots {}",
            self.epoch_label,
            self.epoch_seconds,
            self.slot_ms,
            self.round_interval_slots,
            self.epoch_slots,
        );
        println!(
            "  network {} group{} x {} nodes = {} nodes",
            self.groups,
            if self.groups == 1 { "" } else { "s" },
            self.group_size,
            self.total_nodes,
        );
        // The group count only rises, and only by operator vote, so a preset's
        // genesis value is a floor. Pass --spool-groups to project a wider one.
        if self.groups_from_preset {
            println!("  group count is this preset's genesis value, not a mature width");
        }
        println!();

        println!("measured inputs (one group, {} epochs x {} rounds)", self.measure_epochs, self.measure_rounds_per_epoch);
        println!("  certificates per round        {:.3}  ({:.1}% of {} challenges certified)", self.certs_per_round, self.cert_fraction * 100.0, self.group_size);
        println!("  attestations per round        {:.1}", self.attestations_per_round);
        println!("  evictions per epoch per group {:.3}  (seeded adversary mix, front-loaded)", self.evictions_per_epoch_per_group);
        println!("  proof bytes (sub-leaf + paths) {}", human_bytes(self.proof_bytes as f64));
        println!("  attestation bytes (sig + hdr) {}", human_bytes(self.attestation_bytes as f64));
        println!("  certificate bytes (sig + map) {}", human_bytes(self.certificate_bytes as f64));
        println!("  whole-slice reading           {}", human_bytes(self.full_slice_bytes as f64));
        println!();

        println!("rounds per epoch");
        println!("  per group      {}", self.rounds_per_epoch);
        println!("  all groups     {}", self.rounds_per_epoch_all_groups);
        // The cadence floors at the round width, so a wide schedule on a short
        // epoch fits fewer rounds than the eviction rule needs. Say so rather
        // than let the rate path quietly never engage.
        if self.rounds_per_epoch < TARGET_ROUNDS_PER_EPOCH {
            println!(
                "  short: {} rounds is under the {} an epoch needs, so the rate path cannot engage inside one and only the consecutive-miss path can evict",
                self.rounds_per_epoch, TARGET_ROUNDS_PER_EPOCH,
            );
        }
        println!();

        println!("certificates per epoch (off-chain signal, not transactions)");
        println!("  per group      {}", round_count(self.certs_per_epoch_per_group));
        println!("  network        {}", round_count(self.certs_per_epoch_network));
        println!();

        println!("on-chain transactions per epoch");
        println!("  lifecycle      {}  (advance 1, commit 1, assignment 1, snapshot per group {})", self.lifecycle_total, self.lifecycle_snapshots);
        println!("  challenges add none: a challenge round is off chain and forms no record");
        println!("  evictions      not projected: the measured rate is a one-time purge of the");
        println!("                 seeded roster, so it is a constant rather than a rate");
        println!();

        println!("rpc calls per epoch");
        self.print_rpc_line(RpcModel::OwnIngestor, 0.0);
        self.print_rpc_line(RpcModel::ExternalRpc, self.rpc_external_per_epoch);
        println!();

        println!("bandwidth per epoch (from measured per-round bytes)");
        println!(
            "  proof         per node {}  network {}",
            human_bytes(self.proof_bytes_node_epoch),
            human_bytes(self.proof_bytes_network_epoch),
        );
        println!(
            "  attestation   per node {}  network {}",
            human_bytes(self.attestation_bytes_node_epoch),
            human_bytes(self.attestation_bytes_network_epoch),
        );
        println!(
            "  total         per node {}  network {}",
            human_bytes(self.proof_bytes_node_epoch + self.attestation_bytes_node_epoch),
            human_bytes(self.proof_bytes_network_epoch + self.attestation_bytes_network_epoch),
        );
        println!();

        self.print_footer();
    }

    /// Print one rpc assumption line, marking the headlined model.
    fn print_rpc_line(&self, model: RpcModel, value: f64) {
        let selected = self.rpc_model == model || self.rpc_model == RpcModel::Both;
        let mark = if selected { "*" } else { " " };
        let note = match model {
            RpcModel::OwnIngestor => "zero per-round reads, only occasional catch-up",
            RpcModel::ExternalRpc => "one finalized block-hash read per round per node",
            RpcModel::Both => "",
        };
        println!("  {} {:<12} {:>18}  ({})", mark, model.label(), round_count(value), note);
    }

    /// State plainly which inputs were measured and which are analytic scalings.
    fn print_footer(&self) {
        println!("measured vs modeled");
        println!("  measured from the run: certificates per round, the certified fraction, attestations per round, and evictions per epoch per group");
        println!("  measured from the real code: a proof carries the sampled sub-leaf bytes plus the path to its slice root and on to the commitment, not a leaf hash alone, and the signature is a real compressed bls signature");
        println!("  modeled: rounds per epoch from the slot cadence, the scale to all groups and a full epoch, proof fan-out as a full-group broadcast, the {} byte attestation header, and lifecycle transaction counts", ATTESTATION_HEADER_BYTES);
        println!("  the eviction rate reflects the seeded adversary mix and the initial purge, so it is an upper bound on the steady state");
    }
}

/// Round a projected count to a whole number for the report.
fn round_count(value: f64) -> u64 {
    value.round() as u64
}

/// Human-readable IEC byte size across the full range the projection spans.
fn human_bytes(value: f64) -> String {
    StorageUnits(value.round() as u64).human()
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use super::*;
    use crate::sim::epoch::BlsMode;
    use crate::spool::TREE_HEIGHT;
    use crate::types::RoundNumber;

    fn ping_data() -> &'static PingData {
        static DATA: OnceLock<PingData> = OnceLock::new();
        DATA.get_or_init(|| PingData::load().expect("load ping dataset"))
    }

    /// A fixed cost model so the projection tests skip the one-time measurement.
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

    fn mainnet_scale() -> ScaleParams {
        ScaleParams {
            preset: Some(Preset::Mainnet),
            epoch_seconds: Preset::Mainnet.epoch_seconds(),
            slot_ms: 400,
            round_interval_slots: 150,
            spool_groups: 50,
            groups_from_preset: false,
            rpc_model: RpcModel::Both,
        }
    }

    #[test]
    fn preset_groups() {
        // These are the genesis values. Devnet inherits localnet and simnet
        // inherits mainnet, which is why the pairs match.
        assert_eq!(Preset::Mainnet.spool_groups(), 1);
        assert_eq!(Preset::Devnet.spool_groups(), 5);
        assert_eq!(Preset::Localnet.spool_groups(), 5);
        assert_eq!(Preset::Simnet.spool_groups(), 1);
    }

    /// A small one-group measurement config; certificate bls off, as the bin uses.
    fn measurement_config(epochs: u64, rounds_per_epoch: u64) -> EpochConfig {
        use crate::sim::epoch::{BehaviorConfig, Roster};
        use crate::sim::scoreboard::EvictionRule;
        EpochConfig {
            epochs,
            rounds_per_epoch,
            blob_bytes: 60_000,
            bls_mode: BlsMode::Off,
            roster: Roster::with_colluders(20, 2),
            behavior: BehaviorConfig {
                selective_deliver_probability: 0.4,
                offline_after_round: RoundNumber::new(rounds_per_epoch / 3),
            },
            eviction: EvictionRule::default(),
            ..EpochConfig::default()
        }
    }

    #[test]
    fn cadence_rounds() {
        let scale = mainnet_scale();
        assert_eq!(scale.epoch_slots(), 604_800 * 1000 / 400);
        assert_eq!(scale.rounds_per_epoch(), scale.epoch_slots() / 150);
    }

    #[test]
    fn projection_consistency() {
        let measure = measurement_config(4, 16);
        let scale = mainnet_scale();
        let projection = Projection::compute_with_costs(ping_data(), &measure, &scale, test_costs()).expect("project");

        // Rounds across all groups are the per-group cadence times the groups.
        assert_eq!(
            projection.rounds_per_epoch_all_groups,
            projection.rounds_per_epoch * projection.groups,
        );

        // Certificates are rounds times groups times the measured per-round rate.
        let expected_certs =
            projection.rounds_per_epoch as f64 * projection.groups as f64 * projection.certs_per_round;
        assert!((projection.certs_per_epoch_network - expected_certs).abs() < 1.0);

        // Lifecycle is the global handful plus one snapshot per group.
        assert_eq!(projection.lifecycle_total, GLOBAL_LIFECYCLE_TXNS + projection.groups);

        // External rpc is one read per round per node; own-ingestor adds none.
        let expected_rpc = projection.rounds_per_epoch as f64 * projection.total_nodes as f64;
        assert!((projection.rpc_external_per_epoch - expected_rpc).abs() < 1.0);

        // Network bandwidth is the per-node figure times the node count.
        let network = projection.proof_bytes_node_epoch * projection.total_nodes as f64;
        assert!((projection.proof_bytes_network_epoch - network).abs() / network < 1e-9);
    }

    #[test]
    fn response_bytes() {
        let measure = measurement_config(2, 8);
        let scale = mainnet_scale();
        let projection = Projection::compute_with_costs(ping_data(), &measure, &scale, test_costs()).expect("project");

        // The response is the sampled sub-leaf plus both merkle paths.
        assert_eq!(projection.proof_bytes, SpoolHoldings::coded_response_bytes());

        // It must exceed a leaf hash with one path, which is the cached response
        // a free-rider can replay without holding any bytes.
        assert!(projection.proof_bytes > 32 * (TREE_HEIGHT + 1));

        // And still sit far below reading the whole slice.
        assert!(projection.full_slice_bytes > projection.proof_bytes);
    }
}
