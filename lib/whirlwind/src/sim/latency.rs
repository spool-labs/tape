//! Per-position latency decomposition of one Whirlwind challenge round.
//!
//! For a challenged spool X in a group placed at real cities, each other spool
//! receives X's possession proof, verifies it, signs an attestation, and gossips
//! it back. The round latency to gather each attestation decomposes into four
//! phases, mirroring the Alpenglow latency histogram:
//!
//! - network: the proof's forward hop to the peer (real ping / 2)
//! - proof: proof generation plus the peer's verify (measured compute)
//! - attestation: the peer's BLS sign plus the return hop (compute + real ping)
//! - certificate: aggregate plus aggregate-verify, present once q are gathered
//!
//! Averaging over many random placements gives a smooth curve of latency to
//! reach each fraction of the group. A free-rider that stored nothing must first
//! reconstruct the sampled chunk from d helpers, which shifts its whole curve up
//! by one intra-group round trip plus the Clay decode.

use anyhow::Result;
use serde::Serialize;
use tape_core::erasure::SUB_LEAF_BYTES;
use tape_crypto::hash::hashv;

use crate::network::simulated::ping_data::PingData;
use crate::network::simulated::Group;
use crate::probes::lifecycle::LifecycleReport;
use crate::probes::reconstruct::ReconstructReport;
use crate::crypto::median_nanos;
use crate::sim::node::intra_group_rtt_ms;
use crate::sim::round::{relay_arrivals, RelayPolicy};
use crate::sim::track::SpoolHoldings;
use crate::spool::Spool;
use crate::types::GroupPosition;

const COMPUTE_ITERATIONS: usize = 51;
const CHALLENGED: usize = 0;
/// Domain tag for the sample the cost measurement proves against.
const COST_DOMAIN: &[u8] = b"WHRLWCST1";

/// Measured crypto and reconstruction costs (ms) on this machine.
#[derive(Clone, Copy, Serialize)]
pub struct Costs {
    pub proof_gen_ms: f64,
    pub verify_ms: f64,
    pub sign_ms: f64,
    pub aggregate_ms: f64,
    pub cert_verify_ms: f64,
    pub decode_ms: f64,
    pub helper_count: usize,
}

impl Costs {
    /// Measure against the payload this run actually challenges, so the proof
    /// and decode costs describe it rather than a fixed reference blob, and
    /// against the sub-leaf the challenge really samples.
    ///
    /// Proof generation and verification are timed on the sub-leaf proof a round
    /// actually asks for. Timing the top tree instead would measure a five-hash
    /// path against the real sixteen and understate both.
    pub fn measure(threshold: usize, blob_bytes: usize, group_size: usize) -> Result<Self> {
        let spool = Spool::build(blob_bytes)?;
        let lifecycle = LifecycleReport::measure(&spool, threshold, COMPUTE_ITERATIONS)?;
        let reconstruct =
            ReconstructReport::measure(&spool, SUB_LEAF_BYTES, &[10.0], 1_000.0, 5_000.0)?;

        let holdings = SpoolHoldings::build(blob_bytes, group_size)?;
        let seed = hashv(&[COST_DOMAIN, &(blob_bytes as u64).to_le_bytes()]);
        let sample = holdings.sample(GroupPosition::new(CHALLENGED), &seed);
        let proof = holdings.prove(sample)?;
        let proof_gen_nanos = median_nanos(COMPUTE_ITERATIONS, || holdings.prove(sample).ok());
        let proof_verify_nanos = median_nanos(COMPUTE_ITERATIONS, || holdings.verify(sample, &proof));

        let ms = |nanos: u128| nanos as f64 / 1_000_000.0;
        Ok(Self {
            proof_gen_ms: ms(proof_gen_nanos),
            verify_ms: ms(proof_verify_nanos),
            sign_ms: ms(lifecycle.sign_nanos),
            aggregate_ms: ms(lifecycle.aggregate_nanos),
            cert_verify_ms: ms(lifecycle.cert_verify_nanos),
            decode_ms: reconstruct.decode_micros as f64 / 1_000.0,
            helper_count: reconstruct.helper_count,
        })
    }
}

/// Per-position averaged phase latencies in milliseconds, one entry per group
/// position. Certificate cost is zero below the threshold position. Two overlays
/// sit on top: the reconstruction free-rider that rebuilds from group helpers,
/// and the nearby-fetch free-rider that reads the public bytes from a metro edge.
pub struct PhaseCurve {
    pub group_size: usize,
    pub threshold: usize,
    pub rounds: usize,
    pub network_ms: Vec<f64>,
    pub proof_ms: Vec<f64>,
    pub attest_ms: Vec<f64>,
    pub cert_ms: Vec<f64>,
    pub freerider_total_ms: Vec<f64>,
    pub nearby_total_ms: Vec<f64>,
}

impl PhaseCurve {
    /// Total honest latency to reach each position (network + proof + attest + cert).
    pub fn honest_total_ms(&self) -> Vec<f64> {
        (0..self.group_size)
            .map(|p| self.network_ms[p] + self.proof_ms[p] + self.attest_ms[p] + self.cert_ms[p])
            .collect()
    }

    /// Peak total latency across honest and free-rider curves, for axis scaling.
    pub fn peak_ms(&self) -> f64 {
        let honest = self.honest_total_ms();
        honest
            .iter()
            .chain(self.freerider_total_ms.iter())
            .chain(self.nearby_total_ms.iter())
            .copied()
            .fold(0.0, f64::max)
    }
}

/// One peer's per-phase latency in one round.
struct Sample {
    network: f64,
    proof: f64,
    attest: f64,
    total: f64,
}

/// Simulate rounds random placements and average the per-position phases.
pub fn simulate(
    data: &PingData,
    costs: &Costs,
    group_size: usize,
    threshold: usize,
    rounds: usize,
    edge_rtt_ms: f64,
    relay: RelayPolicy,
) -> PhaseCurve {
    let mut network_ms = vec![0.0; group_size];
    let mut proof_ms = vec![0.0; group_size];
    let mut attest_ms = vec![0.0; group_size];
    let mut freerider_total_ms = vec![0.0; group_size];
    let mut nearby_total_ms = vec![0.0; group_size];

    let cert_cost = costs.aggregate_ms + costs.cert_verify_ms;
    // A nearby-fetch free-rider reads the public bytes from a metro edge and
    // re-encodes, so its extra cost is one small round trip plus a decode.
    let nearby_delay = edge_rtt_ms + costs.decode_ms;

    for round in 0..rounds {
        let group = Group::sample(data, group_size, round as u64 + 1);
        let size = group.len();
        if size == 0 {
            continue;
        }

        // An honest owner sends to the whole group, and the policy decides
        // whether peers relay onward, so a distant peer can be reached by a
        // shorter path than the direct hop.
        let sends_to = vec![true; size];
        let forward_ms = relay_arrivals(&group, CHALLENGED, 0.0, &sends_to, costs.verify_ms, relay);

        // Per-peer attestation latency for the challenged spool.
        let mut samples: Vec<Sample> = (0..size)
            .map(|peer| {
                let forward = forward_ms[peer];
                let back = group.one_way_ms(peer, CHALLENGED);
                let network = forward;
                let proof = costs.proof_gen_ms + costs.verify_ms;
                let attest = costs.sign_ms + back;
                Sample {
                    network,
                    proof,
                    attest,
                    total: network + proof + attest,
                }
            })
            .collect();
        samples.sort_by(|a, b| a.total.total_cmp(&b.total));

        // Reconstruction delay for a free-rider: the d-th nearest helper round
        // trip plus the Clay decode, applied before the owner can build.
        let recon_delay =
            intra_group_rtt_ms(GroupPosition::new(CHALLENGED), &group, costs.helper_count)
                + costs.decode_ms;

        for position in 0..size.min(group_size) {
            let sample = &samples[position];
            network_ms[position] += sample.network;
            proof_ms[position] += sample.proof;
            attest_ms[position] += sample.attest;
            let cert = if position + 1 >= threshold { cert_cost } else { 0.0 };
            freerider_total_ms[position] += sample.total + cert + recon_delay;
            nearby_total_ms[position] += sample.total + cert + nearby_delay;
        }
    }

    let divisor = rounds.max(1) as f64;
    let average = |values: &mut Vec<f64>| values.iter_mut().for_each(|value| *value /= divisor);
    average(&mut network_ms);
    average(&mut proof_ms);
    average(&mut attest_ms);
    average(&mut freerider_total_ms);
    average(&mut nearby_total_ms);

    let cert_ms = (0..group_size)
        .map(|position| if position + 1 >= threshold { cert_cost } else { 0.0 })
        .collect();

    PhaseCurve {
        group_size,
        threshold,
        rounds,
        network_ms,
        proof_ms,
        attest_ms,
        cert_ms,
        freerider_total_ms,
        nearby_total_ms,
    }
}
