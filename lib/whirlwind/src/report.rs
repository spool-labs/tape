//! Human-readable output for each probe. Plain lines, no decorative banners.

use tape_core::types::StorageUnits;

use crate::probes::commitment::CommitmentReport;
use crate::probes::detection::DetectionReport;
use crate::probes::grinding::GrindingReport;
use crate::probes::lifecycle::LifecycleReport;
use crate::probes::quorum::QuorumReport;
use crate::probes::reconstruct::ReconstructReport;
use crate::spool::Spool;

pub fn print_spool(spool: &Spool) {
    println!("spool");
    println!("  payload bytes      {}", human_bytes(spool.blob_len));
    println!("  profile            clay n={} k={} d={}", spool.group_size, spool.data_shards, spool.helper_count);
    println!("  stripe size        {}", human_bytes(spool.stripe_size));
    println!("  stripes            {}", spool.num_stripes);
    println!("  chunk size         {}", human_bytes(spool.chunk_size));
    println!("  alpha / beta       {} / {}", spool.alpha, spool.beta);
    println!("  sub-chunk size     {}", human_bytes(spool.sub_chunk_size()));
    println!("  slice bytes        {}", human_bytes(spool.slice_len()));
    let root = spool.commitment.0;
    println!(
        "  commitment root    {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}...",
        root[0], root[1], root[2], root[3], root[4], root[5], root[6], root[7]
    );
    println!();
}

pub fn print_lifecycle(report: &LifecycleReport) {
    println!("honest round cost (real per-phase compute, median, no network)");
    println!("  group / threshold       n={} q={}", report.group_size, report.threshold);
    println!("  seed derivation         {}", nanos(report.seed_nanos));
    println!("  merkle proof gen        {}", nanos(report.proof_gen_nanos));
    println!("  merkle proof verify     {}", nanos(report.proof_verify_nanos));
    println!("  bls attestation sign    {}", nanos(report.sign_nanos));
    println!("  bls aggregate (q sigs)  {}", nanos(report.aggregate_nanos));
    println!("  bls certificate verify  {}", nanos(report.cert_verify_nanos));
    println!("  total compute           {}", nanos(report.compute_nanos));
    println!("  certificate verifies?   {}", yes_no(report.cert_ok));
    println!();
    println!("  finding: the honest round's compute is dominated by the BLS certificate");
    println!("  verify and aggregate. It is a real fraction of a fast RTT, not negligible,");
    println!("  but small next to cross-region latency. The histogram shows where each");
    println!("  phase sits once network is added.");
    println!();
}

pub fn print_commitment(report: &CommitmentReport) {
    println!("commitment structure (what the registered root commits to)");
    println!("  leaves under root       {}", report.leaf_count);
    println!("  top tree height         {}", report.tree_height);
    println!("  bytes per top leaf      {}  (one whole slice)", human_bytes(report.slice_bytes));
    println!("  sub-leaves per slice    {}", report.sub_leaves_per_slice);
    println!("  sub-tree height         {}", report.sub_tree_height);
    println!();
    println!("  reading A: top level only (slice root + top path, no slice bytes)");
    println!("    response size         {}", human_bytes(report.slice_root_bytes));
    println!("    verifies?             {}", yes_no(report.slice_root_passes));
    println!("    -> a free-rider caching {} answers forever without the slice", human_bytes(report.slice_root_bytes));
    println!();
    println!("  reading B: top level with the bytes (verifier rebuilds the slice root)");
    println!("    response size         {}", human_bytes(report.full_slice_bytes));
    println!("    verifies?             {}", yes_no(report.full_slice_verifies));
    println!("    -> possession is tested, but the response grows with the track");
    println!();
    println!("  reading C: one sampled sub-leaf (bytes + path to the slice root)");
    println!("    response size         {}", human_bytes(report.sub_leaf_bytes));
    println!("    verifies?             {}", yes_no(report.sub_leaf_verifies));
    println!("    replays at another position? {}", yes_no(report.sub_leaf_replays));
    println!("    -> real bytes, one fixed size at any track size");
    println!();
    println!("  finding: the top level alone is answerable from a cached hash, which");
    println!("  is why the challenge samples the sub-leaf level instead.");
    println!();
}

pub fn print_reconstruct(report: &ReconstructReport) {
    println!("faking one sampled unit by reconstruction (real Clay repair, one stripe)");
    println!("  sampled sub-leaf        {}", human_bytes(report.sub_leaf_bytes));
    println!("  chunk size              {}", human_bytes(report.chunk_size));
    println!("  helpers contacted       {}", report.helper_count);
    println!("  bytes per helper        {}", human_bytes(report.bytes_per_helper));
    println!("  total bytes fetched     {}", human_bytes(report.bytes_fetched));
    println!("  as chunk-equivalents    {:.2}x  (Appendix B: d/(d-k+1))", report.chunk_equivalents);
    println!("  full recovery instead   {}", human_bytes(report.recovery_bytes));
    println!("  decode time (median)    {}", micros(report.decode_micros));
    println!("  rebuilt owner chunk?    {}", yes_no(report.reconstruct_is_correct));
    println!("  honest local read       {}", human_bytes(report.honest_read_bytes));
    println!();
    println!("  wall-clock model (parallel helper fetch, one round trip):");
    println!("    {:>8}  {:>12}  {:>12}  {:>10}", "rtt", "honest", "free-rider", "margin");
    for row in &report.rows {
        println!(
            "    {:>6.0}ms  {:>10.3}ms  {:>10.3}ms  {:>8.3}ms",
            row.rtt_ms, row.honest_ms, row.freeloader_ms, row.margin_ms
        );
    }
    println!();
    println!("  finding: the free-rider's extra cost over an honest read is ~one RTT plus");
    println!("  {} of transfer and {} of decode. Any deadline loose enough to admit", human_bytes(report.bytes_per_helper), micros(report.decode_micros));
    println!("  a slow honest node (jitter > one RTT) also admits the fake. Repair is not");
    println!("  even the cheapest fake: fetching the chunk from one peer that holds it");
    println!("  moves {} over one link.", human_bytes(report.chunk_size));
    println!();
}

pub fn print_detection(report: &DetectionReport) {
    println!("detection of a silently discarded fraction (one sample per round)");
    println!("  discard fraction p      {:.4}", report.discard_fraction);
    println!("  cadence                 {} s", report.cadence_secs);
    println!("  expected rounds to catch {:.1}  (= 1/p)", report.expected_rounds);
    println!("  expected time to detect  {:.2} h", report.expected_hours);
    match report.empirical_rounds {
        Some(rounds) => println!("  simulated mean rounds    {rounds:.1}"),
        None => println!("  simulated mean rounds    n/a (p too small to simulate cheaply)"),
    }
    println!("  survival (1-p)^l:");
    for (rounds, probability) in &report.survival {
        println!("    l={rounds:<5} {probability:.6}");
    }
    println!();
}

pub fn print_grinding(report: &GrindingReport) {
    println!("entropy-block grinding (producer regrinds until sample lands on kept data)");
    println!("  discard fraction p      {:.4}", report.discard_fraction);
    println!("  expected attempts       {:.3}  (= 1/(1-p))", report.expected_attempts);
    println!("  simulated attempts      {:.3}", report.empirical_attempts);
    println!();
}

pub fn print_quorum(report: &QuorumReport) {
    println!("q-signature collection under one slow honest peer (owner self-signs)");
    println!("  group n={} byzantine f={} honest={}", report.group_size, report.byzantine, report.honest);
    println!("    {:<34} {:>3} {:>6} {:>6} {:>10} {:>8}", "threshold", "q", "slow", "reach", "certify?", "margin");
    for row in &report.rows {
        println!(
            "    {:<34} {:>3} {:>6} {:>6} {:>10} {:>+8}",
            row.label, row.threshold, row.slow_honest, row.reachable_honest, yes_no(row.can_certify), row.margin
        );
    }
    println!();
    println!("  finding: reusing the storage threshold q=n-f makes one slow honest peer");
    println!("  void the round; q=f+1 (one honest verifier) keeps slack.");
    println!();
}

fn human_bytes(bytes: usize) -> String {
    StorageUnits(bytes as u64).human()
}

fn micros(value: u128) -> String {
    if value >= 1_000 {
        format!("{:.3} ms", value as f64 / 1_000.0)
    } else {
        format!("{value} us")
    }
}

fn nanos(value: u128) -> String {
    if value >= 1_000_000 {
        format!("{:.3} ms", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.3} us", value as f64 / 1_000.0)
    } else {
        format!("{value} ns")
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}
