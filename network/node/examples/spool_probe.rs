//! Fetch spool tracks from the network with the spool manager's own code and
//! report where the time goes.
//!
//! It runs as an existing node's identity, so peers answer it, and keeps what
//! it fetches in a memory store it throws away. It reads tracks and encodings
//! from a node store opened read-only.
//!
//! Fetch mode calls repair or recovery directly, at a fixed concurrency:
//!
//! ```text
//! spool_probe --mode fetch --config /etc/tape/0/node.yaml --source /var/lib/tape/0 \
//!   --virtual-group 7 --position 13 --method repair --tracks 1000 --concurrency 1,4,16
//! ```
//!
//! Loop mode runs the spool manager itself on the node's own live spool, as if
//! the node had lost it, once for each `max_peer_fetches` value:
//!
//! ```text
//! spool_probe --mode loop --config /etc/tape/0/node.yaml --source /var/lib/tape/0 \
//!   --tracks 3000 --seconds 120 --peer-fetches 2,4,8,16 --max-fetches 32
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use tape_core::encoding::ErasureCode;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_node::config::node::NodeConfig;
use tape_node::config::recovery::RecoveryConfig;
use tape_node::context::{NodeContext, NodeContextBuilder};
use tape_node::core::atlas::AtlasBuffer;
use tape_node::core::ingest::IngestState;
use tape_node::features::spool::advance::get_owned_live_spools;
use tape_node::features::spool::history::{load_owner_history, record_epoch_owners};
use tape_node::features::spool::manager::SpoolManager;
use tape_node::features::spool::{recover, repair};
use tape_peer_http::{HttpApi, HttpApiBuilder, TRANSFER_CEILING};
use tape_peer_manager::PeerManager;
use tape_protocol::fetch::fetch_state_with_commitment;
use tape_protocol::ProtocolState;
use tape_recover::attempt::{can_recover, can_repair, Method, NextAttempt};
use tape_recover::fetch::{get_time_limit, get_track_bytes, FetchReq, FetchRes, FetchTrack};
use tape_recover::history::{get_group_peers, PositionPeer};
use tape_recover::limits::SpoolLimits;
use tape_recover::peer::{PeerOutcome, PeerScores};
use tape_recover::time::WallTime;
use tape_recover::track::MissingTrack;
use tape_rpc::CommitmentLevel;
use tape_rpc_client::RpcClient;
use tape_rpc_solana::{RpcConfig, SolanaRpc};
use tape_slicer::coded_slice_len;
use tape_store::ops::{SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps};
use tape_store::TapeStore;
use tape_store_memory::MemoryStore;
use tokio_util::sync::CancellationToken;

type Probe = NodeContext<MemoryStore, HttpApi, SolanaRpc>;
type Failure = Box<dyn std::error::Error + Send + Sync>;
type SourceTrack = (VirtualGroup, Address, CompressedTrack, BlobData);

/// Snapshot tapes carry this bit; their encodings record 8 bytes less than
/// their slices hold, so every repair of them fails
const SYSTEM_TAPE_BIT: u64 = 1 << 56;

/// How often loop mode reads its progress
const SAMPLE_EVERY: Duration = Duration::from_secs(10);

struct Args {
    values: HashMap<String, String>,
}

struct Job {
    track: FetchTrack,
    peers: Vec<PositionPeer>,
    bytes: StorageUnits,
}

struct Sample {
    elapsed: Duration,
    res: FetchRes,
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let args = parse_args()?;
    match args.get("mode")?.as_str() {
        "fetch" => run_fetch_mode(&args).await,
        "loop" => run_loop_mode(&args).await,
        other => Err(format!("mode {other}: fetch or loop").into()),
    }
}

/// Run the spool manager on the node's own live spool once per
/// `max_peer_fetches` value, each time from an empty memory store
async fn run_loop_mode(args: &Args) -> Result<(), Failure> {
    let config = NodeConfig::from_yaml_file(args.get("config")?)?;
    let source = TapeStore::open_read_only(args.get("source")?)?;
    let tracks: usize = args.get("tracks")?.parse()?;
    let seconds: u64 = args.get("seconds")?.parse()?;
    let variants = args.get("variants")?.clone();

    for variant in variants.split(';') {
        let (context, state) = build_live_probe(&config).await?;
        context.ingest.publish(IngestState::AtTip);
        let fold = state.fold().ok_or("no live groups")?;
        let me = context.node_address();
        let spools = get_owned_live_spools(&state, me);
        let seeded = seed_owned_spools(&source, &context.store, fold, &spools, tracks)?;

        let recovery = get_variant_config(variant)?;
        let cancel = CancellationToken::new();
        let manager = SpoolManager::new(Arc::clone(&context), recovery, cancel.clone());
        let task = tokio::spawn(manager.run());
        let report = watch_progress(&context, seconds).await?;
        cancel.cancel();
        let _ = task.await;

        println!(
            "{variant}: seeded {} coded and {} inline on live spools {spools:?}",
            seeded.0, seeded.1
        );
        println!("{report}");
        println!("  {}", count_left(&context.store, fold, &spools)?);
    }
    Ok(())
}

/// The default recovery config with a variant's `key=value` pairs set
fn get_variant_config(variant: &str) -> Result<RecoveryConfig, Failure> {
    let mut config = RecoveryConfig::default();
    for pair in variant.split(',') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(format!("variant pair {pair}: key=value").into());
        };
        let number: u64 = value.parse()?;
        let count = number as usize;
        match key {
            "max_peer_fetches" => config.max_peer_fetches = count,
            "max_fetches" => config.max_fetches = count,
            "max_recoveries" => config.max_recoveries = count,
            "fetch_tracks" => config.fetch_tracks = count,
            "turn_tracks" => config.turn_tracks = count,
            "scan_page" => config.scan_page = count,
            "turns_per_wake" => config.turns_per_wake = count,
            "wake_secs" => config.wake_secs = number,
            other => return Err(format!("unknown limit {other}").into()),
        }
    }
    Ok(config)
}

/// The missing tracks left on the list, by what their last attempt was
fn count_left(
    store: &TapeStore<MemoryStore>,
    fold: GroupFold,
    spools: &[SpoolIndex],
) -> Result<String, Failure> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for spool in spools {
        let mut after: Option<MissingTrack> = None;
        loop {
            let page = store.get_missing_tracks(*spool, fold, after.as_ref(), 4_096)?;
            let Some(last) = page.last().copied() else {
                break;
            };
            for missing in &page {
                let label = match missing.last_attempt {
                    Some(attempt) => format!("{:?} {:?}", attempt.method, attempt.failure),
                    None => "never tried".to_string(),
                };
                *counts.entry(label).or_default() += 1;
            }
            after = Some(last);
        }
    }
    Ok(format!("left on the list: {counts:?}"))
}

/// Fetch with repair or recovery directly, a wave per concurrency value
async fn run_fetch_mode(args: &Args) -> Result<(), Failure> {
    let config = NodeConfig::from_yaml_file(args.get("config")?)?;
    let (context, state) = build_live_probe(&config).await?;
    let profile = context.params().profile;
    record_epoch_owners(&context.store, &state.current, profile)?;
    if let Some(previous) = &state.previous {
        record_epoch_owners(&context.store, previous, profile)?;
    }

    let method = match args.get("method")?.as_str() {
        "repair" => Method::Repair,
        "recover" => Method::Recover,
        other => return Err(format!("method {other}: repair or recover").into()),
    };
    let group = VirtualGroup(args.get("virtual-group")?.parse()?);
    let position: usize = args.get("position")?.parse()?;
    let fold = state.fold().ok_or("no live groups")?;
    let live_group = fold.live_group(group);
    println!(
        "epoch {}, live group {}, live spool {}, position {position}, me {}",
        state.epoch().0,
        live_group.0,
        live_group.spool_at(position).0,
        context.node_address()
    );

    let source = TapeStore::open_read_only(args.get("source")?)?;
    let ranges = fold.carried_ranges(live_group);
    let count: usize = args.get("tracks")?.parse()?;
    let tracks = read_tracks(&source, &ranges, count, true)?;
    println!("{} coded user tracks read", tracks.len());

    let jobs = build_jobs(&context, method, position, &tracks, profile, state.epoch())?;
    let widths = args.get_list("concurrency")?;
    let share = jobs.len() / widths.len().max(1);
    let mut offset = 0;
    for width in widths {
        let end = (offset + share).min(jobs.len());
        run_wave(&context, &jobs[offset..end], width, method).await;
        offset = end;
    }
    Ok(())
}

fn parse_args() -> Result<Args, Failure> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut values = HashMap::new();
    for pair in raw.chunks(2) {
        let [key, value] = pair else {
            return Err("arguments come in --key value pairs".into());
        };
        values.insert(key.trim_start_matches("--").to_string(), value.clone());
    }
    Ok(Args { values })
}

impl Args {
    fn get(&self, key: &str) -> Result<&String, Failure> {
        let missing = || format!("--{key} is required");
        Ok(self.values.get(key).ok_or_else(missing)?)
    }

    fn get_list(&self, key: &str) -> Result<Vec<usize>, Failure> {
        let mut list = Vec::new();
        for value in self.get(key)?.split(',') {
            list.push(value.parse()?);
        }
        Ok(list)
    }
}

/// A probe context with the chain's state and peers loaded
async fn build_live_probe(
    config: &NodeConfig,
) -> Result<(Arc<Probe>, ProtocolState), Failure> {
    let context = build_probe(config).await?;
    let state = fetch_state_with_commitment(&context.rpc, CommitmentLevel::Finalized).await?;
    context.set_state(state.clone())?;
    context.refresh_peers().await?;
    Ok((context, state))
}

/// A node context on a memory store, with the config's identity
async fn build_probe(config: &NodeConfig) -> Result<Arc<Probe>, Failure> {
    let keypair = config.load_node_keypair()?;
    let bls_keypair = config.load_bls_keypair()?;
    let tls_keypair = Arc::new(config.load_or_generate_tls_keypair()?);
    let rpc_config = RpcConfig {
        endpoints: config.solana.rpc.clone(),
        strategy: config.solana.rpc_strategy,
        ..RpcConfig::default()
    };
    let rpc = RpcClient::new(rpc_config)?;
    let peer_manager = Arc::new(PeerManager::new());
    let api = HttpApiBuilder::new()
        .local_identity(tls_keypair.clone())
        .get_slice_timeout(TRANSFER_CEILING)
        .build(peer_manager.clone())?;
    let store = TapeStore::new(MemoryStore::new());
    let atlas = Arc::new(AtlasBuffer::new(Vec::new()));
    let context = NodeContextBuilder::new(
        config.clone(),
        keypair,
        bls_keypair,
        tls_keypair,
        store,
        rpc,
        peer_manager,
        Arc::new(api),
        atlas,
    )
    .build()
    .await?;
    Ok(context)
}

/// Copy up to `count` tracks of the owned live spools into the probe store,
/// with their tapes, as a node that lost the spools holds them: coded tracks
/// keep their encodings, and inline tracks lack their payloads
fn seed_owned_spools<S: tape_store_api::Store>(
    source: &TapeStore<S>,
    store: &TapeStore<MemoryStore>,
    fold: GroupFold,
    spools: &[SpoolIndex],
    count: usize,
) -> Result<(usize, usize), Failure> {
    let mut ranges = Vec::new();
    for spool in spools {
        ranges.extend(fold.carried_ranges(GroupIndex::containing(*spool)));
    }
    let tracks = read_tracks(source, &ranges, count, false)?;
    let mut tapes = HashSet::new();
    let (mut coded, mut inline) = (0, 0);
    for (_, address, track, data) in tracks {
        if tapes.insert(track.tape) {
            let tape = source.get_tape(track.tape)?.ok_or("track without a tape")?;
            store.put_tape(track.tape, tape)?;
        }
        store.put_track(address, track)?;
        if track.is_inline() {
            inline += 1;
            continue;
        }
        store.put_track_data(address, data)?;
        coded += 1;
    }
    Ok((coded, inline))
}

/// Read the store's slice count every few seconds, then report the rates
async fn watch_progress(context: &Arc<Probe>, seconds: u64) -> Result<String, Failure> {
    let started = Instant::now();
    let mut lines = Vec::new();
    let mut last = (0u64, 0u64);
    let mut rates = Vec::new();
    while started.elapsed() < Duration::from_secs(seconds) {
        tokio::time::sleep(SAMPLE_EVERY).await;
        let (slices, bytes) = context.store.slice_totals()?;
        let bytes = bytes.map(|units| units.as_u64()).unwrap_or(0);
        let step = SAMPLE_EVERY.as_secs_f64();
        let rate = (slices - last.0) as f64 / step;
        rates.push(rate);
        lines.push(format!(
            "  {:>4.0} s: {slices} slices, {rate:.1} slices/s, {:.2} MB/s",
            started.elapsed().as_secs_f64(),
            (bytes - last.1) as f64 / 1e6 / step,
        ));
        last = (slices, bytes);
    }
    let total = started.elapsed().as_secs_f64();
    let lowest = rates.iter().copied().fold(f64::MAX, f64::min);
    let highest = rates.iter().copied().fold(0.0, f64::max);
    lines.push(format!(
        "  overall: {} slices, {:.2} slices/s (10 s low {lowest:.1}, high {highest:.1}), \
         {:.2} MB/s of slices written",
        last.0,
        last.0 as f64 / total,
        last.1 as f64 / 1e6 / total
    ));
    Ok(lines.join("\n"))
}

/// Up to `count` certified tracks of the live group's virtual groups, off
/// the snapshot tapes; `is_coded_only` leaves out inline tracks
fn read_tracks<S: tape_store_api::Store>(
    source: &TapeStore<S>,
    ranges: &[std::ops::Range<VirtualGroup>],
    count: usize,
    is_coded_only: bool,
) -> Result<Vec<SourceTrack>, Failure> {
    let mut picked = Vec::new();
    let mut after = None;
    while picked.len() < count {
        let page = source.get_group_tracks(ranges, after, 4_096)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(*last);
        let addresses: Vec<Address> = page.iter().map(|(_, address)| *address).collect();
        let tracks = source.get_tracks(&addresses)?;
        let datas = source.get_track_datas(&addresses)?;
        for (((group, address), track), data) in page.iter().zip(tracks).zip(datas) {
            let (Some(track), Some(data)) = (track, data) else {
                continue;
            };
            let is_system = track.tape_number.0 & SYSTEM_TAPE_BIT != 0;
            let is_left_out = is_coded_only && !track.is_coded();
            if !track.is_certified() || is_system || is_left_out {
                continue;
            }
            picked.push((*group, *address, track, data));
            if picked.len() == count {
                break;
            }
        }
    }
    Ok(picked)
}

/// One fetch per track, with the peers the spool manager picks on fresh
/// peer scores
fn build_jobs(
    context: &Arc<Probe>,
    method: Method,
    position: usize,
    tracks: &[SourceTrack],
    profile: ErasureCode,
    epoch: EpochNumber,
) -> Result<Vec<Job>, Failure> {
    let limits = SpoolLimits::default();
    let me = context.node_address();
    let scores = PeerScores::default();
    let now = WallTime::now();
    let mut peers_by_group: HashMap<VirtualGroup, Vec<PositionPeer>> = HashMap::new();
    let mut jobs = Vec::with_capacity(tracks.len());
    for (group, address, track, data) in tracks {
        let BlobData::Coded(encoding) = data else {
            continue;
        };
        if !peers_by_group.contains_key(group) {
            let history = load_owner_history(&context.store, epoch, *group, profile, &limits)?;
            peers_by_group.insert(*group, get_group_peers(&history, position, me));
        }
        let group_peers = &peers_by_group[group];
        let missing = MissingTrack::new_handover(group.spool_at(position), *address, now);
        let attempt = match method {
            Method::Repair => can_repair(&missing, group_peers, &scores, profile, now, &limits)
                .unwrap_or(NextAttempt::NotNow),
            _ => can_recover(&missing, group_peers, &scores, profile, now, &limits),
        };
        let peers = match attempt {
            NextAttempt::Repair { helpers } => helpers,
            NextAttempt::Recover { peers } => peers,
            other => {
                println!("{address}: no peers ({other:?})");
                continue;
            }
        };
        let slice = coded_slice_len(
            profile,
            encoding.size.as_usize(),
            encoding.stripe_size.as_usize(),
            encoding.stripe_count.as_usize(),
        );
        let bytes = get_track_bytes(method, profile, track, Some(StorageUnits(slice as u64)));
        let track = FetchTrack {
            missing_track: missing,
            track: *track,
            encoding: Some(*encoding),
            holder_epoch: EpochNumber(0),
        };
        jobs.push(Job { track, peers, bytes });
    }
    Ok(jobs)
}

/// Run a wave of fetches `width` at a time and report it
async fn run_wave(context: &Arc<Probe>, wave: &[Job], width: usize, method: Method) {
    let limits = SpoolLimits::default();
    let started = Instant::now();
    let runs = wave.iter().map(|job| {
        let context = Arc::clone(context);
        let mut req = FetchReq::new(method, job.track.missing_track.spool, 0, job.peers.clone());
        req.add_track(job.track.clone(), job.bytes);
        let limit = get_time_limit(job.bytes, &limits);
        async move {
            let began = Instant::now();
            let res = match method {
                Method::Repair => repair::run(Arc::clone(&context), req, limit).await,
                _ => recover::run(Arc::clone(&context), req, limit).await,
            };
            let missing = job.track.missing_track;
            let _ = context.store.delete_slice(missing.spool, missing.track);
            Sample { elapsed: began.elapsed(), res }
        }
    });
    let samples: Vec<Sample> = stream::iter(runs).buffer_unordered(width).collect().await;
    report_wave(&samples, width, started.elapsed());
}

fn report_wave(samples: &[Sample], width: usize, wall: Duration) {
    let mut fetch_times: Vec<Duration> = samples.iter().map(|sample| sample.elapsed).collect();
    fetch_times.sort();
    let mut outcomes: HashMap<String, usize> = HashMap::new();
    let mut peer_failures: HashMap<String, usize> = HashMap::new();
    let mut bytes = 0u64;
    for sample in samples {
        for (_, outcome) in &sample.res.tracks {
            let label = match outcome {
                Ok(()) => "got it".to_string(),
                Err(failure) => format!("{failure:?}"),
            };
            *outcomes.entry(label).or_default() += 1;
        }
        for (_, outcome) in &sample.res.peers {
            match outcome {
                PeerOutcome::Worked { bytes: sent, .. } => bytes += sent.as_u64(),
                other => *peer_failures.entry(format!("{other:?}")).or_default() += 1,
            }
        }
    }
    let seconds = wall.as_secs_f64();
    println!(
        "\nconcurrency {width}: {} fetches in {seconds:.1} s, {:.2} tracks/s, {:.2} MB/s",
        samples.len(),
        samples.len() as f64 / seconds,
        bytes as f64 / 1e6 / seconds
    );
    println!(
        "  fetch time p50 {:?} p90 {:?} max {:?}",
        get_percentile(&fetch_times, 50),
        get_percentile(&fetch_times, 90),
        fetch_times.last().copied().unwrap_or_default()
    );
    println!("  tracks {outcomes:?}");
    println!("  peer failures {peer_failures:?}");
}

fn get_percentile(sorted: &[Duration], percent: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = (sorted.len() * percent / 100).min(sorted.len() - 1);
    sorted[index]
}
