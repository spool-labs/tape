//! Pushes board updates to connected dashboards as server-sent events.
//!
//! The board endpoint is cumulative, so every viewer used to re-fetch the whole
//! thing once a second and difference it locally. Here the node does the
//! differencing once, at a higher cadence than any viewer polled, and sends only
//! the numbers that moved. The full board and the network topology still go out,
//! but on their own much slower clocks, because almost nothing on them changes
//! between epoch boundaries.

use std::convert::Infallible;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::{Stream, StreamExt};
use rpc::Rpc;
use store::Store;
use tokio::select;
use tokio::sync::broadcast;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tape_metrics::prometheus::proto::MetricFamily;
use tape_observe_api::{
    Board, Bucket, ChainStats, Hello, HttpStats, StoreIo, Tick, BACKFILL_SPAN_MS,
    BACKFILL_STEP_MS, BOARD_PERIOD_MS, EVENT_BACKFILL, EVENT_BOARD, EVENT_HELLO, EVENT_TICK,
    EVENT_TOPOLOGY, SPOOL_OPS, STREAM_PROTOCOL, TICK_PERIOD_MS,
};
use tape_protocol::Api;

use super::board;
use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Frames retained for a subscriber that falls behind before it is dropped.
const BACKLOG: usize = 64;

/// One encoded event, ready to write to any number of sockets.
#[derive(Clone)]
pub struct Frame {
    pub event: &'static str,
    pub data: Bytes,
}

impl Frame {
    fn new<T: serde::Serialize>(event: &'static str, value: &T) -> Option<Self> {
        serde_json::to_vec(value).ok().map(|data| Frame { event, data: Bytes::from(data) })
    }

    /// The frame as one named SSE event. The payload is already JSON, so it goes
    /// out as-is rather than being re-serialized.
    fn into_event(self) -> Event {
        Event::default()
            .event(self.event)
            .data(String::from_utf8_lossy(&self.data).into_owned())
    }
}

/// Ticks kept for the connect backfill: one per [`BACKFILL_STEP_MS`] over
/// [`BACKFILL_SPAN_MS`].
const BACKFILL_LEN: usize = (BACKFILL_SPAN_MS / BACKFILL_STEP_MS) as usize;

/// What a fresh connection needs before the live stream makes sense: who is
/// talking, the current topology, one full board, and enough recent history to
/// fill the charts.
#[derive(Default)]
struct Replay {
    hello: Option<Frame>,
    topology: Option<Frame>,
    board: Option<Frame>,
    history: std::collections::VecDeque<Tick>,
}

/// Fan-out to connected dashboards.
pub struct StreamHub {
    tx: broadcast::Sender<Frame>,
    replay: Mutex<Replay>,
}

impl StreamHub {
    fn new() -> Self {
        Self { tx: broadcast::channel(BACKLOG).0, replay: Mutex::new(Replay::default()) }
    }

    /// Keep one tick for the connect backfill, thinned to the backfill step.
    fn remember(&self, tick: &Tick) {
        let Ok(mut replay) = self.replay.lock() else { return };
        let is_due = replay
            .history
            .back()
            .map(|last| tick.at_ms.saturating_sub(last.at_ms) >= BACKFILL_STEP_MS)
            .unwrap_or(true);
        if !is_due {
            return;
        }
        replay.history.push_back(tick.clone());
        while replay.history.len() > BACKFILL_LEN {
            replay.history.pop_front();
        }
    }

    /// Fan out one frame, keeping the ones a fresh connection needs replayed.
    fn publish(&self, frame: Frame) {
        if let Ok(mut replay) = self.replay.lock() {
            match frame.event {
                EVENT_HELLO => replay.hello = Some(frame.clone()),
                EVENT_TOPOLOGY => replay.topology = Some(frame.clone()),
                EVENT_BOARD => replay.board = Some(frame.clone()),
                _ => {}
            }
        }
        // send only fails with no subscribers, which is the normal idle case
        let _ = self.tx.send(frame);
    }

    /// The replay frames plus a receiver for everything after. The receiver is
    /// taken first so no frame slips between the two, at the cost of a possible
    /// duplicate board on connect, which is harmless.
    pub fn subscribe(&self) -> (Vec<Frame>, broadcast::Receiver<Frame>) {
        let rx = self.tx.subscribe();
        let mut frames = Vec::new();
        if let Ok(replay) = self.replay.lock() {
            frames.extend(replay.hello.clone());
            frames.extend(replay.topology.clone());
            frames.extend(replay.board.clone());
            // Sampling stops once nobody has watched for a while, which leaves
            // the ring holding whatever was current then. Sending that would
            // draw a full window of stale readings as if they were live.
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let history: Vec<&Tick> = replay
                .history
                .iter()
                .filter(|tick| now_ms.saturating_sub(tick.at_ms) <= BACKFILL_SPAN_MS)
                .collect();
            if !history.is_empty() {
                frames.extend(Frame::new(EVENT_BACKFILL, &history));
            }
        }
        (frames, rx)
    }

    /// Whether anyone is listening. Sampling is skipped when nobody is.
    fn has_subscribers(&self) -> bool {
        self.tx.receiver_count() > 0
    }
}

static HUB: OnceLock<Arc<StreamHub>> = OnceLock::new();

/// The process-wide stream hub.
pub fn hub() -> &'static Arc<StreamHub> {
    HUB.get_or_init(|| Arc::new(StreamHub::new()))
}

/// Serve the live stream. The hub is process-global, so this handler needs no
/// state and both the node and the gateway mount the same function.
///
/// A reverse proxy in front of this must not buffer the response. nginx needs
/// `proxy_buffering off` on the location, or frames arrive in batches and the
/// stream behaves worse than the polling it replaced.
pub async fn sse() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (replay, rx) = hub().subscribe();
    let opening = futures::stream::iter(replay.into_iter().map(|f| Ok(f.into_event())));
    let live = futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(frame) => return Some((Ok(frame.into_event()), rx)),
                // A viewer whose socket stalled misses frames rather than
                // holding the sampler up; the next tick puts it back in step.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(opening.chain(live)).keep_alive(KeepAlive::default())
}

/// One reading of every cumulative counter a tick is derived from. Deliberately
/// free of store reads: the whole point of the tick is that it is cheap enough
/// to take four times a second.
struct Sample {
    taken: Instant,
    http: HttpStats,
    peers: HttpStats,
    chain: ChainStats,
    store: StoreIo,
    decode_buckets: Vec<Bucket>,
    decode_latency_total: u64,
    decode_ok: u64,
    decode_failed: u64,
    cache_hits: u64,
    cache_lookups: u64,
    blocks: u64,
    replay_events: u64,
    spool_persisted: Vec<u64>,
    spool_fetched: Vec<u64>,
    cpu_seconds: f64,
    queue_depth: u64,
}

impl Sample {
    fn take(families: &[MetricFamily]) -> Self {
        let m = tape_metrics::metrics();
        let (decode_buckets, decode_latency_total) = board::decode_latency(families);
        let read = |vec: &tape_metrics::prometheus::IntCounterVec, label: &str| {
            vec.with_label_values(&[label]).get()
        };
        let decode_ok = read(&m.decode_total, "ok");
        let decode_failed: u64 = tape_observe_api::DECODE_FAILURES
            .iter()
            .map(|&f| read(&m.decode_total, f))
            .sum();
        let cache_hits = read(&m.cache_requests_total, "hit");
        let cache_lookups: u64 = tape_observe_api::CACHE_RESULTS
            .iter()
            .map(|&r| read(&m.cache_requests_total, r))
            .sum();
        let spool = |stage: &str| -> Vec<u64> {
            SPOOL_OPS
                .iter()
                .map(|&op| m.spool_bytes_total.with_label_values(&[op, stage]).get())
                .collect()
        };
        Self {
            taken: Instant::now(),
            http: board::http_stats(families),
            peers: board::peer_stats(families),
            chain: board::chain_stats(families),
            store: board::store_io_stats(families),
            decode_buckets,
            decode_latency_total,
            decode_ok,
            decode_failed,
            cache_hits,
            cache_lookups,
            blocks: m.blocks_processed_total.get(),
            replay_events: m.replay_events_total.get(),
            spool_persisted: spool("persisted"),
            spool_fetched: spool("fetched"),
            // a float counter, so it needs the full-precision sum: truncating to
            // whole seconds would quantise every quarter-second diff to zero
            cpu_seconds: board::family_counter_f64(families, "process_cpu_seconds_total"),
            queue_depth: board::family_gauge(families, "tape_node_channel_depth"),
        }
    }
}

/// Per-second rate between two cumulative readings.
fn rate(now: u64, before: u64, interval: f32) -> f32 {
    now.saturating_sub(before) as f32 / interval
}

/// Quantile in milliseconds over the requests that landed between two readings.
/// A window with no requests has no percentile to report and reads as zero.
fn quantile_ms(now: &[Bucket], before: &[Bucket], count: u64, q: f64) -> f32 {
    if count == 0 {
        return 0.0;
    }
    let delta = HttpStats::bucket_delta(now, before);
    (HttpStats::quantile(&delta, count, q) * 1000.0) as f32
}

/// Requests by status class that count as errors on the serving path.
fn serving_errors(stats: &HttpStats) -> u64 {
    Board::lookup(&stats.by_status, "4xx") + Board::lookup(&stats.by_status, "5xx")
}

/// Peer-client statuses of 400 and above.
fn peer_errors(stats: &HttpStats) -> u64 {
    stats
        .by_status
        .iter()
        .filter(|l| l.label.parse::<u16>().map(|c| c >= 400).unwrap_or(false))
        .map(|l| l.value)
        .sum()
}

/// Difference two samples into the frame the dashboard plots.
fn diff<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    before: &Sample,
    now: &Sample,
) -> Tick
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    let interval = now.taken.duration_since(before.taken).as_secs_f32().max(1e-3);
    let (tip_slot, dispatched_slot, lag_slots) = context.ingest.progress().tip_and_lag();
    let http_count = now.http.total.saturating_sub(before.http.total);
    let peer_count = now.peers.total.saturating_sub(before.peers.total);
    let decode_count = now.decode_latency_total.saturating_sub(before.decode_latency_total);
    let cache_lookups = now.cache_lookups.saturating_sub(before.cache_lookups);
    let cache_hits = now.cache_hits.saturating_sub(before.cache_hits);
    let per_op = |now: &[u64], before: &[u64]| -> Vec<f32> {
        now.iter()
            .zip(before)
            .map(|(n, b)| rate(*n, *b, interval))
            .collect()
    };

    Tick {
        at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        interval_secs: interval,

        req_per_s: rate(now.http.total, before.http.total, interval),
        egress_per_s: rate(now.http.response_bytes, before.http.response_bytes, interval),
        err_per_s: rate(serving_errors(&now.http), serving_errors(&before.http), interval),
        serving_p50_ms: quantile_ms(&now.http.buckets, &before.http.buckets, http_count, 0.50),
        serving_p95_ms: quantile_ms(&now.http.buckets, &before.http.buckets, http_count, 0.95),
        serving_p99_ms: quantile_ms(&now.http.buckets, &before.http.buckets, http_count, 0.99),

        peer_req_per_s: rate(now.peers.total, before.peers.total, interval),
        peer_ingress_per_s: rate(now.peers.response_bytes, before.peers.response_bytes, interval),
        peer_err_per_s: rate(peer_errors(&now.peers), peer_errors(&before.peers), interval),
        peer_p50_ms: quantile_ms(&now.peers.buckets, &before.peers.buckets, peer_count, 0.50),
        peer_p95_ms: quantile_ms(&now.peers.buckets, &before.peers.buckets, peer_count, 0.95),
        peer_p99_ms: quantile_ms(&now.peers.buckets, &before.peers.buckets, peer_count, 0.99),

        store_ops_per_s: rate(now.store.total_ops, before.store.total_ops, interval),
        store_read_per_s: rate(now.store.bytes_read, before.store.bytes_read, interval),
        store_write_per_s: rate(now.store.bytes_written, before.store.bytes_written, interval),

        rpc_per_s: rate(now.chain.rpc_total, before.chain.rpc_total, interval),
        rpc_err_per_s: rate(now.chain.rpc_errors, before.chain.rpc_errors, interval),
        tx_per_s: rate(now.chain.tx_total, before.chain.tx_total, interval),
        tx_err_per_s: rate(now.chain.tx_errors, before.chain.tx_errors, interval),

        blocks_per_s: rate(now.blocks, before.blocks, interval),
        replay_per_s: rate(now.replay_events, before.replay_events, interval),
        lag_slots: if context.bootstrap.is_ready() {
            lag_slots
        } else {
            let b = context.bootstrap.snapshot();
            b.target_slot.saturating_sub(b.current_slot)
        },
        tip_slot,
        dispatched_slot,

        // cpu_seconds is per-core-second, so the rate is a core fraction
        cpu_pct: ((now.cpu_seconds - before.cpu_seconds) / interval as f64 * 100.0).max(0.0) as f32,
        rss_bytes: memory_stats::memory_stats().map(|m| m.physical_mem as u64).unwrap_or(0),
        queue_depth: now.queue_depth,

        decode_per_s: rate(
            now.decode_ok + now.decode_failed,
            before.decode_ok + before.decode_failed,
            interval,
        ),
        decode_fail_per_s: rate(now.decode_failed, before.decode_failed, interval),
        decode_p50_ms: quantile_ms(
            &now.decode_buckets,
            &before.decode_buckets,
            decode_count,
            0.50,
        ),
        decode_p95_ms: quantile_ms(
            &now.decode_buckets,
            &before.decode_buckets,
            decode_count,
            0.95,
        ),
        cache_hit_pct: if cache_lookups == 0 {
            0.0
        } else {
            cache_hits as f32 / cache_lookups as f32 * 100.0
        },

        spool_persisted_per_s: per_op(&now.spool_persisted, &before.spool_persisted),
        spool_fetched_per_s: per_op(&now.spool_fetched, &before.spool_fetched),
    }
}

/// Background service that samples counters into ticks and republishes the
/// board and topology on their slower clocks.
pub struct StreamPublisher<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
}

impl<Db, Cluster, Blockchain> StreamPublisher<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        cancel: CancellationToken,
    ) -> Self {
        Self { context, cancel }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let mut ticker = interval(Duration::from_millis(TICK_PERIOD_MS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let hub = hub().clone();
        hub.publish_hello(&self.context);

        let mut previous: Option<Sample> = None;
        let mut since_board = Duration::ZERO;
        let mut idle = Duration::ZERO;
        let mut last_topology: Option<Bytes> = None;

        loop {
            select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    // An idle node with no dashboard open does no work here. The
                    // dropped baseline means the first tick after someone
                    // connects establishes it and the second carries real rates.
                    // Sampling continues with nobody watching only for as long as
                    // the backfill needs; past that an idle node does no work.
                    if !hub.has_subscribers() && idle > Duration::from_millis(BACKFILL_SPAN_MS) {
                        previous = None;
                        since_board = Duration::ZERO;
                        continue;
                    }
                    if hub.has_subscribers() {
                        idle = Duration::ZERO;
                    } else {
                        idle += Duration::from_millis(TICK_PERIOD_MS);
                    }

                    let families = tape_metrics::prometheus::gather();
                    let sample = Sample::take(&families);
                    if let Some(before) = previous.as_ref() {
                        let tick = diff(&self.context, before, &sample);
                        hub.remember(&tick);
                        if let Some(frame) = Frame::new(EVENT_TICK, &tick) {
                            hub.publish(frame);
                        }
                    }
                    since_board += Duration::from_millis(TICK_PERIOD_MS);
                    previous = Some(sample);

                    if since_board >= Duration::from_millis(BOARD_PERIOD_MS) {
                        since_board = Duration::ZERO;
                        if let Some(frame) = Frame::new(EVENT_BOARD, &board::build(&self.context)) {
                            hub.publish(frame);
                        }
                        // Topology is committee and spool ownership, which only
                        // move at an epoch boundary, so it goes out on change.
                        if let Some(frame) =
                            Frame::new(EVENT_TOPOLOGY, &board::build_network(&self.context))
                        {
                            if last_topology.as_ref() != Some(&frame.data) {
                                last_topology = Some(frame.data.clone());
                                hub.publish(frame);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl StreamHub {
    /// Publish the opening frame describing this producer.
    fn publish_hello<Db, Cluster, Blockchain>(
        &self,
        context: &NodeContext<Db, Cluster, Blockchain>,
    ) where
        Db: Store + 'static,
        Cluster: Api,
        Blockchain: Rpc,
    {
        let hello = Hello {
            protocol: STREAM_PROTOCOL,
            tick_ms: TICK_PERIOD_MS,
            board_ms: BOARD_PERIOD_MS,
            kind: super::board_kind(),
            address: context.node_address().to_string(),
        };
        if let Some(frame) = Frame::new(EVENT_HELLO, &hello) {
            self.publish(frame);
        }
    }
}
