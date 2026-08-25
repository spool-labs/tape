//! Pushes board updates to connected dashboards as server-sent events
//!
//! The node differences the cumulative counters once and sends only what moved,
//! rather than every viewer re-fetching the whole board and differencing it.
//! The board and the topology go out on their own slower clocks.

use std::convert::Infallible;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    Counters, Gauges, Hello, Tick, BACKFILL_SPAN_MS, BACKFILL_STEP_MS, BOARD_PERIOD_MS,
    EVENT_BACKFILL, EVENT_BOARD, EVENT_HELLO, EVENT_ROUND, EVENT_TICK, EVENT_TOPOLOGY,
    RoundTrace, SPOOL_OPS,
    TOPOLOGY_PERIOD_MS,
    STREAM_PROTOCOL, TICK_PERIOD_MS,
};
use tape_protocol::Api;

use super::{bandwidth, board};
use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Frames retained for a subscriber that falls behind before it is dropped
const BACKLOG: usize = 64;

/// Window each tick's rates are averaged over
///
/// Differencing a single sampling interval aliases at low rates: one request a
/// second lands in one bucket and reads as four, with zeroes after it. The
/// window also sets the resolution of a whole-number counter.
const RATE_WINDOW_MS: u64 = 2_000;

/// One encoded event, ready to write to any number of sockets
#[derive(Clone)]
pub struct Frame {
    pub event: &'static str,
    pub data: String,
}

impl Frame {
    fn new<T: serde::Serialize>(event: &'static str, value: &T) -> Option<Self> {
        serde_json::to_string(value).ok().map(|data| Frame { event, data })
    }

    /// The frame as one named event, handing over the JSON it already holds
    fn into_event(self) -> Event {
        Event::default().event(self.event).data(self.data)
    }
}

/// Ticks kept for the connect backfill, thinned to the backfill step
const BACKFILL_LEN: usize = (BACKFILL_SPAN_MS / BACKFILL_STEP_MS) as usize;

/// What a fresh connection needs before the live stream makes sense
#[derive(Default)]
struct Replay {
    hello: Option<Frame>,
    topology: Option<Frame>,
    board: Option<Frame>,
    history: std::collections::VecDeque<Tick>,
}

/// Fan-out to connected dashboards
pub struct StreamHub {
    tx: broadcast::Sender<Frame>,
    replay: Mutex<Replay>,
}

impl StreamHub {
    fn new() -> Self {
        Self { tx: broadcast::channel(BACKLOG).0, replay: Mutex::new(Replay::default()) }
    }

    /// Keep one tick for the connect backfill
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

    /// Fan out one frame, keeping the ones a fresh connection replays
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

    /// The replay frames plus a receiver for everything after
    ///
    /// The receiver is taken first so no frame slips between the two. A
    /// duplicate board on connect is harmless.
    pub fn subscribe(&self) -> (Vec<Frame>, broadcast::Receiver<Frame>) {
        let rx = self.tx.subscribe();
        let mut frames = Vec::new();
        if let Ok(replay) = self.replay.lock() {
            frames.extend(replay.hello.clone());
            frames.extend(replay.topology.clone());
            frames.extend(replay.board.clone());
            // Sampling stops once nobody has watched for a while, so anything
            // older than the span would draw as if it were current
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

    /// Whether anyone is listening, so an idle node can skip sampling
    fn has_subscribers(&self) -> bool {
        self.tx.receiver_count() > 0
    }
}

static HUB: OnceLock<Arc<StreamHub>> = OnceLock::new();

/// The process-wide stream hub
pub fn hub() -> &'static Arc<StreamHub> {
    HUB.get_or_init(|| Arc::new(StreamHub::new()))
}

/// Whether anyone is watching, so an unwatched node builds no frames
pub fn watching() -> bool {
    hub().has_subscribers()
}

/// Send one round trace out as it changes
///
/// A round's evidence all arrives within a few slots, well inside the board
/// period, so waiting for the next whole board would land it in one lump.
pub fn push_round(trace: &RoundTrace) {
    if let Some(frame) = Frame::new(EVENT_ROUND, trace) {
        hub().publish(frame);
    }
}

/// Serve the live stream
///
/// The hub is process-global, so the node and the gateway mount the same
/// handler. A reverse proxy in front of this must not buffer the response, or
/// frames arrive in batches.
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

/// One reading of every cumulative counter a tick is derived from
///
/// Deliberately free of store reads, so it stays cheap enough to take four
/// times a second.
struct Sample {
    taken: Instant,
    counters: Counters,
}

impl Sample {
    fn take(families: &[MetricFamily]) -> Self {
        let m = tape_metrics::metrics();
        let (decode_buckets, decode_latency_total) = board::decode_latency(families);
        let read = |vec: &tape_metrics::prometheus::IntCounterVec, label: &str| {
            vec.with_label_values(&[label]).get()
        };
        let spool = |stage: &str| -> Vec<u64> {
            SPOOL_OPS
                .iter()
                .map(|&op| m.spool_bytes_total.with_label_values(&[op, stage]).get())
                .collect()
        };
        let store = board::store_io_totals(families);
        Self {
            taken: Instant::now(),
            counters: Counters {
                http: board::serving_totals(families),
                peers: board::peer_totals(families),
                chain: board::chain_totals(families),
                store_ops: store.0,
                store_read: store.1,
                store_written: store.2,
                decode_buckets,
                decode_latency_total,
                decode_ok: read(&m.decode_total, "ok"),
                decode_failed: tape_observe_api::DECODE_FAILURES
                    .iter()
                    .map(|&f| read(&m.decode_total, f))
                    .sum(),
                cache_hits: read(&m.cache_requests_total, "hit"),
                cache_lookups: tape_observe_api::CACHE_RESULTS
                    .iter()
                    .map(|&r| read(&m.cache_requests_total, r))
                    .sum(),
                blocks: m.blocks_processed_total.get(),
                replay_events: m.replay_events_total.get(),
                spool_persisted: spool("persisted"),
                spool_fetched: spool("fetched"),
                // a float counter: truncating to whole seconds would quantise
                // every quarter-second diff to zero
                cpu_seconds: board::family_counter_f64(families, "process_cpu_seconds_total"),
            },
        }
    }
}

/// Gauges read straight off the node at the instant of a tick
fn gauges<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    families: &[MetricFamily],
) -> Gauges
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    let (tip_slot, dispatched_slot, lag_slots) = context.ingest.progress().tip_and_lag();
    let counters = &context.challenge_counters;
    Gauges {
        challenge: tape_observe_api::ChallengeRounds {
            opened: counters.opened.load(std::sync::atomic::Ordering::Relaxed),
            settled_certified: counters.settled_certified.load(std::sync::atomic::Ordering::Relaxed),
            settled_missed: counters.settled_missed.load(std::sync::atomic::Ordering::Relaxed),
            answers_refused: counters.answers_refused.load(std::sync::atomic::Ordering::Relaxed),
            voided: counters.voided.load(std::sync::atomic::Ordering::Relaxed),
            discarded: counters.discarded.load(std::sync::atomic::Ordering::Relaxed),
            own_certified: counters.own_certified.load(std::sync::atomic::Ordering::Relaxed),
            own_missed: counters.own_missed.load(std::sync::atomic::Ordering::Relaxed),
        },
        lag_slots: if context.bootstrap.is_ready() {
            lag_slots
        } else {
            let b = context.bootstrap.snapshot();
            b.target_slot.saturating_sub(b.current_slot)
        },
        tip_slot,
        dispatched_slot,
        rss_bytes: memory_stats::memory_stats().map(|m| m.physical_mem as u64).unwrap_or(0),
        queue_depth: board::family_gauge(families, "tape_node_channel_depth"),
    }
}

/// Samples counters into ticks and republishes the board and topology
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

        // Samples spanning RATE_WINDOW_MS; the front is what each tick is
        // differenced against.
        let mut history: std::collections::VecDeque<Sample> = std::collections::VecDeque::new();
        let mut since_board = Duration::ZERO;
        let mut since_topology = Duration::ZERO;
        let mut idle = Duration::ZERO;
        // epoch and committee size are what actually move topology, and both
        // are in memory; the full build is far too expensive to run per board
        let mut last_shape: Option<(u64, usize, usize)> = None;

        loop {
            select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    // The minute history is what a dashboard opens on, so it is
                    // kept whether or not anyone is watching now.
                    bandwidth::sample();

                    // An idle node does no work here. The dropped baseline means
                    // the first tick after a connect establishes it.
                    // Sampling continues unwatched only as long as the backfill
                    // needs it
                    if !hub.has_subscribers() && idle > Duration::from_millis(BACKFILL_SPAN_MS) {
                        history.clear();
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
                    // Drop samples past the rate window, so the front sits just
                    // outside it
                    while history.len() > 1
                        && sample.taken.duration_since(history[1].taken)
                            >= Duration::from_millis(RATE_WINDOW_MS)
                    {
                        history.pop_front();
                    }
                    if let Some(before) = history.front() {
                        let interval =
                            sample.taken.duration_since(before.taken).as_secs_f32();
                        let at_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let tick = tape_observe_api::diff(
                            &before.counters,
                            &sample.counters,
                            gauges(&self.context, &families),
                            at_ms,
                            interval,
                        );
                        hub.remember(&tick);
                        if let Some(frame) = Frame::new(EVENT_TICK, &tick) {
                            hub.publish(frame);
                        }
                    }
                    since_board += Duration::from_millis(TICK_PERIOD_MS);
                    history.push_back(sample);

                    if since_board >= Duration::from_millis(BOARD_PERIOD_MS) {
                        since_board = Duration::ZERO;
                        if let Some(frame) = Frame::new(EVENT_BOARD, &board::build(&self.context)) {
                            hub.publish(frame);
                        }
                        // The shape moves at an epoch boundary, but the peer
                        // stats inside the frame move with every aggregator
                        // probe, so the topology also repeats on its own clock.
                        let state = self.context.state();
                        let shape = (
                            state.epoch().0,
                            state.current.committee.len(),
                            state.current.groups.len(),
                        );
                        drop(state);
                        since_topology += Duration::from_millis(BOARD_PERIOD_MS);
                        if last_shape != Some(shape)
                            || since_topology >= Duration::from_millis(TOPOLOGY_PERIOD_MS)
                        {
                            last_shape = Some(shape);
                            since_topology = Duration::ZERO;
                            if let Some(frame) =
                                Frame::new(EVENT_TOPOLOGY, &board::build_network(&self.context))
                            {
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
    /// Publish the opening frame describing this producer
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
            address: context.node_address().to_string(),
        };
        if let Some(frame) = Frame::new(EVENT_HELLO, &hello) {
            self.publish(frame);
        }
    }
}
