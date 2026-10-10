//! How many chunks a stream write keeps in flight, decided from the machine and the link
//!
//! A stream starts with one chunk uploading and widens while each wider window
//! moves more bytes per second. Free memory caps the window and is read again
//! every tick, so a stream grows into memory the machine can spare and gives it
//! back when other work needs it. Every coded chunk holds a slot until its last
//! slice push ends, stragglers included, so a slow owner stalls the reader and
//! adds no memory.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tape_core::encoding::ErasureCode;
use tokio::sync::{Notify, oneshot, watch};
use tokio::time::{Instant, sleep};
use tracing::info;

use crate::stream::manifest::max_track_size;
use crate::track::write::UploadPlan;

/// Most chunks a stream window holds, whatever the machine
const MAX_WINDOW: usize = 16;

/// Chunks read, coded and registered ahead of the uploading window
const LEAD: usize = 3;

/// Slots a stream keeps however little memory is free, one chunk coding and one uploading
const MIN_SLOTS: usize = 2;

/// Share of the machine's memory left free for everything else, as a divisor
const HEADROOM_SHARE: u64 = 4;

/// Share of the machine's memory a stream holds where the system won't say what is free, as a divisor
const BLIND_SHARE: u64 = 8;

/// How often the pace reads the link
const PACE_TICK: Duration = Duration::from_secs(1);

/// A window runs at least this long before its rate counts
const PACE_SPAN: Duration = Duration::from_secs(8);

/// A settled window steps down once the link moves less than this share of its settled rate
const PACE_FALL: f64 = 0.7;

/// A wider window stays only when it moves this much more than the narrower one
const PACE_GAIN: f64 = 1.15;

/// How often a settled pace tries one chunk more, so a link that got faster gets used
const PACE_REPROBE: Duration = Duration::from_secs(60);

/// Coded bytes of one full stream chunk, its slices together
pub(crate) fn coded_chunk_bytes(profile: ErasureCode) -> u64 {
    max_track_size(profile) as u64 * u64::from(profile.n()) / u64::from(profile.k()).max(1)
}

/// Bytes one stream chunk holds, its plain and coded copies together
fn slot_bytes(profile: ErasureCode) -> u64 {
    max_track_size(profile) as u64 + coded_chunk_bytes(profile)
}

/// The machine's memory, in bytes
#[derive(Clone, Copy, Debug)]
pub(crate) struct Memory {
    pub(crate) total: u64,
    /// What the system can hand out now without swapping, or nothing when it won't say
    pub(crate) available: Option<u64>,
}

impl Memory {
    pub(crate) fn read() -> Self {
        Self {
            total: physical_memory(),
            available: available_memory(),
        }
    }
}

/// Slots a stream may hold: the ones it has plus what the machine can spare above its headroom
fn memory_slots(memory: Memory, held: usize, slot_bytes: u64) -> usize {
    let slots = match memory.available {
        Some(available) => {
            let spare = available as i128 - (memory.total / HEADROOM_SHARE) as i128;
            held as i128 + spare.div_euclid(slot_bytes.max(1) as i128)
        }
        None => (memory.total / BLIND_SHARE / slot_bytes.max(1)) as i128,
    };
    slots.clamp(MIN_SLOTS as i128, (MAX_WINDOW + LEAD) as i128) as usize
}

/// The widest window a slot limit leaves room for, its lead taken off
fn window_cap(slots: usize) -> usize {
    slots.saturating_sub(LEAD).clamp(1, MAX_WINDOW)
}

/// Bytes of RAM in the machine, or zero when the system will not say
fn physical_memory() -> u64 {
    // SAFETY: sysconf takes a constant name and reads no caller memory.
    let (pages, page_size) = unsafe { (libc::sysconf(libc::_SC_PHYS_PAGES), libc::sysconf(libc::_SC_PAGESIZE)) };
    match pages > 0 && page_size > 0 {
        true => pages as u64 * page_size as u64,
        false => 0,
    }
}

/// Bytes the kernel can hand out without swapping, from `MemAvailable`
#[cfg(any(target_os = "linux", target_os = "android"))]
fn available_memory() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|line| line.starts_with("MemAvailable:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

/// Bytes the kernel can hand out without memory pressure, from the share it reports free
#[cfg(target_vendor = "apple")]
fn available_memory() -> Option<u64> {
    let mut level: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is a nul terminated literal, and level is an int sized by len.
    let status = unsafe {
        libc::sysctlbyname(
            c"kern.memorystatus_level".as_ptr(),
            (&raw mut level).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    match status == 0 && (0..=100).contains(&level) {
        true => Some(physical_memory() / 100 * level as u64),
        false => None,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn available_memory() -> Option<u64> {
    None
}

/// Chunks alive in a stream, each from its read to the end of its last slice push
pub(crate) struct Slots {
    alive: AtomicUsize,
    /// Slots the machine's free memory allows, read again each pace tick
    limit: AtomicUsize,
    slot_bytes: u64,
    read_memory: Box<dyn Fn() -> Memory + Send + Sync>,
    window: watch::Receiver<usize>,
    freed: Notify,
}

impl Slots {
    pub(crate) fn new(window: watch::Receiver<usize>, profile: ErasureCode) -> Arc<Self> {
        Self::with_memory(window, slot_bytes(profile), Memory::read)
    }

    fn with_memory(
        window: watch::Receiver<usize>,
        slot_bytes: u64,
        read_memory: impl Fn() -> Memory + Send + Sync + 'static,
    ) -> Arc<Self> {
        let limit = memory_slots(read_memory(), 0, slot_bytes);
        Arc::new(Self {
            alive: AtomicUsize::new(0),
            limit: AtomicUsize::new(limit),
            slot_bytes,
            read_memory: Box::new(read_memory),
            window,
            freed: Notify::new(),
        })
    }

    /// Read free memory again and set the slot limit from it
    pub(crate) fn fit_memory(&self) -> usize {
        let held = self.alive.load(Ordering::Acquire);
        let limit = memory_slots((self.read_memory)(), held, self.slot_bytes);
        if self.limit.swap(limit, Ordering::AcqRel) < limit {
            self.freed.notify_waiters();
        }
        limit
    }

    /// Wait until the window, its lead and free memory have room for one more chunk
    pub(crate) async fn acquire(self: &Arc<Self>) -> Slot {
        let mut window = self.window.clone();
        let mut is_pacing = true;
        loop {
            let freed = self.freed.notified();
            let limit = (*window.borrow_and_update() + LEAD).min(self.limit.load(Ordering::Acquire));
            let alive = self.alive.load(Ordering::Acquire);
            if alive < limit
                && self
                    .alive
                    .compare_exchange(alive, alive + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return Slot { slots: self.clone() };
            }
            match is_pacing {
                true => tokio::select! {
                    () = freed => {}
                    // Safe: a new window only wakes the loop, which reads it again.
                    changed = window.changed() => is_pacing = changed.is_ok(),
                },
                false => freed.await,
            }
        }
    }
}

/// One chunk's hold on the stream's memory
pub(crate) struct Slot {
    slots: Arc<Slots>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.slots.alive.fetch_sub(1, Ordering::AcqRel);
        self.slots.freed.notify_waiters();
    }
}

/// A slice body that keeps its chunk's slot until the last push holding it ends
struct HeldBody {
    body: Bytes,
    _slot: Arc<Slot>,
}

impl AsRef<[u8]> for HeldBody {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

/// Tie every slice body of a coded chunk to its slot, so stragglers count against the stream
pub(crate) fn hold_slices(mut plan: UploadPlan, slot: Slot) -> UploadPlan {
    let slot = Arc::new(slot);
    for slice in &mut plan.slices {
        let body = HeldBody {
            body: slice.body.clone(),
            _slot: slot.clone(),
        };
        slice.body = Bytes::from_owner(body);
    }
    plan
}

/// Where the pace is in finding the window the link rewards
enum Phase {
    /// Each span tries one chunk more
    Probing,
    /// The window held; it tries one more again at the instant given
    Settled(Instant),
}

/// Widen the window while a wider one moves more bytes, hold it, and follow the link after
///
/// `accepted` reads the slice bytes nodes have taken from this client so far.
/// Pushes finish in bursts, so a span closes only once the nodes have taken a
/// window's worth of chunks, and the span after each change is left out while
/// the new window fills. A settled window steps down when the link falls away
/// and tries one chunk more every so often. Free memory caps the window on every
/// tick, and a window above it steps down at once. The pace ends when the store
/// stage finishes and drops `stop`.
pub(crate) async fn pace(
    window: watch::Sender<usize>,
    slots: Arc<Slots>,
    chunk_bytes: u64,
    accepted: impl Fn() -> u64,
    mut stop: oneshot::Receiver<()>,
) {
    let mut best = 1usize;
    let mut best_rate = 0f64;
    let mut settled_rate = 0f64;
    let mut phase = Phase::Probing;
    let mut is_warming = false;
    let mut span_start = Instant::now();
    let mut span_bytes = accepted();
    window.send_replace(1);

    loop {
        tokio::select! {
            // Safe: the stop only ends the loop.
            _ = &mut stop => return,
            () = sleep(PACE_TICK) => {}
        }
        let cap = window_cap(slots.fit_memory());
        let current = *window.borrow();
        if current > cap {
            info!(window = cap, was = current, cap, "stream window, memory is short");
            window.send_replace(cap);
            best = best.min(cap);
            // the rate falls with the window, so the next spans set a new baseline
            phase = Phase::Settled(Instant::now() + PACE_REPROBE);
            settled_rate = 0.0;
            span_start = Instant::now();
            span_bytes = accepted();
            is_warming = true;
            continue;
        }
        let elapsed = span_start.elapsed();
        let moved = accepted().saturating_sub(span_bytes);
        if elapsed < PACE_SPAN || moved < current as u64 * chunk_bytes {
            continue;
        }
        span_start = Instant::now();
        span_bytes = accepted();
        if is_warming {
            is_warming = false;
            continue;
        }
        let rate = moved as f64 / elapsed.as_secs_f64();

        let next = match phase {
            Phase::Probing if rate > best_rate * PACE_GAIN => {
                best = current;
                best_rate = rate;
                match current < cap {
                    true => current + 1,
                    false => {
                        phase = Phase::Settled(Instant::now() + PACE_REPROBE);
                        settled_rate = rate;
                        current
                    }
                }
            }
            Phase::Probing => {
                phase = Phase::Settled(Instant::now() + PACE_REPROBE);
                settled_rate = best_rate;
                best
            }
            Phase::Settled(_) if rate < settled_rate * PACE_FALL && current > 1 => {
                settled_rate = rate;
                current - 1
            }
            Phase::Settled(until) if Instant::now() >= until && current < cap => {
                phase = Phase::Probing;
                best = current;
                best_rate = rate;
                current + 1
            }
            Phase::Settled(_) => {
                settled_rate = settled_rate.max(rate);
                current
            }
        };

        if next != current {
            info!(
                window = next,
                was = current,
                mib_per_sec = format!("{:.1}", rate / (1024.0 * 1024.0)),
                cap,
                "stream window"
            );
            window.send_replace(next);
            is_warming = true;
        }
    }
}

/// Keep a pinned window's chunks inside free memory until the store stage ends
pub(crate) async fn guard_memory(slots: Arc<Slots>, mut stop: oneshot::Receiver<()>) {
    loop {
        tokio::select! {
            // Safe: the stop only ends the loop.
            _ = &mut stop => return,
            () = sleep(PACE_TICK) => {}
        }
        slots.fit_memory();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

    /// Coded bytes of one chunk in these tests
    const CHUNK: u64 = 1_000_000;

    /// Bytes one slot holds in these tests
    const SLOT: u64 = 1_000_000;

    /// Slots whose memory leaves room for a window of `cap`
    fn slots_for(receiver: watch::Receiver<usize>, cap: usize) -> Arc<Slots> {
        let total = (cap + LEAD) as u64 * SLOT * BLIND_SHARE;
        Slots::with_memory(receiver, SLOT, move || Memory { total, available: None })
    }

    /// Memory with a headroom of 4 GB and `free` slots spare above it
    fn spare(free: u64) -> Memory {
        let total = 16_000_000_000;
        Memory { total, available: Some(total / HEADROOM_SHARE + free * SLOT) }
    }

    // a stream grows into spare memory, gives it back when it runs short, and keeps two slots at worst
    #[test]
    fn slots_follow_free_memory() {
        assert_eq!(memory_slots(spare(100), 0, SLOT), MAX_WINDOW + LEAD);
        assert_eq!(memory_slots(spare(5), 0, SLOT), 5);
        assert_eq!(memory_slots(spare(5), 4, SLOT), 9);
        let short = Memory { total: 16_000_000_000, available: Some(16_000_000_000 / HEADROOM_SHARE - 3 * SLOT) };
        assert_eq!(memory_slots(short, 8, SLOT), 5);
        assert_eq!(memory_slots(Memory { total: 16_000_000_000, available: Some(0) }, 8, SLOT), MIN_SLOTS);
        assert_eq!(memory_slots(Memory { total: 80 * SLOT, available: None }, 0, SLOT), 10);
    }

    // the system reports free memory on the platforms a stream reads it from
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    #[test]
    fn free_memory_reads() {
        let memory = Memory::read();
        let available = memory.available.expect("free memory");
        assert!(available > 0 && available <= memory.total, "{memory:?}");
    }

    // a full slot limit holds the next chunk until free memory grows
    #[tokio::test]
    async fn slots_wait_for_memory() {
        let (_sender, receiver) = watch::channel(8);
        let free = Arc::new(AtomicU64::new(2));
        let reader = free.clone();
        let slots = Slots::with_memory(receiver, SLOT, move || spare(reader.load(Ordering::Relaxed)));
        let held = [slots.acquire().await, slots.acquire().await];
        let waiting = {
            let slots = slots.clone();
            tokio::spawn(async move { slots.acquire().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        free.store(1, Ordering::Relaxed);
        assert_eq!(slots.fit_memory(), 3);
        let slot = tokio::time::timeout(Duration::from_secs(1), waiting).await;
        assert!(matches!(slot, Ok(Ok(_))));
        drop(held);
    }

    // a window above what free memory allows steps down on the next tick
    #[tokio::test(start_paused = true)]
    async fn short_memory_steps_the_window_down() {
        let (sender, receiver) = watch::channel(1);
        let pushed = Arc::new(AtomicU64::new(0));
        let feed = {
            let pushed = pushed.clone();
            let receiver = receiver.clone();
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_millis(100)).await;
                    let window = *receiver.borrow() as u64;
                    pushed.fetch_add(window * CHUNK, Ordering::Relaxed);
                }
            })
        };
        let free = Arc::new(AtomicU64::new(6 + LEAD as u64));
        let reader = free.clone();
        let slots = Slots::with_memory(receiver.clone(), SLOT, move || spare(reader.load(Ordering::Relaxed)));
        let counter = pushed.clone();
        let (_stop, stopped) = oneshot::channel();
        let task = tokio::spawn(pace(sender, slots, CHUNK, move || counter.load(Ordering::Relaxed), stopped));

        sleep(PACE_SPAN * 12).await;
        assert!(*receiver.borrow() >= 4, "window {}", *receiver.borrow());
        free.store(2 + LEAD as u64, Ordering::Relaxed);
        sleep(PACE_TICK * 2).await;
        assert_eq!(*receiver.borrow(), 2);

        drop(receiver);
        feed.abort();
        task.abort();
    }

    // a link that falls away after the pace settles steps the window down
    #[tokio::test(start_paused = true)]
    async fn a_falling_link_steps_down() {
        let (sender, receiver) = watch::channel(1);
        let accepted = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let feed = {
            let accepted = accepted.clone();
            let receiver = receiver.clone();
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_millis(100)).await;
                    let window = (*receiver.borrow() as u64).min(3);
                    let share = match started.elapsed() < PACE_SPAN * 12 {
                        true => window,
                        false => 1,
                    };
                    accepted.fetch_add(share * CHUNK, Ordering::Relaxed);
                }
            })
        };
        let reader = accepted.clone();
        let (_stop, stopped) = oneshot::channel();
        let slots = slots_for(receiver.clone(), 8);
        let task = tokio::spawn(pace(sender, slots, CHUNK, move || reader.load(Ordering::Relaxed), stopped));

        sleep(PACE_SPAN * 11).await;
        assert_eq!(*receiver.borrow(), 3);
        sleep(PACE_SPAN * 8).await;
        assert!(*receiver.borrow() < 3, "window {}", *receiver.borrow());

        drop(receiver);
        feed.abort();
        task.abort();
    }

    // a link that moves more with every chunk widens the window up to the cap
    #[tokio::test(start_paused = true)]
    async fn a_link_that_scales_widens_to_the_cap() {
        let (sender, receiver) = watch::channel(1);
        let pushed = Arc::new(AtomicU64::new(0));
        let feed = {
            let pushed = pushed.clone();
            let receiver = receiver.clone();
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_millis(100)).await;
                    let window = *receiver.borrow() as u64;
                    pushed.fetch_add(window * CHUNK, Ordering::Relaxed);
                }
            })
        };
        let reader = pushed.clone();
        let (_stop, stopped) = oneshot::channel();
        let slots = slots_for(receiver.clone(), 4);
        let task = tokio::spawn(pace(sender, slots, CHUNK, move || reader.load(Ordering::Relaxed), stopped));

        sleep(PACE_SPAN * 12).await;

        assert_eq!(*receiver.borrow(), 4);
        drop(receiver);
        feed.abort();
        task.abort();
    }

    // a link that is already full keeps the window where it stopped paying
    #[tokio::test(start_paused = true)]
    async fn a_full_link_settles() {
        let (sender, receiver) = watch::channel(1);
        let pushed = Arc::new(AtomicU64::new(0));
        let feed = {
            let pushed = pushed.clone();
            let receiver = receiver.clone();
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_millis(100)).await;
                    let window = (*receiver.borrow() as u64).min(2);
                    pushed.fetch_add(window * CHUNK, Ordering::Relaxed);
                }
            })
        };
        let reader = pushed.clone();
        let (_stop, stopped) = oneshot::channel();
        let slots = slots_for(receiver.clone(), 8);
        let task = tokio::spawn(pace(sender, slots, CHUNK, move || reader.load(Ordering::Relaxed), stopped));

        sleep(PACE_SPAN * 12).await;

        assert_eq!(*receiver.borrow(), 2);
        drop(receiver);
        feed.abort();
        task.abort();
    }

    // a slot frees when the last body that holds it drops
    #[tokio::test]
    async fn a_slot_lives_as_long_as_its_bodies() {
        let (_sender, receiver) = watch::channel(0);
        let slots = slots_for(receiver, 1);
        let slot = slots.acquire().await;
        let held = Arc::new(slot);
        let body = Bytes::from_owner(HeldBody {
            body: Bytes::from_static(b"slice"),
            _slot: held.clone(),
        });
        drop(held);
        let straggler = body.clone();
        drop(body);
        assert_eq!(slots.alive.load(Ordering::Acquire), 1);

        drop(straggler);
        assert_eq!(slots.alive.load(Ordering::Acquire), 0);
    }
}
