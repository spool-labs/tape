//! How many chunks a stream write keeps in flight, decided from the machine and the link
//!
//! A stream starts with one chunk uploading and widens while each wider window
//! moves more bytes per second. The memory available at the start caps the
//! window, and every coded chunk holds a slot until its last slice push ends,
//! stragglers included, so a slow owner stalls the reader and adds no memory.

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

/// A stream holds at most this share of the machine's memory, as a divisor
const MEMORY_SHARE: u64 = 8;

/// How often the pace reads the link
const PACE_TICK: Duration = Duration::from_secs(1);

/// How long a window runs before its rate counts
const PACE_SPAN: Duration = Duration::from_secs(8);

/// A wider window stays only when it moves this much more than the narrower one
const PACE_GAIN: f64 = 1.15;

/// How often a settled pace tries one chunk more, so a link that got faster gets used
const PACE_REPROBE: Duration = Duration::from_secs(60);

/// The widest window the machine's memory allows
pub(crate) fn window_cap(profile: ErasureCode) -> usize {
    let chunk = max_track_size(profile) as u64;
    let coded = chunk * u64::from(profile.n()) / u64::from(profile.k()).max(1);
    let budget = physical_memory() / MEMORY_SHARE;
    let chunks = (budget / (chunk + coded)) as usize;
    chunks.saturating_sub(LEAD).clamp(1, MAX_WINDOW)
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

/// Chunks alive in a stream, each from its read to the end of its last slice push
pub(crate) struct Slots {
    alive: AtomicUsize,
    window: watch::Receiver<usize>,
    freed: Notify,
}

impl Slots {
    pub(crate) fn new(window: watch::Receiver<usize>) -> Arc<Self> {
        Arc::new(Self {
            alive: AtomicUsize::new(0),
            window,
            freed: Notify::new(),
        })
    }

    /// Wait until the window and its lead have room for one more chunk
    pub(crate) async fn acquire(self: &Arc<Self>) -> Slot {
        let mut window = self.window.clone();
        let mut is_pacing = true;
        loop {
            let freed = self.freed.notified();
            let limit = *window.borrow_and_update() + LEAD;
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

/// Widen the window while a wider one moves more bytes, then hold it and try again now and then
///
/// `pushed` reads the bytes the client has handed to the network so far. The
/// pace ends when the store stage finishes and drops `stop`.
pub(crate) async fn pace(
    window: watch::Sender<usize>,
    cap: usize,
    pushed: impl Fn() -> u64,
    mut stop: oneshot::Receiver<()>,
) {
    let mut best = 1usize;
    let mut best_rate = 0f64;
    let mut phase = Phase::Probing;
    let mut span_start = Instant::now();
    let mut span_bytes = pushed();
    window.send_replace(1);

    loop {
        tokio::select! {
            // Safe: the stop only ends the loop.
            _ = &mut stop => return,
            () = sleep(PACE_TICK) => {}
        }
        let elapsed = span_start.elapsed();
        if elapsed < PACE_SPAN {
            continue;
        }
        let moved = pushed().saturating_sub(span_bytes);
        let rate = moved as f64 / elapsed.as_secs_f64();
        let current = *window.borrow();

        let next = match phase {
            Phase::Probing if rate > best_rate * PACE_GAIN && rate > 0.0 => {
                best = current;
                best_rate = rate;
                match current < cap {
                    true => current + 1,
                    false => {
                        phase = Phase::Settled(Instant::now() + PACE_REPROBE);
                        current
                    }
                }
            }
            Phase::Probing => {
                phase = Phase::Settled(Instant::now() + PACE_REPROBE);
                best
            }
            Phase::Settled(until) => {
                best_rate = rate;
                match Instant::now() >= until && current < cap {
                    true => {
                        phase = Phase::Probing;
                        current + 1
                    }
                    false => current,
                }
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
        }
        span_start = Instant::now();
        span_bytes = pushed();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

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
                    pushed.fetch_add(window * 1_000_000, Ordering::Relaxed);
                }
            })
        };
        let reader = pushed.clone();
        let (_stop, stopped) = oneshot::channel();
        let task = tokio::spawn(pace(sender, 4, move || reader.load(Ordering::Relaxed), stopped));

        sleep(PACE_SPAN * 6).await;

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
                    pushed.fetch_add(window * 1_000_000, Ordering::Relaxed);
                }
            })
        };
        let reader = pushed.clone();
        let (_stop, stopped) = oneshot::channel();
        let task = tokio::spawn(pace(sender, 8, move || reader.load(Ordering::Relaxed), stopped));

        sleep(PACE_SPAN * 6).await;

        assert_eq!(*receiver.borrow(), 2);
        drop(receiver);
        feed.abort();
        task.abort();
    }

    // a slot frees when the last body that holds it drops
    #[tokio::test]
    async fn a_slot_lives_as_long_as_its_bodies() {
        let (_sender, receiver) = watch::channel(0);
        let slots = Slots::new(receiver);
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
