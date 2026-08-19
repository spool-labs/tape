//! The backend a node's volume actually opened on, not the one it asked for
//!
//! `select_backend` downgrades a configured ring to posix with one warning and
//! serves the volume anyway, so a node can ask for the ring and run its whole
//! life on the fallback. Neither the config nor a shape assertion notices, since
//! both of those restate the request. Until the engine reports the backend it
//! took, that warning is the only place the outcome is stated, so this test
//! holds a run to it.

use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::subscriber::with_default;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// Whatever else the downgrade says, it says this
const DOWNGRADE: &str = "ring is unavailable";

/// Warnings a run emitted, kept so a test can hold the run to them
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl Warnings {
    /// Every warning recorded so far
    fn lines(&self) -> Vec<String> {
        self.0.lock().expect("warnings").clone()
    }
}

impl<S: Subscriber> Layer<S> for Warnings {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != Level::WARN {
            return;
        }
        let mut message = String::new();
        event.record(&mut Message(&mut message));
        self.0.lock().expect("warnings").push(message);
    }
}

/// One event's message field, as a string
struct Message<'a>(&'a mut String);

impl Visit for Message<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0.push_str(&format!("{value:?}"));
        }
    }
}

// a node that ships with the ring opens on one, rather than on the fallback
//
// Linux only: everywhere else there is no ring to be had and the downgrade is
// the right answer, so asserting it elsewhere would be a platform check wearing
// a correctness test's name. A red here on linux means this machine would not
// give the node the backend it ships with, which is worth failing over: a
// container needs `--security-opt seccomp=unconfined` before it can, and a
// kernel with `io_uring_disabled` set never will.
#[cfg(target_os = "linux")]
#[test]
fn the_ring_is_not_silently_downgraded() {
    let dir = tempfile::TempDir::new().expect("dir");
    let warnings = Warnings::default();

    with_default(tracing_subscriber::registry().with(warnings.clone()), || {
        reel_store::open_node_store(
            dir.path().join("volume"),
            0,
            reel_store::DEFAULT_SYNC_BYTES,
            reel::IoBackend::Uring,
        )
        .expect("open the node store");
    });

    let downgraded: Vec<String> = warnings
        .lines()
        .into_iter()
        .filter(|line| line.contains(DOWNGRADE))
        .collect();

    assert!(
        downgraded.is_empty(),
        "a volume configured for the ring was served by the posix fallback: {downgraded:?}",
    );
}

// the capture itself catches a warning, so a silent run means silence and not a
// subscriber that was never listening
#[test]
fn the_capture_hears_a_warning() {
    let warnings = Warnings::default();

    with_default(tracing_subscriber::registry().with(warnings.clone()), || {
        tracing::warn!("the ring is unavailable, in a manner of speaking");
    });

    assert_eq!(warnings.lines().len(), 1);
    assert!(warnings.lines()[0].contains(DOWNGRADE));
}
