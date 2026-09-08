//! The backend a node's volume actually opened on, not the one it asked for
//!
//! The engine downgrades a configured ring to posix with one warning and serves
//! the volume anyway, so a node can ask for the ring and run its whole life on
//! the fallback. The config restates the request, so only the volume's own
//! answer and that warning say what happened.

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
// The volume's own answer and the line an operator reads are two statements of
// the same fact, so both are checked. Linux only: elsewhere there is no ring to
// be had. A red here means this machine will not give a node the backend it
// ships with, which a container needs seccomp=unconfined for.
#[cfg(target_os = "linux")]
#[test]
fn ring_holds() {
    let dir = tempfile::TempDir::new().expect("dir");
    let warnings = Warnings::default();

    let store = with_default(tracing_subscriber::registry().with(warnings.clone()), || {
        reel_store::open_node_store(
            dir.path().join("volume"),
            reel_store::NodeStoreOptions {
                backend: reel::IoBackend::Uring,
                ..reel_store::NodeStoreOptions::default()
            },
        )
        .expect("open the node store")
    });

    let serving = store.inner().inner().serving_backend();
    let downgraded: Vec<String> = warnings
        .lines()
        .into_iter()
        .filter(|line| line.contains(DOWNGRADE))
        .collect();

    assert!(
        serving.is_ring(),
        "a volume configured for the ring is served by {serving}",
    );
    assert!(
        downgraded.is_empty(),
        "the volume reports {serving} while its log reports a downgrade: {downgraded:?}",
    );
}

// the capture catches a warning, so a silent run means silence and not a
// subscriber that was never listening
#[test]
fn capture_hears() {
    let warnings = Warnings::default();

    with_default(tracing_subscriber::registry().with(warnings.clone()), || {
        tracing::warn!("the ring is unavailable, in a manner of speaking");
    });

    assert_eq!(warnings.lines().len(), 1);
    assert!(warnings.lines()[0].contains(DOWNGRADE));
}
