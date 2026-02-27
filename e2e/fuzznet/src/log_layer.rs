use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

// (source, message) → (worst_level, count)
#[derive(Debug, Default, Clone)]
pub struct LogHistogram {
    entries: Arc<Mutex<HashMap<(String, String), (String, u64)>>>,
}

fn level_rank(level: &str) -> u8 {
    match level {
        "ERROR" => 3,
        "WARN" => 2,
        "INFO" => 1,
        _ => 0,
    }
}

impl LogHistogram {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns (source, level, message, count) sorted by count desc.
    pub fn snapshot_top(&self, n: usize) -> Vec<(String, String, String, u64)> {
        let entries = self.entries.lock().expect("log histogram lock poisoned");
        let mut out: Vec<(String, String, String, u64)> = entries
            .iter()
            .map(|((source, message), (level, count))| {
                (source.clone(), level.clone(), message.clone(), *count)
            })
            .collect();
        out.sort_by(|a, b| b.3.cmp(&a.3));
        out.truncate(n);
        out
    }

    /// Clear non-error log entries. Errors are preserved across epochs.
    pub fn clear(&self) {
        let mut entries = self.entries.lock().expect("log histogram lock poisoned");
        entries.retain(|_, (level, _)| level == "ERROR");
    }
}

impl<S> Layer<S> for LogHistogram
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let level = *event.metadata().level();
        let level_str = level.to_string();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let source = event.metadata().target().to_owned();
        let message = visitor
            .message
            .unwrap_or_else(|| source.clone());

        let key = (source, message);

        let mut entries = self.entries.lock().expect("log histogram lock poisoned");
        let entry = entries.entry(key).or_insert_with(|| (level_str.clone(), 0));
        entry.1 += 1;
        if level_rank(&level_str) > level_rank(&entry.0) {
            entry.0 = level_str;
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        }
    }
}
