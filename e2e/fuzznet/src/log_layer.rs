use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Default, Clone)]
pub struct LogHistogram {
    counts: Arc<Mutex<HashMap<String, u64>>>,
}

impl LogHistogram {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn snapshot_top(&self, n: usize) -> Vec<(String, u64)> {
        let counts = self.counts.lock().expect("log histogram lock poisoned");
        let mut entries: Vec<(String, u64)> = counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }

    pub fn clear(&self) {
        self.counts.lock().expect("log histogram lock poisoned").clear();
    }
}

impl<S> Layer<S> for LogHistogram
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let key = visitor
            .message
            .unwrap_or_else(|| event.metadata().target().to_owned());
        let mut counts = self.counts.lock().expect("log histogram lock poisoned");
        *counts.entry(key).or_default() += 1;
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
