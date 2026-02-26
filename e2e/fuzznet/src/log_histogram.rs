use std::{collections::{HashMap, VecDeque}, fmt, fs, io::Write, path::Path, sync::{Arc, Mutex}};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Default, Clone)]
pub struct LogHistogram {
    pub(crate) current: Arc<Mutex<HashMap<(Level, String), u64>>>,
}

impl LogHistogram {
    pub fn new() -> Self {
        Self {
            current: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn snapshot_and_reset(&self) -> HashMap<(Level, String), u64> {
        let mut current = self.current.lock().expect("log histogram lock poisoned");
        let snapshot = current.clone();
        current.clear();
        snapshot
    }
}

impl<S> Layer<S> for LogHistogram
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut counts = self.current.lock().expect("log histogram lock poisoned");
        let key = (metadata.level().clone(), metadata.target().to_owned());
        *counts.entry(key).or_default() += 1;
    }
}

#[derive(Debug, Default, Clone)]
pub struct RingBuffer {
    pub(crate) lines: Arc<Mutex<VecDeque<String>>>,
    pub(crate) capacity: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            lines: Arc::new(Mutex::new(VecDeque::new())),
            capacity,
        }
    }

    pub fn dump_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut lines = self.lines.lock().expect("ring buffer lock poisoned");
        let mut out = fs::File::create(path)?;
        for line in lines.iter() {
            writeln!(out, "{line}")?;
        }
        Ok(())
    }
}

impl<S> Layer<S> for RingBuffer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let line = format_event(event);
        let mut lines = self.lines.lock().expect("ring buffer lock poisoned");
        lines.push_back(line);
        while lines.len() > self.capacity {
            let _ = lines.pop_front();
        }
    }
}

#[derive(Default)]
struct EventFieldVisitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl Visit for EventFieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let encoded = format!("{value:?}");
        if field.name() == "message" {
            self.message = Some(encoded);
            return;
        }
        self.fields.push((field.name().to_string(), encoded));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_debug(field, &value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_debug(field, &value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_debug(field, &value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_debug(field, &value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record_debug(field, &value);
    }
}

fn format_event(event: &Event<'_>) -> String {
    let metadata = event.metadata();
    let mut visitor = EventFieldVisitor::default();
    event.record(&mut visitor);

    let mut payload = visitor.message.unwrap_or_else(|| {
        if visitor.fields.is_empty() {
            String::new()
        } else {
            visitor
                .fields
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    });

    if !payload.is_empty() {
        payload = format!(" {payload}");
    }

    let module = metadata.module_path().unwrap_or("unknown");
    format!("{} {}{payload}", metadata.level(), module)
}
