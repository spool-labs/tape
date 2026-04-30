use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Duration;

use tape_sdk::metrics::{InMemory, Key, Summary};

pub fn print_stderr(metrics: &InMemory) -> io::Result<()> {
    print(metrics.snapshot(), &mut io::stderr())
}

fn print(mut snapshot: HashMap<Key, Summary>, out: &mut impl Write) -> io::Result<()> {
    if snapshot.is_empty() {
        return Ok(());
    }

    let mut rows: Vec<_> = snapshot.drain().collect();
    rows.sort_by_key(|(key, _)| {
        (
            key.operation.as_str(),
            key.phase.as_str(),
            key.outcome.as_str(),
        )
    });

    writeln!(out, "timings:")?;
    writeln!(
        out,
        "  {:<14} {:<16} {:<8} {:>5} {:>10} {:>10} {:>10} {:>10} {:>12} {:>8}",
        "operation",
        "phase",
        "outcome",
        "count",
        "total",
        "avg",
        "min",
        "max",
        "bytes",
        "chunks"
    )?;

    for (key, summary) in rows {
        writeln!(
            out,
            "  {:<14} {:<16} {:<8} {:>5} {:>10} {:>10} {:>10} {:>10} {:>12} {:>8}",
            key.operation.as_str(),
            key.phase.as_str(),
            key.outcome.as_str(),
            summary.count,
            fmt_duration(summary.total),
            fmt_duration(summary.average().unwrap_or_default()),
            fmt_duration(summary.min),
            fmt_duration(summary.max),
            summary.bytes,
            summary.chunks,
        )?;
    }

    Ok(())
}

fn fmt_duration(duration: Duration) -> String {
    format!("{:.3}s", duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tape_sdk::metrics::{Event, Metrics, Operation, Outcome, Phase};

    use super::*;

    #[test]
    fn renders_summary() {
        let metrics = InMemory::new();
        metrics.record(
            Event::new(
                Operation::WriteStream,
                Phase::Store,
                Outcome::Ok,
                Duration::from_millis(1500),
            )
            .bytes(1024)
            .chunks(1),
        );

        let mut out = Vec::new();
        print(metrics.snapshot(), &mut out).expect("render timings");
        let text = String::from_utf8(out).expect("utf8");

        assert!(text.contains("timings:"));
        assert!(text.contains("write_stream"));
        assert!(text.contains("store"));
        assert!(text.contains("1.500s"));
    }
}
