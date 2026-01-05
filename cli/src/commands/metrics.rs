//! Metrics display commands.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Subcommand;
use comfy_table::{presets::UTF8_FULL, Table};

use crate::output::OutputFormat;
use crate::utils::spinner;
use crate::Context;

/// Default metrics endpoint URL.
const DEFAULT_METRICS_URL: &str = "http://localhost:9090/metrics";

#[derive(Subcommand, Debug)]
pub enum MetricsCommand {
    /// Display current metrics.
    Show {
        /// Metrics endpoint URL.
        #[arg(long)]
        url: Option<String>,

        /// Filter metrics by prefix (e.g., "tape_store").
        #[arg(long)]
        filter: Option<String>,
    },

    /// Live metrics dashboard.
    Watch {
        /// Refresh interval in seconds.
        #[arg(long, default_value = "2")]
        interval: u64,

        /// Metrics endpoint URL.
        #[arg(long)]
        url: Option<String>,

        /// Filter metrics by prefix.
        #[arg(long)]
        filter: Option<String>,
    },

    /// Export metrics.
    Export {
        /// Output file (stdout if not specified).
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Metrics endpoint URL.
        #[arg(long)]
        url: Option<String>,

        /// Export format: prometheus, json.
        #[arg(long, default_value = "prometheus")]
        format: String,
    },
}

/// Parsed Prometheus metric.
#[derive(Debug, Clone)]
struct Metric {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
    metric_type: Option<String>,
    help: Option<String>,
}

/// Parse Prometheus text format into structured metrics.
fn parse_prometheus_metrics(text: &str) -> Vec<Metric> {
    let mut metrics = Vec::new();
    let mut current_type: Option<String> = None;
    let mut current_help: Option<String> = None;
    let mut current_name: Option<String> = None;

    for line in text.lines() {
        let line = line.trim();

        // Skip empty lines
        if line.is_empty() {
            continue;
        }

        // Parse HELP comment
        if line.starts_with("# HELP ") {
            let rest = &line[7..];
            if let Some(space_idx) = rest.find(' ') {
                current_name = Some(rest[..space_idx].to_string());
                current_help = Some(rest[space_idx + 1..].to_string());
            }
            continue;
        }

        // Parse TYPE comment
        if line.starts_with("# TYPE ") {
            let rest = &line[7..];
            if let Some(space_idx) = rest.find(' ') {
                let name = &rest[..space_idx];
                let type_str = &rest[space_idx + 1..];
                if current_name.as_deref() == Some(name) {
                    current_type = Some(type_str.to_string());
                }
            }
            continue;
        }

        // Skip other comments
        if line.starts_with('#') {
            continue;
        }

        // Parse metric line: name{labels} value
        if let Some(metric) = parse_metric_line(line, &current_type, &current_help) {
            metrics.push(metric);
        }
    }

    metrics
}

/// Parse a single metric line.
fn parse_metric_line(
    line: &str,
    metric_type: &Option<String>,
    help: &Option<String>,
) -> Option<Metric> {
    // Split name/labels from value
    let (name_labels, value_str) = if let Some(_brace_idx) = line.find('{') {
        // Has labels: name{label="value",...} value
        let close_brace = line.find('}')?;
        let after_brace = &line[close_brace + 1..].trim();
        let value_str = after_brace.split_whitespace().next()?;
        (line[..close_brace + 1].to_string(), value_str)
    } else {
        // No labels: name value
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            return None;
        }
        (parts[0].to_string(), parts[1])
    };

    // Parse the value
    let value: f64 = value_str.parse().ok()?;

    // Extract name and labels
    let (name, labels) = if let Some(brace_idx) = name_labels.find('{') {
        let name = name_labels[..brace_idx].to_string();
        let labels_str = &name_labels[brace_idx + 1..name_labels.len() - 1];
        let labels = parse_labels(labels_str);
        (name, labels)
    } else {
        (name_labels, HashMap::new())
    };

    Some(Metric {
        name,
        labels,
        value,
        metric_type: metric_type.clone(),
        help: help.clone(),
    })
}

/// Parse label string into HashMap.
fn parse_labels(labels_str: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    let mut current_key = String::new();
    let mut current_value = String::new();
    let mut in_value = false;
    let mut in_quotes = false;

    for ch in labels_str.chars() {
        match ch {
            '=' if !in_quotes => {
                in_value = true;
            }
            '"' if in_value => {
                if in_quotes {
                    // End of value
                    labels.insert(
                        current_key.trim().to_string(),
                        current_value.clone(),
                    );
                    current_key.clear();
                    current_value.clear();
                    in_value = false;
                }
                in_quotes = !in_quotes;
            }
            ',' if !in_quotes => {
                // Next label
            }
            _ => {
                if in_value && in_quotes {
                    current_value.push(ch);
                } else if !in_value {
                    current_key.push(ch);
                }
            }
        }
    }

    labels
}

/// Fetch metrics from endpoint.
async fn fetch_metrics(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Failed to fetch metrics: HTTP {}",
            response.status()
        );
    }

    Ok(response.text().await?)
}

/// Format a metric value for display.
fn format_metric_value(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1_000_000.0 {
        // Integer-like value
        format!("{}", value as i64)
    } else if value.abs() >= 1_000_000.0 {
        // Large value - use scientific notation
        format!("{:.2e}", value)
    } else {
        // Decimal value
        format!("{:.4}", value)
    }
}

/// Format labels for display.
fn format_labels(labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }

    let parts: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();

    format!("{{{}}}", parts.join(", "))
}

pub async fn execute(ctx: &Context, cmd: MetricsCommand) -> Result<()> {
    match cmd {
        MetricsCommand::Show { url, filter } => show_metrics(ctx, url, filter).await,
        MetricsCommand::Watch { interval, url, filter } => {
            watch_metrics(ctx, url, interval, filter).await
        }
        MetricsCommand::Export { output, url, format } => {
            export_metrics(ctx, url, output, format).await
        }
    }
}

async fn show_metrics(
    ctx: &Context,
    url: Option<String>,
    filter: Option<String>,
) -> Result<()> {
    let url = url.unwrap_or_else(|| DEFAULT_METRICS_URL.to_string());

    let pb = spinner(&format!("Fetching metrics from {}...", url));
    let text = fetch_metrics(&url).await?;
    pb.finish_and_clear();

    let metrics = parse_prometheus_metrics(&text);

    // Apply filter if specified
    let filtered: Vec<_> = if let Some(ref prefix) = filter {
        metrics
            .into_iter()
            .filter(|m| m.name.starts_with(prefix))
            .collect()
    } else {
        metrics
    };

    if filtered.is_empty() {
        ctx.print("No metrics found.");
        return Ok(());
    }

    match ctx.output {
        OutputFormat::Json => {
            let json_metrics: Vec<_> = filtered
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "name": m.name,
                        "labels": m.labels,
                        "value": m.value,
                        "type": m.metric_type,
                        "help": m.help,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&json_metrics)?);
        }
        _ => {
            // Group metrics by name for better display
            let mut grouped: HashMap<String, Vec<&Metric>> = HashMap::new();
            for m in &filtered {
                grouped.entry(m.name.clone()).or_default().push(m);
            }

            let mut names: Vec<_> = grouped.keys().cloned().collect();
            names.sort();

            for name in names {
                let metrics = &grouped[&name];

                // Print header with help if available
                if let Some(help) = metrics.first().and_then(|m| m.help.as_ref()) {
                    println!("# {}", name);
                    println!("# {}", help);
                } else {
                    println!("# {}", name);
                }

                if metrics.len() == 1 && metrics[0].labels.is_empty() {
                    // Simple metric without labels
                    println!("  {}", format_metric_value(metrics[0].value));
                } else {
                    // Metric with labels - display as table
                    let mut table = Table::new();
                    table.load_preset(UTF8_FULL);

                    // Get all label keys
                    let mut label_keys: Vec<_> = metrics
                        .iter()
                        .flat_map(|m| m.labels.keys().cloned())
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .collect();
                    label_keys.sort();

                    // Build header
                    let mut header = label_keys.clone();
                    header.push("value".to_string());
                    table.set_header(header);

                    // Add rows
                    for m in metrics {
                        let mut row: Vec<String> = label_keys
                            .iter()
                            .map(|k| m.labels.get(k).cloned().unwrap_or_default())
                            .collect();
                        row.push(format_metric_value(m.value));
                        table.add_row(row);
                    }

                    println!("{}", table);
                }
                println!();
            }
        }
    }

    Ok(())
}

async fn watch_metrics(
    ctx: &Context,
    url: Option<String>,
    interval: u64,
    filter: Option<String>,
) -> Result<()> {
    let url = url.unwrap_or_else(|| DEFAULT_METRICS_URL.to_string());
    let interval = Duration::from_secs(interval);

    ctx.print(&format!(
        "Watching metrics at {} (Ctrl+C to stop)...",
        url
    ));

    // Track previous values for delta calculation
    let mut prev_values: HashMap<String, f64> = HashMap::new();

    loop {
        // Clear screen
        print!("\x1B[2J\x1B[1;1H");

        // Fetch current metrics
        let text = match fetch_metrics(&url).await {
            Ok(t) => t,
            Err(e) => {
                println!("Error fetching metrics: {}", e);
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let metrics = parse_prometheus_metrics(&text);

        // Apply filter
        let filtered: Vec<_> = if let Some(ref prefix) = filter {
            metrics
                .into_iter()
                .filter(|m| m.name.starts_with(prefix))
                .collect()
        } else {
            metrics
        };

        // Display header
        println!(
            "Tapedrive Metrics Dashboard - {}",
            chrono::Local::now().format("%H:%M:%S")
        );
        println!("Endpoint: {}", url);
        if let Some(ref f) = filter {
            println!("Filter: {}", f);
        }
        println!("{}", "=".repeat(60));
        println!();

        // Group and display key metrics
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(vec!["Metric", "Value", "Delta/s"]);

        for m in &filtered {
            let key = format!("{}{}", m.name, format_labels(&m.labels));
            let delta = if let Some(prev) = prev_values.get(&key) {
                let rate = (m.value - prev) / interval.as_secs_f64();
                if rate.abs() < 0.001 {
                    "-".to_string()
                } else {
                    format!("{:+.2}", rate)
                }
            } else {
                "-".to_string()
            };

            table.add_row(vec![
                key.clone(),
                format_metric_value(m.value),
                delta,
            ]);

            prev_values.insert(key, m.value);
        }

        println!("{}", table);
        println!();
        println!("Press Ctrl+C to exit");

        tokio::time::sleep(interval).await;
    }
}

async fn export_metrics(
    ctx: &Context,
    url: Option<String>,
    output: Option<PathBuf>,
    format: String,
) -> Result<()> {
    let url = url.unwrap_or_else(|| DEFAULT_METRICS_URL.to_string());

    let pb = spinner(&format!("Exporting metrics from {}...", url));
    let text = fetch_metrics(&url).await?;
    pb.finish_and_clear();

    let output_content = match format.to_lowercase().as_str() {
        "prometheus" | "prom" => {
            // Export raw Prometheus format
            text
        }
        "json" => {
            // Parse and export as JSON
            let metrics = parse_prometheus_metrics(&text);
            let json_metrics: Vec<_> = metrics
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "name": m.name,
                        "labels": m.labels,
                        "value": m.value,
                        "type": m.metric_type,
                        "help": m.help,
                    })
                })
                .collect();
            serde_json::to_string_pretty(&json_metrics)?
        }
        _ => {
            anyhow::bail!("Unknown format: {}. Use 'prometheus' or 'json'", format);
        }
    };

    match output {
        Some(path) => {
            let mut file = std::fs::File::create(&path)?;
            file.write_all(output_content.as_bytes())?;
            ctx.print(&format!("Metrics exported to: {}", path.display()));
        }
        None => {
            println!("{}", output_content);
        }
    }

    Ok(())
}
