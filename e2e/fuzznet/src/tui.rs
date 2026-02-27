use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    symbols::Marker,
    text::Line,
    widgets::{Axis, Block, Borders, Cell, Chart, Dataset, GraphType, Paragraph, Row, Table},
    Frame, Terminal,
};

use crate::stats::{FuzzPhase, FuzzStats};

struct RenderState {
    phase: String,
    elapsed: String,
    total_uploads: usize,
    total_uploaded_bytes: u64,
    total_downloaded: usize,
    total_download_passed: usize,
    latest_epoch_wall_secs: f64,
    total_churn_stopped: usize,
    total_churn_started: usize,
    warning_count: usize,
    target_epochs: u64,
    duration_points: Vec<(f64, f64)>,
    upload_mib_points: Vec<(f64, f64)>,
    network_mib_points: Vec<(f64, f64)>,
    committee_count_points: Vec<(f64, f64)>,
    sync_kib_points: Vec<(f64, f64)>,
    repair_kib_points: Vec<(f64, f64)>,
    log_rows: Vec<(String, String, u64)>,
    seed: u64,
}

struct TerminalDropGuard;

impl Drop for TerminalDropGuard {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

pub async fn run_tui(
    stats: Arc<Mutex<FuzzStats>>,
    abort: Arc<AtomicBool>,
    tick_ms: u64,
) -> Result<()> {
    let mut terminal = init_terminal()?;
    let _drop_guard = TerminalDropGuard;

    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    loop {
        if abort.load(Ordering::Acquire) {
            break;
        }

        let state = snapshot_stats(&stats);
        terminal.draw(|frame| render_frame(frame, &state))?;

        if should_exit(&abort).await? {
            break;
        }

        interval.tick().await;
    }

    restore_terminal(&mut terminal)?;
    Ok(())
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    enable_raw_mode()?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

async fn should_exit(abort: &Arc<AtomicBool>) -> Result<bool> {
    if event::poll(Duration::from_millis(1))? {
        if let Event::Key(key) = event::read()? {
            if key.kind == KeyEventKind::Press {
                if key.code == KeyCode::Char('q')
                    || (key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL))
                {
                    abort.store(true, Ordering::Release);
                    return Ok(true);
                }
            }
        }
    }

    Ok(abort.load(Ordering::Acquire))
}

fn snapshot_stats(stats: &Arc<Mutex<FuzzStats>>) -> RenderState {
    let state = stats.lock().expect("stats lock poisoned");
    let latest = state.epochs.last();

    let mut latest_wall = 0.0f64;
    let mut log_rows: Vec<(String, String, u64)> = Vec::new();
    if let Some(last) = latest {
        latest_wall = last.wall_duration.as_secs_f64();
        log_rows = last
            .log_counts
            .iter()
            .map(|((level, target), count)| (level.to_string(), target.clone(), *count))
            .collect();
    }
    log_rows.sort_by(|a, b| b.2.cmp(&a.2));
    log_rows.truncate(6);

    let duration_points: Vec<(f64, f64)> = state
        .epochs
        .iter()
        .enumerate()
        .map(|(i, e)| (i as f64, e.wall_duration.as_secs_f64()))
        .collect();

    let upload_mib_points: Vec<(f64, f64)> = state
        .epochs
        .iter()
        .enumerate()
        .map(|(i, e)| (i as f64, e.uploaded_bytes as f64 / (1024.0 * 1024.0)))
        .collect();

    let network_mib_points: Vec<(f64, f64)> = state
        .epochs
        .iter()
        .enumerate()
        .map(|(i, e)| (i as f64, e.network_size_bytes as f64 / (1024.0 * 1024.0)))
        .collect();

    let committee_count_points: Vec<(f64, f64)> = state
        .epochs
        .iter()
        .enumerate()
        .map(|(i, e)| (i as f64, e.committee_count as f64))
        .collect();

    let sync_kib_points: Vec<(f64, f64)> = state
        .epochs
        .iter()
        .enumerate()
        .map(|(i, e)| (i as f64, e.sync_bytes as f64 / 1024.0))
        .collect();

    let repair_kib_points: Vec<(f64, f64)> = state
        .epochs
        .iter()
        .enumerate()
        .map(|(i, e)| (i as f64, e.repair_bytes as f64 / 1024.0))
        .collect();

    let (checked, passed) = state.downloaded_count();
    let warning_count = state.total_warnings();
    let total_churn_stopped = state.total_churn_stopped();
    let total_churn_started = state.total_churn_started();

    let phase = match &state.phase {
        FuzzPhase::Bootstrap => "Bootstrap".to_string(),
        FuzzPhase::Warmup => "Warmup".to_string(),
        FuzzPhase::Fuzzing { current_epoch, target_epoch } => format!("Epoch: {current_epoch}/{target_epoch}"),
        FuzzPhase::Done { passed } => format!("Done ({})", if *passed { "pass" } else { "fail" }),
    };

    RenderState {
        phase,
        elapsed: format_duration(state.start_time.elapsed()),
        total_uploads: state.upload_registry.len(),
        total_uploaded_bytes: state.uploaded_bytes_total(),
        total_downloaded: checked,
        total_download_passed: passed,
        latest_epoch_wall_secs: latest_wall,
        total_churn_stopped,
        total_churn_started,
        warning_count,
        target_epochs: state.target_epochs,
        duration_points,
        upload_mib_points,
        network_mib_points,
        committee_count_points,
        sync_kib_points,
        repair_kib_points,
        log_rows,
        seed: state.seed,
    }
}

fn render_frame(frame: &mut Frame<'_>, state: &RenderState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // header
            Constraint::Length(8),  // run stats
            Constraint::Length(1),  // epoch duration title
            Constraint::Length(1),  // epoch duration chart
            Constraint::Length(1),  // upload mib title
            Constraint::Length(1),  // upload mib chart
            Constraint::Length(1),  // total store title
            Constraint::Length(1),  // total store chart
            Constraint::Length(1),  // alive nodes title
            Constraint::Length(1),  // alive nodes chart
            Constraint::Length(1),  // sync bandwidth title
            Constraint::Length(1),  // sync bandwidth chart
            Constraint::Length(1),  // repair bandwidth title
            Constraint::Length(1),  // repair bandwidth chart
            Constraint::Length(8),  // log histogram
            Constraint::Length(1),  // footer
        ])
        .split(frame.area());

    // Header
    let header = Paragraph::new(Line::from(format!(
        "{} | Elapsed: {} | Seed: {}",
        state.phase, state.elapsed, state.seed
    )))
    .block(Block::default().borders(Borders::ALL).title("Fuzznet"));
    frame.render_widget(header, chunks[0]);

    // Run Stats table
    let epoch_rows = vec![
        Row::new(vec![
            Cell::from("Duration"),
            Cell::from(format!("{:.1}s", state.latest_epoch_wall_secs)),
        ]),
        Row::new(vec![
            Cell::from("Uploads"),
            Cell::from(format!(
                "{} ({:.1} MiB)",
                state.total_uploads,
                state.total_uploaded_bytes as f64 / (1024.0 * 1024.0),
            )),
        ]),
        Row::new(vec![
            Cell::from("Downloads"),
            Cell::from(format!(
                "{}/{} passed",
                state.total_download_passed, state.total_downloaded
            )),
        ]),
        Row::new(vec![
            Cell::from("Churn"),
            Cell::from(format!(
                "{} stopped, {} started",
                state.total_churn_stopped, state.total_churn_started
            )),
        ]),
        Row::new(vec![
            Cell::from("Warnings"),
            Cell::from(state.warning_count.to_string()),
        ]),
    ];

    let epoch_table = Table::new(
        epoch_rows,
        [Constraint::Length(16), Constraint::Min(20)],
    )
    .block(Block::default().borders(Borders::ALL).title("Run Stats"));
    frame.render_widget(epoch_table, chunks[1]);

    let x_window = state.target_epochs as f64;

    // Charts — each uses 2 rows: title (Length 1) + braille (Length 1)
    render_chart(frame, chunks[2], chunks[3], "Epoch Duration", "s", Color::Cyan, &state.duration_points, x_window);
    render_chart(frame, chunks[4], chunks[5], "Upload MiB", "MiB", Color::Green, &state.upload_mib_points, x_window);
    render_chart(frame, chunks[6], chunks[7], "Total Store", "MiB", Color::Yellow, &state.network_mib_points, x_window);
    render_chart(frame, chunks[8], chunks[9], "Alive Nodes", "", Color::Magenta, &state.committee_count_points, x_window);
    render_chart(frame, chunks[10], chunks[11], "Sync Bandwidth", "KiB", Color::Blue, &state.sync_kib_points, x_window);
    render_chart(frame, chunks[12], chunks[13], "Repair Bandwidth", "KiB", Color::Red, &state.repair_kib_points, x_window);

    // Log Histogram
    let log_rows = if state.log_rows.is_empty() {
        vec![Row::new(vec!["", "No log events", ""])]
    } else {
        state
            .log_rows
            .iter()
            .map(|(level, source, count)| {
                Row::new(vec![
                    Cell::from(level.as_str()),
                    Cell::from(source.clone()),
                    Cell::from(count.to_string()),
                ])
            })
            .collect()
    };

    let logs = Table::new(
        log_rows,
        [
            Constraint::Length(8),
            Constraint::Percentage(70),
            Constraint::Length(8),
        ],
    )
    .header(Row::new(vec!["Level", "Source", "Count"]).style(Style::default().fg(Color::Yellow)))
    .block(Block::default().borders(Borders::ALL).title("Log Histogram (this epoch)"));
    frame.render_widget(logs, chunks[14]);

    // Footer
    let footer = Paragraph::new(Line::from(" q/Ctrl-C to quit"));
    frame.render_widget(footer, chunks[15]);
}

fn render_chart(
    frame: &mut Frame<'_>,
    title_area: ratatui::layout::Rect,
    chart_area: ratatui::layout::Rect,
    name: &str,
    unit: &str,
    color: Color,
    points: &[(f64, f64)],
    x_window: f64,
) {
    let current = points.last().map(|(_, y)| *y).unwrap_or(0.0);
    let title = if unit.is_empty() {
        format!(" {name}: {current:.0}")
    } else {
        format!(" {name}: {current:.1} {unit}")
    };
    let title_line = Paragraph::new(Line::styled(title, Style::default().fg(color)));
    frame.render_widget(title_line, title_area);

    if points.is_empty() {
        return;
    }

    let x_max = x_window.max(1.0);
    let y_max = points
        .iter()
        .map(|(_, y)| *y)
        .fold(0.0f64, f64::max)
        .max(0.001);

    let datasets = vec![Dataset::default()
        .marker(Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(color))
        .data(points)];

    let chart = Chart::new(datasets)
        .x_axis(Axis::default().bounds([0.0, x_max]))
        .y_axis(Axis::default().bounds([0.0, y_max * 1.1]));
    frame.render_widget(chart, chart_area);
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    terminal.show_cursor()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

fn format_duration(duration: Duration) -> String {
    let total = duration.as_secs();
    let mins = (total / 60) % 60;
    let secs = total % 60;
    let hours = total / 3600;
    format!("{hours:02}:{mins:02}:{secs:02}")
}
