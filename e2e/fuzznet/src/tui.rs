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
    text::Line,
    widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table},
    Frame, Terminal,
};
use tape_core::erasure::SPOOL_COUNT;

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
    spool_active: usize,
    spool_sync: usize,
    spool_recover: usize,
    spool_locked: usize,
    spool_coverage: usize,
    expected_spool_count: usize,
    warning_count: usize,
    history: Vec<u64>,
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
    let term_width = crossterm::terminal::size().map(|(w, _)| w as usize).unwrap_or(80);
    let sparkline_width = term_width.saturating_sub(2); // borders eat 2 cols
    let history: Vec<u64> = state
        .epochs
        .iter()
        .map(|epoch| epoch.wall_duration.as_millis() as u64)
        .rev()
        .take(sparkline_width)
        .collect::<Vec<u64>>()
        .into_iter()
        .rev()
        .collect();

    let (checked, passed) = state.downloaded_count();
    let warning_count = state.total_warnings();
    let total_churn_stopped = state.total_churn_stopped();
    let total_churn_started = state.total_churn_started();
    let mut spool_active = 0usize;
    let mut spool_sync = 0usize;
    let mut spool_recover = 0usize;
    let mut spool_locked = 0usize;
    let mut spool_coverage = 0usize;
    let expected_spool_count = SPOOL_COUNT as usize;
    let mut latest_wall = 0.0f64;

    let mut log_rows: Vec<(String, String, u64)> = Vec::new();
    if let Some(last) = latest {
        spool_active = last.spools_active;
        spool_sync = last.spools_sync;
        spool_recover = last.spools_recover;
        spool_locked = last.spools_locked;
        spool_coverage = last.spools_active + last.spools_sync + last.spools_recover + last.spools_locked;
        latest_wall = last.wall_duration.as_secs_f64();
        log_rows = last
            .log_counts
            .iter()
            .map(|((level, target), count)| (level.to_string(), target.clone(), *count))
            .collect();
    }
    log_rows.sort_by(|a, b| b.2.cmp(&a.2));
    log_rows.truncate(6);

    let phase = match &state.phase {
        FuzzPhase::Bootstrap => "Bootstrap".to_string(),
        FuzzPhase::Warmup => "Warmup".to_string(),
        FuzzPhase::Fuzzing { iteration, current_epoch } => format!("Fuzzing: {iteration}/{} | Epoch: {current_epoch}", state.target_epochs),
        FuzzPhase::Verifying { checked, total } => format!("Verifying: {checked}/{total}"),
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
        spool_active,
        spool_sync,
        spool_recover,
        spool_locked,
        spool_coverage,
        expected_spool_count,
        warning_count,
        history,
        log_rows,
        seed: state.seed,
    }
}

fn render_frame(frame: &mut Frame<'_>, state: &RenderState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(10),
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Min(1),
        ])
        .split(frame.area());

    let header = Paragraph::new(Line::from(format!(
        "{} | Elapsed: {} | Seed: {}",
        state.phase, state.elapsed, state.seed
    )))
    .block(Block::default().borders(Borders::ALL).title("Fuzznet"));
    frame.render_widget(header, chunks[0]);

    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
        .split(chunks[1]);

    let epoch_rows = vec![
        Row::new(vec![
            Cell::from("Duration"),
            Cell::from(format!("{:.1}s", state.latest_epoch_wall_secs)),
        ]),
        Row::new(vec![
            Cell::from("Uploads"),
            Cell::from(state.total_uploads.to_string()),
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

    let spool_rows = vec![
        Row::new(vec![Cell::from("Active"), Cell::from(state.spool_active.to_string())]),
        Row::new(vec![Cell::from("Sync"), Cell::from(state.spool_sync.to_string())]),
        Row::new(vec![Cell::from("Recover"), Cell::from(state.spool_recover.to_string())]),
        Row::new(vec![Cell::from("Locked"), Cell::from(state.spool_locked.to_string())]),
        Row::new(vec![
            Cell::from("Coverage"),
            Cell::from(format!("{}/{}", state.spool_coverage, state.expected_spool_count)),
        ]),
        Row::new(vec![
            Cell::from("Total"),
            Cell::from(state.expected_spool_count.to_string()),
        ]),
    ];

    let spool_table = Table::new(
        spool_rows,
        [Constraint::Length(16), Constraint::Min(20)],
    )
    .block(Block::default().borders(Borders::ALL).title("Spool Status"));

    frame.render_widget(epoch_table, body_chunks[0]);
    frame.render_widget(spool_table, body_chunks[1]);

    let sparkline = Sparkline::default()
        .block(Block::default().title(format!(
            "Epoch Duration (min={:.1}s max={:.1}s)",
            state.history.iter().copied().min().unwrap_or(0) as f64 / 1000.0,
            state.history.iter().copied().max().unwrap_or(0) as f64 / 1000.0,
        )).borders(Borders::ALL))
        .data(&state.history)
        .style(Style::default().fg(Color::Cyan));
    frame.render_widget(sparkline, chunks[2]);

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
    frame.render_widget(logs, chunks[3]);

    let footer = Paragraph::new(Line::from(format!(
        "Uploads: {} ({:.1} MiB) | Downloads: {}/{} passed",
        state.total_uploads,
        (state.total_uploaded_bytes as f64) / (1024.0 * 1024.0),
        state.total_download_passed,
        state.total_downloaded,
    )))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(footer, chunks[4]);
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
