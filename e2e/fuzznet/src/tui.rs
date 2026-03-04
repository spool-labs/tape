use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::ArcSwap;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::{Frame, Terminal};
use tape_api::program::EPOCH_DURATION;
use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};

use crate::app::{node_color, Command, PollSnapshot, TrackSnapshot, TrackStatus, NODE_EVENT_HISTORY_EPOCHS};
use crate::sparkline::{render_braille_sparkline, render_node_sparkline};

const GROUP_COLS: usize = 7;
const GROUP_ROWS: usize = 3;
const CHIP_WIDTH: usize = 27;
const NODE_ID_WIDTH: usize = 3;
const NODE_SPOOL_WIDTH: usize = 2;
const NODE_STAKE_WIDTH: usize = 5;
const NODE_EVENT_SPARK_WIDTH: usize = NODE_EVENT_HISTORY_EPOCHS;
const EPOCH_CHART_MAX_MS: u64 = (EPOCH_DURATION as u64) * 10 * 1000;

pub fn run_tui(
    snapshot: Arc<ArcSwap<PollSnapshot>>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<Command>,
) -> Result<()> {
    let mut terminal = init_terminal()?;
    let _guard = TerminalDropGuard;

    loop {
        let snap = snapshot.load();
        let disconnected = cmd_tx.is_closed();
        terminal.draw(|frame| render_frame(frame, &snap, disconnected))?;

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('a') => {
                            let _ = cmd_tx.send(Command::AddNode);
                        }
                        KeyCode::Char('r') => {
                            let _ = cmd_tx.send(Command::RemoveNode);
                        }
                        KeyCode::Char('u') => {
                            let _ = cmd_tx.send(Command::UploadBlob);
                        }
                        KeyCode::Char('s') => {
                            let _ = cmd_tx.send(Command::ToggleStakeFuzz);
                        }
                        KeyCode::Char('q') | KeyCode::Esc => {
                            let _ = cmd_tx.send(Command::Quit);
                            break;
                        }
                        KeyCode::Char('c')
                            if key.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            let _ = cmd_tx.send(Command::Quit);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    restore_terminal(&mut terminal)?;
    Ok(())
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    enable_raw_mode()?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    terminal.show_cursor()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

struct TerminalDropGuard;

impl Drop for TerminalDropGuard {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

fn render_frame(frame: &mut Frame<'_>, snap: &PollSnapshot, disconnected: bool) {
    let area = frame.area();
    let term_h = area.height as usize;

    let spool_inner_w = area.width.saturating_sub(2) as usize;
    let groups_per_row = ((spool_inner_w + 1) / (GROUP_COLS + 1)).max(1);
    let bands = (SPOOL_GROUP_COUNT + groups_per_row - 1) / groups_per_row;
    let spool_grid_height = (bands * (GROUP_ROWS + 1)) as u16 + 2;

    let node_inner_w = area.width.saturating_sub(2) as usize;
    let chips_per_row = (node_inner_w / CHIP_WIDTH).max(1);
    let chip_rows = if snap.nodes.is_empty() {
        1
    } else {
        (snap.nodes.len() + chips_per_row - 1) / chips_per_row
    };
    let node_chips_height = chip_rows as u16 + 2;

    // Cap spool and node sections to prevent pushing everything off-screen
    let max_spool_h = (term_h * 40 / 100) as u16;
    let max_node_h = (term_h * 30 / 100) as u16;
    let capped_spool_h = spool_grid_height.min(max_spool_h).max(5);
    let capped_node_h = node_chips_height.min(max_node_h).max(3);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),              // title bar
            Constraint::Length(capped_spool_h), // spools (capped)
            Constraint::Length(capped_node_h),  // nodes (capped)
            Constraint::Length(7),              // charts (bordered)
            Constraint::Length(8),              // tapes (bordered + track grid)
            Constraint::Min(3),                // log
            Constraint::Length(1),              // help bar
        ])
        .split(area);

    render_title_bar(frame, chunks[0], snap);
    render_spool_grid(frame, chunks[1], snap);
    render_node_chips(frame, chunks[2], snap);
    render_charts(frame, chunks[3], snap);
    render_tapes(frame, chunks[4], snap);
    render_log(frame, chunks[5], snap);
    render_help_bar(frame, chunks[6], disconnected);
}

fn render_title_bar(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let phase_color = match snap.epoch_phase.as_str() {
        "Syncing" => Color::Cyan,
        "Settling" => Color::Yellow,
        "Active" => Color::Green,
        _ => Color::DarkGray,
    };
    let phase_detail = match snap.epoch_phase_weight {
        Some(w) => format!(" {}(w:{})", snap.epoch_phase, w),
        None => format!(" {}", snap.epoch_phase),
    };

    let mut left_spans: Vec<Span> = vec![
        Span::styled(" \u{2299}\u{2299}", Style::default().fg(Color::Yellow)),
        Span::styled(" TAPEDRIVE", Style::default().fg(Color::White)),
        Span::styled(
            format!(
                "  Nodes: {}  Stake: {}  C[{}/{}/{}]",
                snap.node_count,
                format_tape(snap.total_stake),
                snap.committee_prev_size,
                snap.committee_size,
                snap.committee_next_size,
            ),
            Style::default().fg(Color::White),
        ),
        Span::styled(phase_detail, Style::default().fg(phase_color)),
    ];

    // Fuzz stats (inline after stake)
    if snap.stake_fuzz_enabled {
        let fuzz_text = if snap.stake_fuzz_failed > 0 {
            format!("  ({} ok, {} err)", snap.stake_fuzz_succeeded, snap.stake_fuzz_failed)
        } else {
            format!("  ({} ok)", snap.stake_fuzz_succeeded)
        };
        let color = if snap.stake_fuzz_failed > 0 { Color::Red } else { Color::DarkGray };
        left_spans.push(Span::styled(fuzz_text, Style::default().fg(color)));
    }

    // Right side: epoch | slot | time
    let elapsed = format_duration(snap.runtime_secs);
    let right = format!(
        "Epoch: {}  Nodes: {}  {}  slot:{} ",
        snap.epoch, snap.node_count, elapsed, snap.slot,
    );

    let left_len: usize = left_spans.iter().map(|s| s.width()).sum();
    let gap = (area.width as usize).saturating_sub(left_len + right.len());

    left_spans.push(Span::raw(" ".repeat(gap)));
    left_spans.push(Span::styled(right, Style::default().fg(Color::DarkGray)));

    frame.render_widget(Paragraph::new(Line::from(left_spans)), area);
}

fn pad_left(area: Rect) -> Rect {
    Rect { x: area.x + 1, width: area.width.saturating_sub(1), ..area }
}

fn render_spool_grid(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let latest_total_store = snap.total_store_history.last().copied().unwrap_or(0);
    let spool_size = format_spool_size(latest_total_store / (SPOOL_COUNT as u64));
    let available = snap.spool_available.iter().filter(|&&a| a).count();
    let title = if available < SPOOL_COUNT {
        format!(" Spools {}/{} ({spool_size} each) ", available, SPOOL_COUNT)
    } else {
        format!(" Spools ({spool_size} each) ")
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width < 8 || inner.height < 3 {
        return;
    }

    let groups_per_row = ((inner.width as usize + 1) / (GROUP_COLS + 1)).max(1);
    let bands = (SPOOL_GROUP_COUNT + groups_per_row - 1) / groups_per_row;

    let mut lines: Vec<Line> = Vec::new();

    for band in 0..bands {
        let mut label_spans: Vec<Span> = Vec::new();
        for col in 0..groups_per_row {
            let group = band * groups_per_row + col;
            if group >= SPOOL_GROUP_COUNT {
                break;
            }
            let label = format!("{:^width$}", group, width = GROUP_COLS);
            label_spans.push(Span::styled(label, Style::default().fg(Color::DarkGray)));
            label_spans.push(Span::raw(" "));
        }
        lines.push(Line::from(label_spans));

        for row in 0..GROUP_ROWS {
            let mut spans: Vec<Span> = Vec::new();
            for col in 0..groups_per_row {
                let group = band * groups_per_row + col;
                if group >= SPOOL_GROUP_COUNT {
                    break;
                }
                for c in 0..GROUP_COLS {
                    let spool_in_group = row * GROUP_COLS + c;
                    if spool_in_group >= SPOOL_GROUP_SIZE {
                        spans.push(Span::raw(" "));
                        continue;
                    }
                    let spool_idx = group * SPOOL_GROUP_SIZE + spool_in_group;
                    if spool_idx >= SPOOL_COUNT {
                        spans.push(Span::raw(" "));
                        continue;
                    }
                    let owner = snap.spool_owners[spool_idx] as usize;
                    if !snap.spool_available[spool_idx] {
                        spans.push(Span::styled("\u{00d7}", Style::default().fg(Color::Red)));
                    } else {
                        let color = node_color(owner);
                        spans.push(Span::styled("\u{258c}", Style::default().fg(color)));
                    }
                }
                spans.push(Span::raw(" "));
            }
            lines.push(Line::from(spans));
        }
    }

    let p = Paragraph::new(lines);
    frame.render_widget(p, pad_left(inner));
}

fn render_node_chips(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(format!(" Nodes ({}) ", snap.node_count));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if snap.nodes.is_empty() {
        let p = Paragraph::new(Line::styled(
            "(no nodes)",
            Style::default().fg(Color::DarkGray),
        ));
        frame.render_widget(p, inner);
        return;
    }

    let chips_per_row = (inner.width as usize / CHIP_WIDTH).max(1);
    let mut lines: Vec<Line> = Vec::new();
    let mut current_spans: Vec<Span> = Vec::new();

    let mut sorted: Vec<_> = snap.nodes.iter().collect();
    sorted.sort_by(|a, b| b.pool_stake.cmp(&a.pool_stake));

    let node_event_max = sorted
        .iter()
        .flat_map(|ns| ns.event_history.iter().copied())
        .max()
        .unwrap_or(0);

    for (i, ns) in sorted.iter().enumerate() {
        let glyph_color = node_color(ns.id + 1);
        let stake = format_tape_fixed_width(ns.pool_stake, NODE_STAKE_WIDTH);
        let chip_text = format!(
            "{:>id_width$} [{:>spools_width$}] {:>stake_width$}",
            ns.id,
            ns.spool_count,
            stake,
            id_width = NODE_ID_WIDTH,
            spools_width = NODE_SPOOL_WIDTH,
            stake_width = NODE_STAKE_WIDTH,
        );
        let spark = render_node_sparkline(&ns.event_history, NODE_EVENT_SPARK_WIDTH, node_event_max);
        let pad_len = CHIP_WIDTH.saturating_sub(1 + chip_text.len() + 1 + spark.len());
        current_spans.push(Span::styled("\u{25a0}", Style::default().fg(glyph_color)));
        current_spans.push(Span::styled(chip_text, Style::default().fg(Color::White)));
        current_spans.push(Span::raw(" "));
        current_spans.extend_from_slice(&spark);
        current_spans.push(Span::raw(" ".repeat(pad_len)));

        if (i + 1) % chips_per_row == 0 {
            lines.push(Line::from(std::mem::take(&mut current_spans)));
        }
    }

    if !current_spans.is_empty() {
        lines.push(Line::from(current_spans));
    }

    let p = Paragraph::new(lines);
    frame.render_widget(p, pad_left(inner));
}

fn render_charts(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Charts ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(inner);

    render_spark(
        frame,
        rows[0],
        "epoch",
        &snap.epoch_duration_history,
        "ms",
        None,
        Some(EPOCH_CHART_MAX_MS),
    );
    render_spark(
        frame,
        rows[1],
        "store",
        &snap.total_store_history,
        "bytes",
        None,
        None,
    );
    render_spark(
        frame,
        rows[2],
        "repair",
        &snap.repair_bw_history,
        "bytes",
        Some(snap.total_repair_bytes),
        None,
    );
    render_spark(
        frame,
        rows[3],
        "recovery",
        &snap.recovery_bw_history,
        "bytes",
        Some(snap.total_recovery_bytes),
        None,
    );
    render_spark(
        frame,
        rows[4],
        "sync",
        &snap.sync_bw_history,
        "bytes",
        Some(snap.total_sync_bytes),
        None,
    );
    render_spark(
        frame,
        rows[5],
        "upload",
        &snap.upload_bw_history,
        "bytes",
        Some(snap.total_upload_bytes),
        None,
    );
}

fn render_tapes(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Tapes ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let line1 = format!(
        " pending: {}  cert: {}  exp: {}  fail: {}  retry: {}",
        snap.uploads_pending, snap.uploads_certified, snap.uploads_expired, snap.uploads_failed, snap.uploads_retries
    );

    let line2 = if let Some(last_retry_error) = snap.uploads_last_retry_error.as_ref() {
        if let Some(next_retry_in_ms) = snap.uploads_next_retry_in_ms {
            if next_retry_in_ms == 0 {
                format!(" last retry: {last_retry_error} | retrying now")
            } else {
                let delay = format_retry_delay(next_retry_in_ms);
                format!(" last retry: {last_retry_error} | next retry in {delay}")
            }
        } else if snap.uploads_retry_in_progress {
            format!(" last retry: {last_retry_error} | retrying now")
        } else {
            format!(" last retry: {last_retry_error}")
        }
    } else {
        if let Some(next_retry_in_ms) = snap.uploads_next_retry_in_ms {
            if next_retry_in_ms == 0 {
                " retrying now".to_string()
            } else {
                format!(" next retry in {}", format_retry_delay(next_retry_in_ms))
            }
        } else if snap.uploads_retry_in_progress {
            " retrying now".to_string()
        } else {
            " no retry yet".to_string()
        }
    };

    let mut lines: Vec<Line> = vec![Line::from(line1), Line::from(line2)];
    let track_rows = render_track_grid(inner.width as usize, &snap.tracks);
    lines.extend(track_rows);

    let max_rows = inner.height as usize;
    if lines.len() > max_rows {
        lines.truncate(max_rows.saturating_sub(1));
        lines.push(Line::styled(" ...", Style::default().fg(Color::DarkGray)));
    }

    let p = Paragraph::new(lines);
    frame.render_widget(p, inner);
}

fn render_track_grid(width: usize, tracks: &[TrackSnapshot]) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }

    if tracks.is_empty() {
        return vec![Line::styled(
            " no tracks",
            Style::default().fg(Color::DarkGray),
        )];
    }

    let mut rows: Vec<Line<'static>> = Vec::new();
    let mut current: Vec<Span> = Vec::new();
    for track in tracks {
        let (glyph, color) = track_glyph(track);
        current.push(Span::styled(glyph, Style::default().fg(color)));
        if current.len() == width {
            rows.push(Line::from(std::mem::take(&mut current)));
        }
    }
    if !current.is_empty() {
        rows.push(Line::from(current));
    }

    rows
}

fn track_glyph(track: &TrackSnapshot) -> (String, Color) {
    match track.status {
        TrackStatus::Registered => ("◻".to_string(), Color::DarkGray),
        TrackStatus::Certified => ("◼".to_string(), Color::Green),
        TrackStatus::Expired => ("◻".to_string(), Color::Yellow),
        TrackStatus::Failed => ("✗".to_string(), Color::Red),
        TrackStatus::Unknown => ("?".to_string(), Color::DarkGray),
    }
}

fn render_spark(
    frame: &mut Frame<'_>,
    area: Rect,
    label: &str,
    data: &[u64],
    unit: &str,
    total: Option<u64>,
    fixed_scale_max: Option<u64>,
) {
    if area.width < 20 {
        return;
    }

    let label_width = 9u16; // fixed: " network " is the widest
    let value_width = 10u16;
    let spark_width = area.width.saturating_sub(label_width + value_width);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(label_width),
            Constraint::Length(spark_width),
            Constraint::Length(value_width),
        ])
        .split(area);

    let label_p = Paragraph::new(Line::styled(
        format!(" {label:<8}"),
        Style::default().fg(Color::White),
    ));
    frame.render_widget(label_p, chunks[0]);

    let display_val = total.unwrap_or_else(|| data.last().copied().unwrap_or(0));
    let val_str = match unit {
        "ms" => format_ms(display_val),
        _ => format_bytes(display_val),
    };
    let val_p = Paragraph::new(Line::styled(
        format!("{val_str:>9} "),
        Style::default().fg(Color::White),
    ));
    frame.render_widget(val_p, chunks[2]);

    // Braille sparkline with btop gradient
    let chart_w = chunks[1].width as usize;
    let braille_line = render_braille_sparkline(data, chart_w, fixed_scale_max);
    let braille_p = Paragraph::new(Line::from(braille_line));
    frame.render_widget(braille_p, chunks[1]);
}

fn render_log(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Log ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows: Vec<Row> = if snap.log.is_empty() {
        vec![Row::new(vec!["(no log events)", "", ""])]
    } else {
        snap.log
            .iter()
            .map(|(source, level, msg, count)| {
                let color = match level.as_str() {
                    "ERROR" => Color::Red,
                    "WARN" => Color::Yellow,
                    _ => Color::White,
                };
                Row::new(vec![
                    Cell::from(source.as_str()),
                    Cell::from(msg.as_str()),
                    Cell::from(count.to_string()),
                ])
                .style(Style::default().fg(color))
            })
            .collect()
    };

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(25),
            Constraint::Percentage(65),
            Constraint::Percentage(10),
        ],
    )
    .header(
        Row::new(vec!["source", "message", "count"]).style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(table, pad_left(inner));
}

fn render_help_bar(frame: &mut Frame<'_>, area: Rect, disconnected: bool) {
    let mut spans = vec![
        Span::styled(" [a]dd  [r]emove  [u]pload  [s]take-fuzz  [q]uit", Style::default().fg(Color::DarkGray)),
    ];
    if disconnected {
        spans.push(Span::styled("  DISCONNECTED", Style::default().fg(Color::Red)));
    }
    let p = Paragraph::new(Line::from(spans));
    frame.render_widget(p, area);
}

fn format_duration(secs: f64) -> String {
    let total = secs as u64;
    let h = total / 3600;
    let m = (total / 60) % 60;
    let s = total % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn format_retry_delay(ms: u64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

fn format_bytes(b: u64) -> String {
    if b >= 1_073_741_824 {
        format!("{:.1}G", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1}M", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1}K", b as f64 / 1024.0)
    } else {
        format!("{b}B")
    }
}

fn format_ms(ms: u64) -> String {
    if ms >= 10_000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

fn format_tape(flux: u64) -> String {
    let tape = flux / 1_000_000;
    let frac = (flux % 1_000_000) / 1_000;
    if tape >= 1_000_000 {
        format!("{:.1}MT", tape as f64 / 1_000_000.0)
    } else if tape >= 1_000 {
        format!("{:.1}KT", tape as f64 / 1_000.0)
    } else if frac > 0 {
        format!("{tape}.{frac:03}T")
    } else {
        format!("{tape}T")
    }
}

fn format_tape_fixed_width(flux: u64, width: usize) -> String {
    let tape = flux / 1_000_000;
    let frac = (flux % 1_000_000) / 1000;

    let mut raw = if frac == 0 || tape >= 100 {
        tape.to_string()
    } else if tape < 10 {
        format!("{}.{}", tape, frac / 100)
    } else {
        format!("{}.{}", tape, frac / 10 % 10)
    };

    if raw.len() > width {
        raw = tape.to_string();
    }

    if raw.len() > width {
        raw.truncate(width);
    }

    format!("{:>width$}", raw, width = width)
}

fn format_spool_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1}GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1}MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1}KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
