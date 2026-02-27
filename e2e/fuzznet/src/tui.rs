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
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table};
use ratatui::{Frame, Terminal};
use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};

use crate::app::{node_color, Command, PollSnapshot};

const GROUP_COLS: usize = 7;
const GROUP_ROWS: usize = 3;
const CHIP_WIDTH: usize = 16;

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

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(spool_grid_height),
            Constraint::Length(node_chips_height),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);

    render_title_bar(frame, chunks[0], snap);
    render_spool_grid(frame, chunks[1], snap);
    render_node_chips(frame, chunks[2], snap);
    render_spark(frame, chunks[3], "epoch-dur", Color::Cyan, &snap.epoch_duration_history);
    render_spark(frame, chunks[4], "store-sz", Color::Yellow, &snap.total_store_history);
    render_spark(frame, chunks[5], "repair-bw", Color::Red, &snap.repair_bw_history);
    render_spark(frame, chunks[6], "sync-bw", Color::Blue, &snap.sync_bw_history);
    render_spark(frame, chunks[7], "upload-bw", Color::Green, &snap.upload_bw_history);
    render_log_histogram(frame, chunks[8], snap);
    render_help_bar(frame, chunks[9], disconnected);
}

fn render_title_bar(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let elapsed = format_duration(snap.runtime_secs);
    let right = format!(
        "Epoch: {}  Nodes: {}  {}  slot:{} ",
        snap.epoch, snap.node_count, elapsed, snap.slot,
    );
    // display width: " ⊙⊙" (3) + " TAPEDRIVE" (10) + "  [status]" (4+status)
    let left_display_width = 17 + snap.status.len();
    let gap = (area.width as usize).saturating_sub(left_display_width + right.len());
    let status_color = if snap.status == "ready" {
        Color::Green
    } else if snap.status.starts_with("INIT FAILED") || snap.status.starts_with("add_node failed") {
        Color::Red
    } else {
        Color::Yellow
    };
    let line = Line::from(vec![
        Span::styled(" \u{2299}\u{2299}", Style::default().fg(Color::Yellow)),
        Span::styled(" TAPEDRIVE", Style::default().fg(Color::White)),
        Span::styled(format!("  [{}]", snap.status), Style::default().fg(status_color)),
        Span::raw(" ".repeat(gap)),
        Span::styled(right, Style::default().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_spool_grid(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Spools ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width < 8 || inner.height < 3 {
        return;
    }

    let groups_per_row = ((inner.width as usize + 1) / (GROUP_COLS + 1)).max(1);
    let bands = (SPOOL_GROUP_COUNT + groups_per_row - 1) / groups_per_row;

    let mut lines: Vec<Line> = Vec::new();

    for band in 0..bands {
        // Label row
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

        // 3 spool rows per band
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
                    let color = node_color(owner);
                    spans.push(Span::styled("\u{258c}", Style::default().fg(color)));
                }
                spans.push(Span::raw(" "));
            }
            lines.push(Line::from(spans));
        }
    }

    let p = Paragraph::new(lines);
    frame.render_widget(p, inner);
}

fn render_node_chips(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Nodes ");
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

    for (i, ns) in snap.nodes.iter().enumerate() {
        let color = node_color(ns.id + 1);
        let chip = format!("\u{25a0} #{} [{}]", ns.id, ns.spool_count);
        let padded = format!("{:<width$}", chip, width = CHIP_WIDTH);
        current_spans.push(Span::styled(padded, Style::default().fg(color)));

        if (i + 1) % chips_per_row == 0 {
            lines.push(Line::from(std::mem::take(&mut current_spans)));
        }
    }

    if !current_spans.is_empty() {
        lines.push(Line::from(current_spans));
    }

    let p = Paragraph::new(lines);
    frame.render_widget(p, inner);
}

fn render_spark(frame: &mut Frame<'_>, area: Rect, label: &str, color: Color, data: &[u64]) {
    if area.width < 20 {
        return;
    }

    let label_width = label.len() as u16 + 2;
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
        format!(" {label}"),
        Style::default().fg(color),
    ));
    frame.render_widget(label_p, chunks[0]);

    let current = data.last().copied().unwrap_or(0);
    let val_str = format_bytes(current);
    let val_p = Paragraph::new(Line::styled(
        format!("{val_str:>9} "),
        Style::default().fg(color),
    ));
    frame.render_widget(val_p, chunks[2]);

    let spark = Sparkline::default()
        .data(data)
        .style(Style::default().fg(color));
    frame.render_widget(spark, chunks[1]);
}

fn render_log_histogram(frame: &mut Frame<'_>, area: Rect, snap: &PollSnapshot) {
    let rows: Vec<Row> = if snap.log_top.is_empty() {
        vec![Row::new(vec!["(no log events)", ""])]
    } else {
        snap.log_top
            .iter()
            .map(|(msg, count)| {
                Row::new(vec![
                    Cell::from(truncate_str(msg, 60)),
                    Cell::from(count.to_string()),
                ])
            })
            .collect()
    };

    let table = Table::new(rows, [Constraint::Percentage(80), Constraint::Percentage(20)])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(" Log "),
        )
        .header(Row::new(vec!["message", "count"]).style(Style::default().fg(Color::DarkGray)));
    frame.render_widget(table, area);
}

fn render_help_bar(frame: &mut Frame<'_>, area: Rect, disconnected: bool) {
    let mut spans = vec![
        Span::styled(" [a]dd  [r]emove  [q]uit", Style::default().fg(Color::DarkGray)),
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

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        format!("{}...", &s[..max - 3])
    }
}
