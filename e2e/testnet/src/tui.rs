use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::ArcSwap;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::{Frame, Terminal};
use tape_core::erasure::GROUP_SIZE;

use crate::view::{NodeView, TestnetView, UploadView};

const GROUP_COLS: usize = 7;
const GROUP_ROWS: usize = 3;

pub enum Command {
    AddNode,
    RemoveNode,
    UploadBlob,
    Quit,
}

pub fn run_tui(
    snapshot: Arc<ArcSwap<TestnetView>>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<Command>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let mut terminal = init_terminal()?;
    let _guard = TerminalDropGuard;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

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
    Ok(Terminal::new(backend)?)
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

fn render_frame(frame: &mut Frame<'_>, view: &TestnetView, disconnected: bool) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(Color::Rgb(0, 0, 0))),
        area,
    );

    let spool_inner_w = area.width.saturating_sub(2) as usize;
    let groups_per_row = ((spool_inner_w + 1) / (GROUP_COLS + 1)).max(1);
    let group_count = view.spools.len().div_ceil(GROUP_SIZE).max(1);
    let bands = group_count.div_ceil(groups_per_row);
    let spool_grid_height = (bands * (GROUP_ROWS + 1)) as u16 + 2;
    let capped_spool_h = spool_grid_height.min(area.height.saturating_sub(10)).max(5);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(capped_spool_h),
            Constraint::Min(16),
            Constraint::Length(1),
        ])
        .split(area);

    let body_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(8)])
        .split(chunks[2]);

    render_title_bar(frame, chunks[0], view);
    render_spool_grid(frame, chunks[1], view);
    render_node_table(frame, body_chunks[0], view);
    render_upload_table(frame, body_chunks[1], view);
    render_help_bar(frame, chunks[3], view, disconnected);
}

fn render_title_bar(frame: &mut Frame<'_>, area: Rect, view: &TestnetView) {
    let healthy_nodes = view.nodes.iter().filter(|node| node.healthy).count();
    let metrics_nodes = view
        .nodes
        .iter()
        .filter(|node| node.metrics_available)
        .count();

    let phase_color = match view.cluster.phase.as_str() {
        "Sync" => Color::Cyan,
        "Snapshot" => Color::Yellow,
        "Active" => Color::Green,
        "Closing" => Color::Magenta,
        _ => Color::DarkGray,
    };

    let phase = match view.cluster.phase_weight {
        Some(weight) => format!("{}(w:{weight})", view.cluster.phase),
        None => view.cluster.phase.clone(),
    };

    let left = format!(
        " TESTNET  Nodes:{}  Healthy:{}  Metrics:{}  Groups:{}  C[{}/{}/{}]  {}",
        view.nodes.len(),
        healthy_nodes,
        metrics_nodes,
        view.cluster.live_group_count,
        view.cluster.committee_prev_size,
        view.cluster.committee_size,
        view.cluster.committee_next_size,
        phase,
    );
    let right = format!(
        "epoch:{}  registered:{}  slot:{} ",
        view.cluster.epoch,
        view.cluster.total_nodes_registered,
        view.cluster.slot,
    );

    let gap = (area.width as usize).saturating_sub(left.len() + right.len());
    let line = Line::from(vec![
        Span::styled(left, Style::default().fg(phase_color)),
        Span::raw(" ".repeat(gap)),
        Span::styled(right, Style::default().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_spool_grid(frame: &mut Frame<'_>, area: Rect, view: &TestnetView) {
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
    let group_count = view.spools.len().div_ceil(GROUP_SIZE);
    let bands = group_count.div_ceil(groups_per_row);
    let mut lines = Vec::new();

    if group_count == 0 {
        frame.render_widget(
            Paragraph::new(Line::styled(
                "(no live groups)",
                Style::default().fg(Color::DarkGray),
            )),
            pad_left(inner),
        );
        return;
    }

    for band in 0..bands {
        let mut label_spans = Vec::new();
        for col in 0..groups_per_row {
            let group = band * groups_per_row + col;
            if group >= group_count {
                break;
            }
            label_spans.push(Span::styled(
                format!("{:^width$}", group, width = GROUP_COLS),
                Style::default().fg(Color::DarkGray),
            ));
            label_spans.push(Span::raw(" "));
        }
        lines.push(Line::from(label_spans));

        for row in 0..GROUP_ROWS {
            let mut spans = Vec::new();
            for col in 0..groups_per_row {
                let group = band * groups_per_row + col;
                if group >= group_count {
                    break;
                }

                for cell in 0..GROUP_COLS {
                    let spool_in_group = row * GROUP_COLS + cell;
                    if spool_in_group >= GROUP_SIZE {
                        spans.push(Span::raw(" "));
                        continue;
                    }

                    let spool_idx = group * GROUP_SIZE + spool_in_group;
                    let Some(spool) = view.spools.get(spool_idx) else {
                        spans.push(Span::raw(" "));
                        continue;
                    };

                    let (glyph, style) = match spool.owner_local_id {
                        Some(local_id) => (
                            "▌",
                            Style::default().fg(node_color(local_id)),
                        ),
                        None if spool.owner_node.is_some() => {
                            ("▌", Style::default().fg(Color::DarkGray))
                        }
                        None => ("·", Style::default().fg(Color::Red)),
                    };
                    spans.push(Span::styled(glyph, style));
                }

                spans.push(Span::raw(" "));
            }
            lines.push(Line::from(spans));
        }
    }

    frame.render_widget(Paragraph::new(lines), pad_left(inner));
}

fn render_node_table(frame: &mut Frame<'_>, area: Rect, view: &TestnetView) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Nodes ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut nodes: Vec<&NodeView> = view.nodes.iter().collect();
    nodes.sort_by_key(|node| node.local_id);

    let rows = nodes.into_iter().map(|node| {
        let stats = node.stats.as_ref();
        let healthy = if node.healthy { "up" } else { "down" };
        let metrics = if node.metrics_available { "yes" } else { "no" };

        Row::new(vec![
            Cell::from("▌").style(Style::default().fg(node_color(node.local_id))),
            Cell::from(
                node.node_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(
                node.address
                    .as_ref()
                    .and_then(|addr| addr.rsplit(':').next())
                    .unwrap_or("-")
                    .to_string(),
            ),
            Cell::from(node.address.clone().unwrap_or_else(|| "-".into())),
            Cell::from(healthy),
            Cell::from(metrics),
            Cell::from(
                stats.map(|s| s.owned_spools.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(
                stats.map(|s| s.tracks_stored.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(
                stats.map(|s| format_bytes(s.slice_payload_bytes))
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(
                stats.map(|s| format_bytes(s.store_disk_bytes))
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(
                node.pool_stake
                    .map(format_tape)
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(short_pubkey(&node.authority)),
            Cell::from(node.node_address.clone()),
        ])
    });

    let widths = [
        Constraint::Length(3),
        Constraint::Length(6),
        Constraint::Length(6),
        Constraint::Length(17),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Length(12),
        Constraint::Min(44),
    ];

    let table = Table::new(
        rows,
        widths,
    )
    .header(
        Row::new(vec![
            "",
            "Id",
            "Port",
            "Address",
            "Health",
            "Metrics",
            "Spools",
            "Tracks",
            "Payload",
            "Disk",
            "Stake",
            "Authority",
            "Node",
        ])
        .style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(table, inner);
}

fn render_help_bar(frame: &mut Frame<'_>, area: Rect, view: &TestnetView, disconnected: bool) {
    let status = if disconnected { "disconnected" } else { "ready" };
    let mut spans = vec![
        Span::styled(" a ", Style::default().fg(Color::Green)),
        Span::raw("add node  "),
        Span::styled(" r ", Style::default().fg(Color::Yellow)),
        Span::raw("remove last node  "),
        Span::styled(" u ", Style::default().fg(Color::Cyan)),
        Span::raw("upload blob  "),
        Span::styled(" q ", Style::default().fg(Color::Red)),
        Span::raw("quit"),
        Span::raw(format!("  [{status}]")),
    ];

    if let Some(upload) = view.uploads.first() {
        if let Some(error) = upload.last_error.as_deref() {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                truncate_tail(&format!("upload: {error}"), area.width.saturating_sub(40) as usize),
                Style::default().fg(Color::Red),
            ));
        } else if upload.cert_status == "pending" || upload.cert_status == "retry" {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                format!("upload: {}", upload.cert_status),
                Style::default().fg(Color::Yellow),
            ));
        }
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_upload_table(frame: &mut Frame<'_>, area: Rect, view: &TestnetView) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Uploads ");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = view.uploads.iter().map(upload_row);

    let widths = [
        Constraint::Length(44),
        Constraint::Length(8),
        Constraint::Length(7),
        Constraint::Min(44),
    ];

    let table = Table::new(rows, widths).header(
        Row::new(vec!["Tape", "Size", "Cert", "Track"])
            .style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(table, inner);
}

fn upload_row(upload: &UploadView) -> Row<'static> {
    Row::new(vec![
        Cell::from(upload.tape_address.clone()),
        Cell::from(format_bytes(upload.size_bytes)),
        Cell::from(upload.cert_status.clone()).style(cert_style(&upload.cert_status)),
        Cell::from(upload.track_address.clone().unwrap_or_else(|| "-".into())),
    ])
}

fn pad_left(area: Rect) -> Rect {
    Rect {
        x: area.x + 1,
        width: area.width.saturating_sub(1),
        ..area
    }
}

fn short_pubkey(value: &str) -> String {
    if value.len() <= 12 {
        return value.to_string();
    }
    format!("{}..{}", &value[..4], &value[value.len() - 4..])
}

fn truncate_tail(value: &str, max_len: usize) -> String {
    if max_len == 0 || value.len() <= max_len {
        return value.to_string();
    }
    if max_len <= 3 {
        return ".".repeat(max_len);
    }
    format!("{}...", &value[..max_len - 3])
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let bytes = bytes as f64;
    if bytes >= GB {
        format!("{:.1}G", bytes / GB)
    } else if bytes >= MB {
        format!("{:.1}M", bytes / MB)
    } else if bytes >= KB {
        format!("{:.1}K", bytes / KB)
    } else {
        format!("{:.0}B", bytes)
    }
}

fn format_tape(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.1}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn cert_style(status: &str) -> Style {
    match status {
        "yes" => Style::default().fg(Color::Green),
        "pending" => Style::default().fg(Color::Yellow),
        "failed" => Style::default().fg(Color::Red),
        _ => Style::default().fg(Color::DarkGray),
    }
}

fn node_color(index: usize) -> Color {
    let golden = 0.618_033_988_749_895_f64;
    let h = (((index + 1) as f64) * golden).fract();
    let s = match index % 3 {
        0 => 0.85,
        1 => 0.65,
        _ => 0.75,
    };
    let l = match (index / 3) % 3 {
        0 => 0.55,
        1 => 0.45,
        _ => 0.65,
    };
    hsl_to_rgb(h, s, l)
}

fn hsl_to_rgb(h: f64, s: f64, l: f64) -> Color {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((h * 6.0) % 2.0 - 1.0).abs());
    let m = l - c / 2.0;

    let (r1, g1, b1) = match (h * 6.0) as u32 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };

    Color::Rgb(
        ((r1 + m) * 255.0) as u8,
        ((g1 + m) * 255.0) as u8,
        ((b1 + m) * 255.0) as u8,
    )
}
