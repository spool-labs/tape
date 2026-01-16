//! Dashboard view - main monitoring view.
//!
//! Renders the complete dashboard layout with all widgets.

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

use crate::app::{App, EpochPhase};
use crate::theme::Theme;
use crate::ui::widgets::{EpochProgress, EventLog, NodeGrid, SpoolBar, SpoolHighlight};

/// Widget for rendering the complete dashboard.
pub struct Dashboard<'a> {
    /// Application state reference.
    app: &'a App,
    /// Theme reference.
    theme: &'a Theme,
}

impl<'a> Dashboard<'a> {
    /// Create a new dashboard widget.
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    /// Build the header bar with keybindings on the right.
    fn render_header(&self, area: Rect, buf: &mut Buffer) {
        use ratatui::style::Modifier;

        let logo = "⊙⊙";
        let title = "TAPEDRIVE";

        // Determine status text and style
        let (status_text, status_style) = if !self.app.rpc_connected {
            ("[Disconnected]", self.theme.error_style())
        } else if !self.app.fetch_errors.is_empty() {
            (
                "[Partial]",
                Style::default().fg(self.theme.warning),
            )
        } else {
            ("[OK]", self.theme.success_style())
        };

        // Build keybindings string
        let keybindings = "q:Quit n:Nodes l:Log e:Epoch ?:Help";

        // Calculate spacing (padding + logo + space + title + space + status + padding)
        let left_len = 1 + logo.chars().count() + 1 + title.len() + 1 + status_text.len();
        let right_len = keybindings.len() + 1;
        let padding = (area.width as usize).saturating_sub(left_len + right_len + 4);
        let padding_str: String = std::iter::repeat(' ').take(padding).collect();

        let spans = vec![
            Span::raw(" "),  // Left padding
            Span::styled(logo, Style::default().fg(ratatui::style::Color::White).add_modifier(Modifier::BOLD)),
            Span::raw(" "),
            Span::styled(title, self.theme.title_style()),
            Span::raw(" "),
            Span::styled(status_text, status_style),
            Span::raw(padding_str),
            Span::styled(keybindings, self.theme.dim_style()),
            Span::raw(" "),  // Right padding
        ];

        let line = Line::from(spans);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        Paragraph::new(line).render(inner, buf);
    }

    /// Build the network stats panel with compact horizontal layout.
    fn render_network_stats(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .title(Span::styled(" NETWORK ", self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 1 || inner.width < 40 {
            return;
        }

        let mut lines = Vec::new();
        let max_lines = inner.height as usize;

        // Effective width accounting for horizontal padding
        let content_width = (inner.width as usize).saturating_sub(2);

        // Line 1: Storage label + progress bar + percentage (all on one line)
        if lines.len() < max_lines {
            let storage_pct = self.app.stats.storage_percentage();
            let storage_display = self.app.stats.storage_display();
            let label = format!("Storage: {} ", storage_display);
            let pct_str = format!(" {}% ", storage_pct);

            // Calculate bar width
            let bar_width = content_width.saturating_sub(label.len() + pct_str.len()).max(5);
            let filled = (bar_width * storage_pct as usize) / 100;
            let empty = bar_width.saturating_sub(filled);
            let filled_str: String = std::iter::repeat('█').take(filled).collect();
            let empty_str: String = std::iter::repeat('░').take(empty).collect();

            lines.push(Line::from(vec![
                Span::raw(" "),  // Left padding
                Span::styled(label, self.theme.text_style()),
                Span::styled(filled_str, Style::default().fg(self.theme.progress_fg)),
                Span::styled(empty_str, Style::default().fg(self.theme.progress_bg)),
                Span::styled(pct_str, self.theme.text_style()),
            ]));
        }

        // Line 2: Tracks/Tapes on left, Rewards/Stake on right
        if lines.len() < max_lines {
            let tracks = format!("{}", format_number(self.app.stats.tracks_certified));
            let tapes = format!("{}", format_number(self.app.stats.tapes_active));
            let rewards = format_tape(self.app.stats.rewards_pool);
            let total_stake = format_tape(self.app.total_committee_stake().0);

            let left_stats = format!("Tracks: {}  Tapes: {}", tracks, tapes);
            let right_stats = format!("Rewards: {}  Stake: {}", rewards, total_stake);

            let total_len = left_stats.len() + right_stats.len();
            let padding = content_width.saturating_sub(total_len);
            let padding_str: String = std::iter::repeat(' ').take(padding).collect();

            lines.push(Line::from(vec![
                Span::raw(" "),  // Left padding
                Span::styled(left_stats, self.theme.text_style()),
                Span::raw(padding_str),
                Span::styled(right_stats, Style::default().fg(self.theme.primary)),
                Span::raw(" "),  // Right padding
            ]));
        }

        // Line 3: Traffic and requests
        if lines.len() < max_lines {
            let mut spans = Vec::new();

            if self.app.stats.upload_throughput > 0 || self.app.stats.download_throughput > 0 {
                spans.push(Span::styled("Traffic: ", self.theme.text_style()));
                spans.push(Span::styled(
                    format!("↑{} ", format_bytes_per_sec(self.app.stats.upload_throughput)),
                    Style::default().fg(ratatui::style::Color::Blue),
                ));
                spans.push(Span::styled(
                    format!("↓{}", format_bytes_per_sec(self.app.stats.download_throughput)),
                    Style::default().fg(ratatui::style::Color::Magenta),
                ));
            } else {
                spans.push(Span::styled("Traffic: --", self.theme.dim_style()));
            }

            spans.push(Span::raw("  "));

            if self.app.stats.requests_per_sec > 0 {
                spans.push(Span::styled(
                    format!("Reqs: {}/s", format_number(self.app.stats.requests_per_sec as u64)),
                    self.theme.text_style(),
                ));
            } else {
                spans.push(Span::styled("Reqs: --", self.theme.dim_style()));
            }

            // Insert left padding at beginning
            spans.insert(0, Span::raw(" "));

            lines.push(Line::from(spans));
        }

        Paragraph::new(lines).render(inner, buf);
    }
}

impl Widget for Dashboard<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Main vertical layout (no status bar)
        let main_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),   // Header
                Constraint::Length(4),   // Epoch progress
                Constraint::Length(5),   // NETWORK stats (more compact)
                Constraint::Length(14),  // Three committees
                Constraint::Length(11), // Spool distribution (+1)
                Constraint::Min(7),      // Log (-1)
            ])
            .split(area);

        // Render header with keybindings
        self.render_header(main_chunks[0], buf);

        // Render epoch progress
        EpochProgress::new(self.app, self.theme).render(main_chunks[1], buf);

        // Render full-width network stats
        self.render_network_stats(main_chunks[2], buf);

        // Split committees into three columns
        let committee_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(33),
                Constraint::Percentage(34),
                Constraint::Percentage(33),
            ])
            .split(main_chunks[3]);

        // Render PREV committee
        NodeGrid::new(self.theme)
            .nodes(&self.app.committee_prev_nodes)
            .title("PREV")
            .render(committee_chunks[0], buf);

        // Render CURRENT committee
        NodeGrid::new(self.theme)
            .nodes(&self.app.nodes)
            .title("CURRENT")
            .render(committee_chunks[1], buf);

        // Render NEXT committee
        NodeGrid::new(self.theme)
            .nodes(&self.app.committee_next_nodes)
            .title("NEXT")
            .render(committee_chunks[2], buf);

        // Render spool distribution (only if we have committee members)
        let has_committee = !self.app.nodes.is_empty();
        let highlight = match (&self.app.spools_prev, &self.app.spools_current) {
            (Some(prev), Some(curr)) if self.app.phase == EpochPhase::Syncing && has_committee => {
                SpoolHighlight::ShowChanges {
                    spools_prev: prev,
                    spools_current: curr,
                }
            }
            (_, Some(curr)) if has_committee => {
                SpoolHighlight::Normal { spools: curr }
            }
            _ => SpoolHighlight::Unavailable,
        };

        SpoolBar::new(self.theme).highlight(highlight).render(main_chunks[4], buf);

        // Render log (renamed from events)
        EventLog::new(self.app, self.theme).render(main_chunks[5], buf);
    }
}

/// Format a number with thousand separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result
}

/// Format TAPE amount (flux units to display).
/// Returns formatted string with unit (e.g., "123 TAPE" or "456 μTAPE").
fn format_tape(flux: u64) -> String {
    if flux >= 1_000_000 {
        // 1 TAPE or more - show in TAPE
        let tape = flux / 1_000_000;
        format!("{} TAPE", format_number(tape))
    } else if flux > 0 {
        // Less than 1 TAPE - show in μTAPE (micro TAPE)
        format!("{} μTAPE", format_number(flux))
    } else {
        "0 TAPE".to_string()
    }
}

/// Format bytes per second.
fn format_bytes_per_sec(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB/s", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB/s", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB/s", bytes as f64 / 1_000.0)
    } else {
        format!("{} B/s", bytes)
    }
}
