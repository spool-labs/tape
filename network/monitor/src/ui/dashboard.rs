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

use crate::app::App;
use crate::theme::Theme;
use crate::ui::widgets::{EpochProgress, EventLog, NodeGrid, SpoolBar};

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

    /// Build the header bar.
    fn render_header(&self, area: Rect, buf: &mut Buffer) {
        let title = "TAPEDRIVE NETWORK MONITOR";
        let help_hint = "Press ? for help";

        // Determine status text and style
        let (status_text, status_style) = if !self.app.rpc_connected {
            ("[Disconnected]", self.theme.error_style())
        } else if !self.app.fetch_errors.is_empty() {
            // Show partial/warning state when we have errors but are connected
            (
                "[Partial Data]",
                Style::default().fg(self.theme.warning),
            )
        } else {
            ("[Connected]", self.theme.success_style())
        };

        // Add error count if we have errors
        let error_indicator = if !self.app.fetch_errors.is_empty() && self.app.rpc_connected {
            format!(" ({} missing)", self.app.fetch_errors.len())
        } else {
            String::new()
        };

        // Calculate spacing
        let left_len = title.len() + status_text.len() + error_indicator.len() + 2;
        let right_len = help_hint.len();
        let padding = (area.width as usize).saturating_sub(left_len + right_len + 4);
        let padding_str: String = std::iter::repeat(' ').take(padding).collect();

        let mut spans = vec![
            Span::styled(title, self.theme.title_style()),
            Span::raw(" "),
            Span::styled(status_text, status_style),
        ];

        if !error_indicator.is_empty() {
            spans.push(Span::styled(error_indicator, self.theme.dim_style()));
        }

        spans.push(Span::raw(padding_str));
        spans.push(Span::styled(help_hint, self.theme.dim_style()));

        let line = Line::from(spans);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        Paragraph::new(line).render(inner, buf);
    }

    /// Build the network stats panel.
    fn render_network_stats(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .title(Span::styled(" NETWORK STATS ", self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 5 || inner.width < 30 {
            return;
        }

        // Build mini progress bar for storage
        let storage_pct = self.app.stats.storage_percentage();
        let bar_width = 6;
        let filled = (bar_width * storage_pct as usize) / 100;
        let empty = bar_width - filled;
        let filled_str: String = std::iter::repeat('█').take(filled).collect();
        let empty_str: String = std::iter::repeat('░').take(empty).collect();

        let lines = vec![
            Line::from(vec![
                Span::styled("Storage:    ", self.theme.text_style()),
                Span::styled(self.app.stats.storage_display(), self.theme.text_style()),
                Span::raw("  ["),
                Span::styled(filled_str, Style::default().fg(self.theme.progress_fg)),
                Span::styled(empty_str, Style::default().fg(self.theme.progress_bg)),
                Span::raw("] "),
                Span::styled(format!("{}%", storage_pct), self.theme.text_style()),
            ]),
            Line::from(vec![
                Span::styled("Tracks:     ", self.theme.text_style()),
                Span::styled(
                    format!("{} certified", format_number(self.app.stats.tracks_certified)),
                    self.theme.text_style(),
                ),
            ]),
            Line::from(vec![
                Span::styled("Tapes:      ", self.theme.text_style()),
                Span::styled(
                    format!("{} active", format_number(self.app.stats.tapes_active)),
                    self.theme.text_style(),
                ),
            ]),
            Line::from(vec![
                Span::styled("Rewards:    ", self.theme.text_style()),
                Span::styled(
                    format!("{} TAPE", format_tape(self.app.stats.rewards_pool)),
                    Style::default().fg(self.theme.primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("Paid Out:   ", self.theme.text_style()),
                Span::styled(
                    format!("{} TAPE", format_tape(self.app.stats.rewards_paid)),
                    self.theme.text_style(),
                ),
            ]),
            Line::default(),
            // Throughput and requests require node metrics endpoints (not yet implemented)
            if self.app.stats.upload_throughput > 0 || self.app.stats.download_throughput > 0 {
                Line::from(vec![
                    Span::styled("Throughput: ", self.theme.text_style()),
                    Span::styled(
                        format!("^ {} ", format_bytes_per_sec(self.app.stats.upload_throughput)),
                        Style::default().fg(ratatui::style::Color::Blue),
                    ),
                    Span::styled(
                        format!("v {}", format_bytes_per_sec(self.app.stats.download_throughput)),
                        Style::default().fg(ratatui::style::Color::Magenta),
                    ),
                ])
            } else {
                Line::from(vec![
                    Span::styled("Throughput: ", self.theme.text_style()),
                    Span::styled("N/A", self.theme.dim_style()),
                ])
            },
            if self.app.stats.requests_per_sec > 0 {
                Line::from(vec![
                    Span::styled("Requests:   ", self.theme.text_style()),
                    Span::styled(
                        format!("{} req/s", format_number(self.app.stats.requests_per_sec as u64)),
                        self.theme.text_style(),
                    ),
                ])
            } else {
                Line::from(vec![
                    Span::styled("Requests:   ", self.theme.text_style()),
                    Span::styled("N/A", self.theme.dim_style()),
                ])
            },
        ];

        Paragraph::new(lines).render(inner, buf);
    }

    /// Build the status bar.
    fn render_status_bar(&self, area: Rect, buf: &mut Buffer) {
        let keybindings = vec![
            ("q", "Quit"),
            ("n", "Nodes"),
            ("t", "Tracks"),
            ("e", "Epoch"),
            ("/", "Search"),
            ("?", "Help"),
        ];

        let mut spans = Vec::new();
        for (i, (key, desc)) in keybindings.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("  "));
            }
            spans.push(Span::styled(*key, self.theme.keybind_style()));
            spans.push(Span::styled(format!(":{}", desc), self.theme.keybind_desc_style()));
        }

        // Add connection status and refresh timing on the right
        let status_text = if !self.app.rpc_connected {
            "Reconnecting...".to_string()
        } else {
            format!("@ {}", self.app.last_refresh_display())
        };
        let status_style = if !self.app.rpc_connected {
            self.theme.warning_style()
        } else {
            self.theme.dim_style()
        };

        let left_width: usize = spans.iter().map(|s| s.content.len()).sum();
        let padding = (area.width as usize).saturating_sub(left_width + status_text.len() + 4);
        let padding_str: String = std::iter::repeat(' ').take(padding).collect();

        spans.push(Span::raw(padding_str));
        spans.push(Span::styled(status_text, status_style));

        let line = Line::from(spans);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        Paragraph::new(line).render(inner, buf);
    }
}

impl Widget for Dashboard<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Main vertical layout
        let main_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),   // Header
                Constraint::Length(4),   // Epoch progress
                Constraint::Length(12),  // Committee + Stats
                Constraint::Length(10),  // Spool distribution (1024 spools ≈ 8 rows + border)
                Constraint::Min(6),      // Event log
                Constraint::Length(3),   // Status bar
            ])
            .split(area);

        // Render header
        self.render_header(main_chunks[0], buf);

        // Render epoch progress
        EpochProgress::new(self.app, self.theme).render(main_chunks[1], buf);

        // Split middle section into committee (left) and stats (right)
        let middle_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(50),
                Constraint::Percentage(50),
            ])
            .split(main_chunks[2]);

        // Render committee grid
        NodeGrid::new(self.app, self.theme).render(middle_chunks[0], buf);

        // Render network stats
        self.render_network_stats(middle_chunks[1], buf);

        // Render spool distribution
        SpoolBar::new(self.app, self.theme).render(main_chunks[3], buf);

        // Render event log
        EventLog::new(self.app, self.theme).render(main_chunks[4], buf);

        // Render status bar
        self.render_status_bar(main_chunks[5], buf);
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
fn format_tape(flux: u64) -> String {
    let tape = flux / 1_000_000; // Convert from flux (6 decimals)
    format_number(tape)
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
