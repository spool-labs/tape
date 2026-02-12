//! Epoch progress bar widget.
//!
//! Displays epoch phase and progress as a visual bar.
//! Phases: Syncing → Settling → Active → (advance) → Syncing ...

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Widget},
};

use crate::app::{App, EpochPhase};
use crate::theme::Theme;

const FILLED: &str = "█";
const EMPTY: &str = "░";

/// Widget for rendering the epoch progress bar.
pub struct EpochProgress<'a> {
    app: &'a App,
    theme: &'a Theme,
}

impl<'a> EpochProgress<'a> {
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    fn phase_color(&self) -> ratatui::style::Color {
        match self.app.phase {
            EpochPhase::Syncing => ratatui::style::Color::Yellow,
            EpochPhase::Settling => ratatui::style::Color::Cyan,
            EpochPhase::Active => ratatui::style::Color::Green,
            EpochPhase::Unknown => self.theme.unknown,
        }
    }
}

impl Widget for EpochProgress<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = if self.app.is_low_quorum {
            format!(" EPOCH {} [LOW QUORUM] ", self.app.epoch.0)
        } else {
            format!(" EPOCH {} ", self.app.epoch.0)
        };

        let title_style = if self.app.is_low_quorum {
            self.theme.warning_style()
        } else {
            self.theme.header_style()
        };

        let border_style = if self.app.is_low_quorum {
            Style::default().fg(self.theme.warning)
        } else {
            self.theme.border_style()
        };

        let block = Block::default()
            .title(Span::styled(title, title_style))
            .borders(Borders::ALL)
            .border_style(border_style);

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height == 0 || inner.width < 20 {
            return;
        }

        let w = inner.width as usize;
        let phase_color = self.phase_color();
        let progress = self.app.epoch_progress();
        let threshold = self.app.supermajority_threshold();

        // Line 1: phase + contextual info
        let info_line: Line = match self.app.phase {
            EpochPhase::Syncing => Line::from(vec![
                Span::raw(" "),
                Span::styled("SYNCING", Style::default().fg(phase_color)),
                Span::styled(
                    format!("  spools: {}/{}", self.app.epoch_weight, threshold),
                    self.theme.text_style(),
                ),
                Span::styled(
                    format!("  ({}%)", progress),
                    self.theme.dim_style(),
                ),
                Span::styled(
                    format!("  committee: {} nodes", self.app.committee_size()),
                    self.theme.dim_style(),
                ),
            ]),
            EpochPhase::Settling => Line::from(vec![
                Span::raw(" "),
                Span::styled("SETTLING", Style::default().fg(phase_color)),
                Span::styled(
                    format!("  spools: {}/{}", self.app.epoch_weight, threshold),
                    self.theme.text_style(),
                ),
                Span::styled(
                    format!("  ({}%)", progress),
                    self.theme.dim_style(),
                ),
                Span::styled(
                    format!("  next: {} nodes", self.app.committee_next_size),
                    self.theme.dim_style(),
                ),
            ]),
            EpochPhase::Active => {
                let time_left = self.app.time_remaining_display();
                let mut spans = vec![
                    Span::raw(" "),
                    Span::styled("ACTIVE", Style::default().fg(phase_color)),
                    Span::styled(format!("  {}", time_left), self.theme.text_style()),
                    Span::styled(
                        format!("  ({}%)", progress),
                        self.theme.dim_style(),
                    ),
                ];
                if self.app.committee_next_size > 0 {
                    spans.push(Span::styled(
                        format!("  next: {} nodes", self.app.committee_next_size),
                        self.theme.dim_style(),
                    ));
                }
                spans.push(Span::styled(
                    format!("  slot: #{}", format_number(self.app.current_slot)),
                    self.theme.dim_style(),
                ));
                Line::from(spans)
            }
            EpochPhase::Unknown => Line::from(vec![
                Span::raw(" "),
                Span::styled("UNKNOWN", Style::default().fg(phase_color)),
                Span::styled(
                    format!("  slot: #{}", format_number(self.app.current_slot)),
                    self.theme.dim_style(),
                ),
            ]),
        };

        // Render info line at row 0
        let info_area = Rect::new(inner.x, inner.y, inner.width, 1);
        buf.set_line(info_area.x, info_area.y, &info_line, info_area.width);

        if inner.height < 2 {
            return;
        }

        // Line 2: progress bar
        let bar_width = w.saturating_sub(2); // 1 char padding each side
        let filled_count = if bar_width > 0 {
            (bar_width * progress as usize) / 100
        } else {
            0
        };
        let empty_count = bar_width.saturating_sub(filled_count);

        let filled_str: String = FILLED.repeat(filled_count);
        let empty_str: String = EMPTY.repeat(empty_count);

        let bar_line = Line::from(vec![
            Span::raw(" "),
            Span::styled(filled_str, Style::default().fg(phase_color)),
            Span::styled(empty_str, Style::default().fg(self.theme.progress_bg)),
            Span::raw(" "),
        ]);

        let bar_area = Rect::new(inner.x, inner.y + 1, inner.width, 1);
        buf.set_line(bar_area.x, bar_area.y, &bar_line, bar_area.width);
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

/// Calculate required height for the epoch progress widget.
pub fn required_height() -> u16 {
    4 // border + info line + bar + border
}
