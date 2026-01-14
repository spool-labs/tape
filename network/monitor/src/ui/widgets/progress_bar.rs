//! Epoch progress bar widget.
//!
//! Displays epoch progress with a visual bar and percentage label.
//! Handles different epoch phases (Syncing, Settling, Active) and low-quorum mode.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};
use tape_core::erasure::DATA_SLICES;

use crate::app::{App, EpochPhase};
use crate::theme::Theme;

/// Character for filled progress (Unicode block).
const FILLED_CHAR: char = '▓';

/// Character for empty progress (Unicode light shade).
const EMPTY_CHAR: char = '░';


/// Widget for rendering the epoch progress bar.
pub struct EpochProgress<'a> {
    /// Application state reference.
    app: &'a App,
    /// Theme reference.
    theme: &'a Theme,
}

impl<'a> EpochProgress<'a> {
    /// Create a new epoch progress widget.
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    /// Get the style for the current phase.
    fn phase_style(&self) -> Style {
        match self.app.phase {
            EpochPhase::Syncing => self.theme.syncing_style(),
            EpochPhase::Settling => Style::default().fg(ratatui::style::Color::Cyan),
            EpochPhase::Active => self.theme.success_style(),
            EpochPhase::Unknown => self.theme.unknown_style(),
        }
    }

    /// Build the phase info line.
    fn build_phase_info(&self) -> Line<'a> {
        let mut spans = vec![
            Span::styled("Phase: ", self.theme.text_style()),
            Span::styled(self.app.phase.as_str(), self.phase_style()),
        ];

        // Show different secondary info based on phase
        match self.app.phase {
            EpochPhase::Syncing => {
                spans.push(Span::styled(" | Attestations: ", self.theme.text_style()));
                spans.push(Span::styled(
                    format!("{}/{}", self.app.epoch_weight, DATA_SLICES),
                    self.theme.text_style(),
                ));
            }
            EpochPhase::Settling => {
                spans.push(Span::styled(" | Pools Advanced: ", self.theme.text_style()));
                spans.push(Span::styled(
                    format!("{}/{}", self.app.epoch_weight, DATA_SLICES),
                    self.theme.text_style(),
                ));
            }
            EpochPhase::Active | EpochPhase::Unknown => {
                spans.push(Span::styled(" | Time Left: ", self.theme.text_style()));
                spans.push(Span::styled(
                    self.app.time_remaining_display(),
                    self.theme.text_style(),
                ));
            }
        }

        spans.push(Span::styled(" | Slot: ", self.theme.text_style()));
        spans.push(Span::styled(
            format!("#{}", format_number(self.app.current_slot)),
            self.theme.text_style(),
        ));

        // Show next committee size if relevant
        if self.app.phase == EpochPhase::Active && self.app.committee_next_size > 0 {
            spans.push(Span::styled(" | Next: ", self.theme.dim_style()));
            spans.push(Span::styled(
                format!("{} nodes", self.app.committee_next_size),
                self.theme.dim_style(),
            ));
        }

        Line::from(spans)
    }

    /// Build the progress bar line.
    fn build_progress_bar(&self, width: u16) -> Line<'a> {
        let progress = self.app.epoch_progress();
        let label = self.app.epoch_progress_label();
        let label_text = format!(" {}% {}", progress, label);
        let bar_width = width.saturating_sub(label_text.len() as u16) as usize;

        let filled = (bar_width * progress as usize) / 100;
        let empty = bar_width.saturating_sub(filled);

        let filled_str: String = std::iter::repeat(FILLED_CHAR).take(filled).collect();
        let empty_str: String = std::iter::repeat(EMPTY_CHAR).take(empty).collect();

        // Color based on phase
        let bar_color = match self.app.phase {
            EpochPhase::Syncing => ratatui::style::Color::Yellow,
            EpochPhase::Settling => ratatui::style::Color::Cyan,
            EpochPhase::Active => self.theme.progress_fg,
            EpochPhase::Unknown => self.theme.progress_fg,
        };

        Line::from(vec![
            Span::styled(filled_str, Style::default().fg(bar_color)),
            Span::styled(empty_str, Style::default().fg(self.theme.progress_bg)),
            Span::styled(label_text, self.theme.text_style()),
        ])
    }
}

impl Widget for EpochProgress<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Build title with low-quorum warning if applicable
        let title = if self.app.is_low_quorum {
            format!(" EPOCH {} [LOW-QUORUM MODE] ", self.app.epoch.0)
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

        if inner.height < 2 || inner.width < 30 {
            return;
        }

        let lines = vec![
            self.build_phase_info(),
            self.build_progress_bar(inner.width),
        ];

        let paragraph = Paragraph::new(lines);
        paragraph.render(inner, buf);
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
    // phase info + progress bar + border
    4
}
