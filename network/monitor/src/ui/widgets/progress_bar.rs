//! Epoch progress bar widget.
//!
//! Displays epoch progress with a visual bar and percentage label.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

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
        Line::from(vec![
            Span::styled("Phase: ", self.theme.text_style()),
            Span::styled(self.app.phase.as_str(), self.phase_style()),
            Span::styled(" | Time Left: ", self.theme.text_style()),
            Span::styled(
                self.app.time_remaining_display(),
                self.theme.text_style(),
            ),
            Span::styled(" | Slot: ", self.theme.text_style()),
            Span::styled(
                format!("#{}", format_number(self.app.current_slot)),
                self.theme.text_style(),
            ),
        ])
    }

    /// Build the progress bar line.
    fn build_progress_bar(&self, width: u16) -> Line<'a> {
        let progress = self.app.epoch_progress();
        let bar_width = width.saturating_sub(15) as usize; // Leave room for label

        let filled = (bar_width * progress as usize) / 100;
        let empty = bar_width.saturating_sub(filled);

        let filled_str: String = std::iter::repeat(FILLED_CHAR).take(filled).collect();
        let empty_str: String = std::iter::repeat(EMPTY_CHAR).take(empty).collect();

        Line::from(vec![
            Span::styled(filled_str, Style::default().fg(self.theme.progress_fg)),
            Span::styled(empty_str, Style::default().fg(self.theme.progress_bg)),
            Span::styled(format!(" {}% elapsed", progress), self.theme.text_style()),
        ])
    }
}

impl Widget for EpochProgress<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = format!(" EPOCH {} ", self.app.epoch.0);
        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

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
