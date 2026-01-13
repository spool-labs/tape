//! Epoch history view.
//!
//! Shows a historical list of epochs with committee size and stake information.

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Widget},
};

use crate::app::App;
use crate::theme::Theme;

/// Widget for rendering the epoch history view.
pub struct EpochHistoryView<'a> {
    app: &'a App,
    theme: &'a Theme,
}

impl<'a> EpochHistoryView<'a> {
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    fn build_header_line(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled(" EPOCH HISTORY ", self.theme.title_style()),
        ])
    }

    fn build_hint_line(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled("ESC", self.theme.keybind_style()),
            Span::styled(": Back  ", self.theme.keybind_desc_style()),
            Span::styled("↑↓", self.theme.keybind_style()),
            Span::styled(": Navigate  ", self.theme.keybind_desc_style()),
            Span::styled("Enter", self.theme.keybind_style()),
            Span::styled(": Committee details", self.theme.keybind_desc_style()),
        ])
    }

    fn build_table(&self) -> Table<'a> {
        let header_cells = ["EPOCH", "PHASE", "COMMITTEE", "STAKE", "STARTED", "DURATION"]
            .iter()
            .map(|h| Cell::from(*h).style(self.theme.header_style()));
        let header = Row::new(header_cells).height(1);

        // Generate demo epoch history based on current epoch
        let current_epoch = self.app.epoch.0;
        let mut rows = Vec::new();

        for i in 0..10 {
            let epoch_num = current_epoch.saturating_sub(i);
            if epoch_num == 0 && i > 0 {
                break;
            }

            let is_current = i == 0;
            let is_selected = self.app.scroll_offset == i as usize;

            let phase = if is_current {
                match self.app.phase {
                    crate::app::EpochPhase::Active => "Active",
                    crate::app::EpochPhase::Syncing => "Syncing",
                    crate::app::EpochPhase::Settling => "Settling",
                    crate::app::EpochPhase::Unknown => "Unknown",
                }
            } else {
                "Complete"
            };

            let phase_style = if is_current {
                match self.app.phase {
                    crate::app::EpochPhase::Active => self.theme.success_style(),
                    crate::app::EpochPhase::Syncing => self.theme.syncing_style(),
                    crate::app::EpochPhase::Settling => ratatui::style::Style::default()
                        .fg(ratatui::style::Color::Cyan),
                    crate::app::EpochPhase::Unknown => self.theme.unknown_style(),
                }
            } else {
                self.theme.dim_style()
            };

            // Demo data for committee sizes (decreasing as we go back)
            let committee_size = self.app.nodes.len().saturating_sub(i as usize * 2).max(80);
            let stake = format!(
                "{:.1}M TAPE",
                (4.2 - i as f64 * 0.1).max(3.5)
            );

            // Demo dates (going back 7 days per epoch)
            let days_ago = i * 7;
            let started = if days_ago == 0 {
                format!("{}", chrono_lite_date_str(0))
            } else {
                format!("{}", chrono_lite_date_str(days_ago as i64))
            };

            let duration = if is_current {
                "ongoing".to_string()
            } else {
                "7d 0h".to_string()
            };

            let row_style = if is_selected {
                self.theme.highlight_style()
            } else if is_current {
                self.theme.highlight_style()
            } else {
                ratatui::style::Style::default()
            };

            let selector = if is_selected { "▸" } else { " " };

            let cells = vec![
                Cell::from(format!("{} {}", selector, epoch_num)),
                Cell::from(phase).style(phase_style),
                Cell::from(format!("{}", committee_size)),
                Cell::from(stake),
                Cell::from(started),
                Cell::from(duration),
            ];

            rows.push(Row::new(cells).style(row_style));
        }

        Table::new(
            rows,
            [
                Constraint::Length(8),  // EPOCH
                Constraint::Length(10), // PHASE
                Constraint::Length(11), // COMMITTEE
                Constraint::Length(12), // STAKE
                Constraint::Length(18), // STARTED
                Constraint::Length(10), // DURATION
            ],
        )
        .header(header)
        .row_highlight_style(self.theme.highlight_style())
    }

    fn build_footer(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled(
                "Current epoch highlighted. Press Enter for committee details.",
                self.theme.dim_style(),
            ),
        ])
    }
}

impl Widget for EpochHistoryView<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Clear the area
        Clear.render(area, buf);

        // Main block
        let block = Block::default()
            .title(self.build_header_line())
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 5 || inner.width < 60 {
            return;
        }

        // Layout: hint line at top, table in middle, footer at bottom
        let table_height = inner.height.saturating_sub(3);

        // Render hint line
        let hint_area = Rect::new(inner.x, inner.y, inner.width, 1);
        Paragraph::new(self.build_hint_line()).render(hint_area, buf);

        // Render table
        let table_area = Rect::new(inner.x, inner.y + 2, inner.width, table_height);
        self.build_table().render(table_area, buf);

        // Render footer
        let footer_area = Rect::new(inner.x, inner.y + inner.height - 1, inner.width, 1);
        Paragraph::new(self.build_footer()).render(footer_area, buf);
    }
}

/// Simple date string generator (days ago from now).
fn chrono_lite_date_str(days_ago: i64) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let target = now - (days_ago * 86400);

    // Convert to date parts (simple calculation)
    let days_since_epoch = target / 86400;
    let mut year = 1970;
    let mut remaining_days = days_since_epoch;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let days_in_months: [i64; 12] = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for days_in_month in days_in_months {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }

    let day = remaining_days + 1;

    format!("{:04}-{:02}-{:02} 00:00", year, month, day)
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Render the epoch history view.
pub fn render_epoch_history(area: Rect, buf: &mut Buffer, app: &App, theme: &Theme) {
    EpochHistoryView::new(app, theme).render(area, buf);
}
