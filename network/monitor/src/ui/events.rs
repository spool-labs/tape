//! Event list view.
//!
//! Full-screen view showing all network events in a filterable table format.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Widget},
};

use crate::app::{App, EventFilter, EventType};
use crate::theme::Theme;

/// Widget for rendering the event list view.
pub struct EventListView<'a> {
    app: &'a App,
    theme: &'a Theme,
}

impl<'a> EventListView<'a> {
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    /// Get the style for an event type.
    fn event_style(&self, event_type: EventType) -> Style {
        match event_type {
            EventType::TrackCertified => self.theme.success_style(),
            EventType::TapeReserved | EventType::TrackRegistered => {
                Style::default().fg(ratatui::style::Color::Cyan)
            }
            EventType::NodeOnline => self.theme.success_style(),
            EventType::NodeOffline => self.theme.warning_style(),
            EventType::SliceUploaded => Style::default().fg(ratatui::style::Color::Blue),
            EventType::BlobDownloaded => Style::default().fg(ratatui::style::Color::Magenta),
            EventType::EpochTransition => self.theme.text_style(),
            EventType::Error => self.theme.error_style(),
        }
    }

    fn build_header_line(&self) -> Line<'a> {
        let total = self.app.events.len();
        let filtered = self.filtered_events_count();

        let filter_text = match self.app.event_filter {
            EventFilter::All => "all".to_string(),
            EventFilter::Tracks => "tracks".to_string(),
            EventFilter::Tapes => "tapes".to_string(),
            EventFilter::Nodes => "nodes".to_string(),
            EventFilter::System => "system".to_string(),
        };

        Line::from(vec![Span::styled(
            format!(
                " EVENTS ({} showing, {} total, filter: {}) ",
                filtered, total, filter_text
            ),
            self.theme.title_style(),
        )])
    }

    fn build_hint_line(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled("ESC", self.theme.keybind_style()),
            Span::styled(": Back  ", self.theme.keybind_desc_style()),
            Span::styled("↑↓", self.theme.keybind_style()),
            Span::styled(": Scroll  ", self.theme.keybind_desc_style()),
            Span::styled("a", self.theme.keybind_style()),
            Span::styled(": Auto-scroll", self.theme.keybind_desc_style()),
        ])
    }

    fn build_filter_line(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled("Filter: ", self.theme.text_style()),
            Span::styled("[a]", self.theme.keybind_style()),
            Span::styled(
                "ll ",
                if self.app.event_filter == EventFilter::All {
                    self.theme.highlight_style()
                } else {
                    self.theme.dim_style()
                },
            ),
            Span::styled("[t]", self.theme.keybind_style()),
            Span::styled(
                "racks ",
                if self.app.event_filter == EventFilter::Tracks {
                    self.theme.highlight_style()
                } else {
                    self.theme.dim_style()
                },
            ),
            Span::styled("[p]", self.theme.keybind_style()),
            Span::styled(
                "tapes ",
                if self.app.event_filter == EventFilter::Tapes {
                    self.theme.highlight_style()
                } else {
                    self.theme.dim_style()
                },
            ),
            Span::styled("[n]", self.theme.keybind_style()),
            Span::styled(
                "odes ",
                if self.app.event_filter == EventFilter::Nodes {
                    self.theme.highlight_style()
                } else {
                    self.theme.dim_style()
                },
            ),
            Span::styled("[s]", self.theme.keybind_style()),
            Span::styled(
                "ystem",
                if self.app.event_filter == EventFilter::System {
                    self.theme.highlight_style()
                } else {
                    self.theme.dim_style()
                },
            ),
            Span::raw("  "),
            Span::styled("│ ", self.theme.dim_style()),
            Span::styled(
                if self.app.event_auto_scroll {
                    "[Auto-scroll ON]"
                } else {
                    "[Auto-scroll OFF]"
                },
                if self.app.event_auto_scroll {
                    self.theme.success_style()
                } else {
                    self.theme.dim_style()
                },
            ),
        ])
    }

    fn filtered_events_count(&self) -> usize {
        self.app
            .events
            .iter()
            .filter(|e| self.app.event_filter.matches(e.event_type))
            .count()
    }

    fn build_event_lines(&self, max_lines: usize, width: usize) -> Vec<Line<'a>> {
        let filtered_events: Vec<_> = self
            .app
            .events
            .iter()
            .filter(|e| self.app.event_filter.matches(e.event_type))
            .collect();

        let total_filtered = filtered_events.len();

        // Calculate scroll position
        let scroll = if self.app.event_auto_scroll {
            total_filtered.saturating_sub(max_lines)
        } else {
            self.app.event_scroll.min(total_filtered.saturating_sub(max_lines))
        };

        filtered_events
            .into_iter()
            .skip(scroll)
            .take(max_lines)
            .map(|event| self.build_event_line(event, width))
            .collect()
    }

    fn build_event_line(&self, event: &crate::app::NetworkEvent, max_width: usize) -> Line<'a> {
        let timestamp = event.timestamp_display(self.app.app_start);
        let icon = event.event_type.icon();
        let style = self.event_style(event.event_type);

        // Layout: "HH:MM:SS  X description                    actors"
        let prefix_width = 11; // "HH:MM:SS  X " = 8 + 2 + 1
        let actors_width = if event.actors.is_empty() {
            0
        } else {
            event.actors.len() + 2
        };
        let available_for_desc = max_width.saturating_sub(prefix_width + actors_width);

        // Truncate description only if needed
        let description = if event.description.len() > available_for_desc && available_for_desc > 3
        {
            format!(
                "{}...",
                &event.description[..available_for_desc.saturating_sub(3)]
            )
        } else {
            event.description.clone()
        };

        // Build spans
        let mut spans = vec![
            Span::styled(timestamp, self.theme.dim_style()),
            Span::raw("  "),
            Span::styled(icon.to_string(), style),
            Span::raw(" "),
            Span::styled(description.clone(), self.theme.text_style()),
        ];

        // Add actors on the right if present
        if !event.actors.is_empty() {
            let padding =
                max_width.saturating_sub(prefix_width + description.len() + event.actors.len());
            let padding_str: String = std::iter::repeat(' ').take(padding).collect();
            spans.push(Span::raw(padding_str));
            spans.push(Span::styled(event.actors.clone(), self.theme.dim_style()));
        }

        Line::from(spans)
    }

    fn build_footer(&self, visible_rows: usize) -> Line<'a> {
        let filtered_count = self.filtered_events_count();
        let scroll = if self.app.event_auto_scroll {
            filtered_count.saturating_sub(visible_rows)
        } else {
            self.app.event_scroll.min(filtered_count.saturating_sub(visible_rows))
        };

        let start = scroll + 1;
        let end = (scroll + visible_rows).min(filtered_count);

        if filtered_count == 0 {
            Line::from(vec![Span::styled(
                "No events match the current filter",
                self.theme.dim_style(),
            )])
        } else {
            Line::from(vec![
                Span::styled(
                    format!("Showing {}-{} of {}", start, end, filtered_count),
                    self.theme.dim_style(),
                ),
                Span::raw("  "),
                Span::styled("PgUp/PgDn", self.theme.keybind_style()),
                Span::styled(": Page  ", self.theme.keybind_desc_style()),
                Span::styled("Home/End", self.theme.keybind_style()),
                Span::styled(": Jump", self.theme.keybind_desc_style()),
            ])
        }
    }
}

impl Widget for EventListView<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Clear the area first
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

        // Layout: hint line at top, filter line, events in middle, footer at bottom
        let events_height = inner.height.saturating_sub(4) as usize;

        // Render hint line
        let hint_area = Rect::new(inner.x, inner.y, inner.width, 1);
        Paragraph::new(self.build_hint_line()).render(hint_area, buf);

        // Render filter line
        let filter_area = Rect::new(inner.x, inner.y + 1, inner.width, 1);
        Paragraph::new(self.build_filter_line()).render(filter_area, buf);

        // Render events
        let events_area = Rect::new(inner.x, inner.y + 2, inner.width, events_height as u16);
        let event_lines = self.build_event_lines(events_height, inner.width as usize);
        Paragraph::new(event_lines).render(events_area, buf);

        // Render footer
        let footer_area = Rect::new(inner.x, inner.y + inner.height - 1, inner.width, 1);
        Paragraph::new(self.build_footer(events_height)).render(footer_area, buf);
    }
}

/// Render the full-screen event list view.
pub fn render_event_list(area: Rect, buf: &mut Buffer, app: &App, theme: &Theme) {
    EventListView::new(app, theme).render(area, buf);
}
