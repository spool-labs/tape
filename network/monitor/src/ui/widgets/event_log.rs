//! Event log widget for displaying recent network events.
//!
//! Shows a scrollable list of network events with timestamps,
//! icons, and color-coded event types.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

use crate::app::{App, EventType, NetworkEvent};
use crate::theme::Theme;

/// Widget for rendering the event log.
pub struct EventLog<'a> {
    /// Application state reference.
    app: &'a App,
    /// Theme reference.
    theme: &'a Theme,
}

impl<'a> EventLog<'a> {
    /// Create a new event log widget.
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
            EventType::SnapshotCertified => Style::default().fg(ratatui::style::Color::Cyan),
            EventType::Error => self.theme.error_style(),
        }
    }

    /// Build a single event line.
    fn build_event_line(&self, event: &NetworkEvent, max_width: usize) -> Line<'a> {
        let timestamp = event.timestamp_display(self.app.app_start);
        let icon = event.event_type.icon();
        let style = self.event_style(event.event_type);

        // Account for horizontal padding (2 chars)
        let content_width = max_width.saturating_sub(2);

        // Calculate available width for description
        // Layout: " HH:MM:SS  X description                    actors "
        let prefix_width = 11; // "HH:MM:SS  X " = 8 + 2 + 1
        let actors_width = if event.actors.is_empty() { 0 } else { event.actors.len() + 2 };
        let available_for_desc = content_width.saturating_sub(prefix_width + actors_width);

        // Truncate description only if needed
        let description = if event.description.len() > available_for_desc && available_for_desc > 3 {
            format!("{}...", &event.description[..available_for_desc.saturating_sub(3)])
        } else {
            event.description.clone()
        };

        // Build spans
        let mut spans = vec![
            Span::raw(" "),  // Left padding
            Span::styled(timestamp, self.theme.dim_style()),
            Span::raw("  "),
            Span::styled(icon, style),
            Span::raw(" "),
            Span::styled(description.clone(), self.theme.text_style()),
        ];

        // Add actors on the right if present
        if !event.actors.is_empty() {
            let padding = content_width.saturating_sub(prefix_width + description.len() + event.actors.len() + 1);
            let padding_str: String = std::iter::repeat(' ').take(padding).collect();
            spans.push(Span::raw(padding_str));
            spans.push(Span::styled(format!("{} ", event.actors), self.theme.dim_style()));
        }

        Line::from(spans)
    }
}

impl Widget for EventLog<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let auto_scroll_indicator = if self.app.event_auto_scroll {
            "[Auto-scroll]"
        } else {
            "[Manual]"
        };

        let title = format!(" LOG {} ", auto_scroll_indicator);

        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 1 || inner.width < 20 {
            return;
        }

        // Calculate visible range
        let visible_count = inner.height as usize;
        let total_events = self.app.events.len();

        // Determine scroll position
        let start_idx = if self.app.event_auto_scroll {
            total_events.saturating_sub(visible_count)
        } else {
            self.app.event_scroll.min(total_events.saturating_sub(visible_count))
        };

        // Build visible event lines
        let lines: Vec<Line> = self.app.events
            .iter()
            .skip(start_idx)
            .take(visible_count)
            .map(|event| self.build_event_line(event, inner.width as usize))
            .collect();

        let paragraph = Paragraph::new(lines);
        paragraph.render(inner, buf);
    }
}

/// Calculate minimum height for the event log.
pub fn min_height() -> u16 {
    // At least 3 events visible + border
    5
}
