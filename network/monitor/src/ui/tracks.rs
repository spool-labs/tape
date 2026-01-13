//! Track search view.
//!
//! Allows searching for tracks by ID, key hash, or pubkey.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap},
};

use crate::app::App;
use crate::theme::Theme;

/// Widget for rendering the track search view.
pub struct TrackSearchView<'a> {
    #[allow(dead_code)]
    app: &'a App,
    theme: &'a Theme,
    query: &'a str,
}

impl<'a> TrackSearchView<'a> {
    pub fn new(app: &'a App, theme: &'a Theme, query: &'a str) -> Self {
        Self { app, theme, query }
    }

    fn build_search_prompt(&self) -> Vec<Line<'a>> {
        vec![
            Line::from(vec![
                Span::styled("Enter track ID, key hash, or pubkey: ", self.theme.text_style()),
                Span::styled(self.query, self.theme.highlight_style()),
                Span::styled("_", Style::default().add_modifier(ratatui::style::Modifier::SLOW_BLINK)),
            ]),
        ]
    }

    fn build_results(&self) -> Vec<Line<'a>> {
        let mut lines = vec![
            Line::default(),
            Line::styled(
                "─".repeat(60),
                self.theme.dim_style(),
            ),
            Line::styled("RESULTS", self.theme.header_style()),
            Line::styled(
                "─".repeat(60),
                self.theme.dim_style(),
            ),
            Line::default(),
        ];

        if self.query.is_empty() {
            lines.push(Line::styled(
                "Enter a search query to find tracks...",
                self.theme.dim_style(),
            ));
            lines.push(Line::default());
            lines.push(Line::from(vec![
                Span::styled("Examples: ", self.theme.text_style()),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  • ", self.theme.dim_style()),
                Span::styled("48291", self.theme.text_style()),
                Span::styled(" (track ID)", self.theme.dim_style()),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  • ", self.theme.dim_style()),
                Span::styled("a7b2c3d4e5f6...", self.theme.text_style()),
                Span::styled(" (key hash)", self.theme.dim_style()),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  • ", self.theme.dim_style()),
                Span::styled("7xKp...3mQr", self.theme.text_style()),
                Span::styled(" (authority pubkey)", self.theme.dim_style()),
            ]));
        } else {
            // Show that search is not yet implemented
            lines.push(Line::from(vec![
                Span::styled("Query: ", self.theme.dim_style()),
                Span::styled(self.query, self.theme.highlight_style()),
            ]));
            lines.push(Line::default());
            lines.push(Line::styled(
                "Track search is not yet implemented.",
                self.theme.dim_style(),
            ));
            lines.push(Line::default());
            lines.push(Line::styled(
                "To search tracks, use the CLI:",
                self.theme.dim_style(),
            ));
            lines.push(Line::from(vec![
                Span::styled("  tape account track ", self.theme.text_style()),
                Span::styled("<TRACK_NUMBER>", self.theme.highlight_style()),
            ]));
        }

        lines.push(Line::default());
        lines.push(Line::from(vec![
            Span::styled("Press ", self.theme.dim_style()),
            Span::styled("ESC", self.theme.keybind_style()),
            Span::styled(" to close", self.theme.dim_style()),
        ]));

        lines
    }
}

impl Widget for TrackSearchView<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Clear the area
        Clear.render(area, buf);

        // Build the block
        let block = Block::default()
            .title(Span::styled(" SEARCH ", self.theme.title_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_focus_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 5 || inner.width < 40 {
            return;
        }

        // Build content
        let mut lines = self.build_search_prompt();
        lines.extend(self.build_results());

        let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
        paragraph.render(inner, buf);
    }
}

/// Render the track search view.
pub fn render_track_search(area: Rect, buf: &mut Buffer, app: &App, theme: &Theme, query: &str) {
    TrackSearchView::new(app, theme, query).render(area, buf);
}
