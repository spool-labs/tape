//! Node grid widget for displaying committee status.
//!
//! Renders a grid of dots representing committee nodes.
//! Each dot is colored by node health status.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

use crate::app::{App, HealthStatus};
use crate::theme::Theme;

/// Width of each node cell (symbol + space + number + spacing).
/// Format: "● 123 " = 6 chars (symbol + space + number + trailing space)
const NODE_CELL_WIDTH: usize = 6;

/// Node status symbols (Unicode).
const SYMBOL_ONLINE: &str = "●";   // Filled circle
const SYMBOL_OFFLINE: &str = "○";  // Empty circle
const SYMBOL_SYNCING: &str = "◐";  // Half-filled circle
const SYMBOL_UNKNOWN: &str = "◌";  // Dotted circle
const SYMBOL_SELECTED: &str = "◉"; // Selected node

/// Widget for rendering the committee node status grid.
pub struct NodeGrid<'a> {
    /// Application state reference.
    app: &'a App,
    /// Theme reference.
    theme: &'a Theme,
    /// Whether the grid is focused (for border highlighting).
    focused: bool,
}

impl<'a> NodeGrid<'a> {
    /// Create a new node grid widget.
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self {
            app,
            theme,
            focused: false,
        }
    }

    /// Set whether the widget is focused.
    pub fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    /// Get the symbol for a health status.
    fn status_symbol(status: HealthStatus, selected: bool) -> &'static str {
        if selected {
            SYMBOL_SELECTED
        } else {
            match status {
                HealthStatus::Online => SYMBOL_ONLINE,
                HealthStatus::Offline => SYMBOL_OFFLINE,
                HealthStatus::Syncing => SYMBOL_SYNCING,
                HealthStatus::Unknown => SYMBOL_UNKNOWN,
            }
        }
    }

    /// Get the style for a health status.
    fn status_style(&self, status: HealthStatus, selected: bool) -> Style {
        if selected {
            self.theme.highlight_style()
        } else {
            match status {
                HealthStatus::Online => self.theme.online_style(),
                HealthStatus::Offline => self.theme.offline_style(),
                HealthStatus::Syncing => self.theme.syncing_style(),
                HealthStatus::Unknown => self.theme.unknown_style(),
            }
        }
    }

    /// Build the grid lines with node numbers.
    fn build_grid_lines(&self, width: u16) -> Vec<Line<'a>> {
        let mut lines = Vec::new();
        let mut current_row: Vec<Span> = Vec::new();

        // Calculate nodes per row based on available width
        let nodes_per_row = (width as usize / NODE_CELL_WIDTH).max(1);

        for (idx, node) in self.app.nodes.iter().enumerate() {
            let selected = self.app.selected_node == Some(idx);
            let symbol = Self::status_symbol(node.health, selected);
            let style = self.status_style(node.health, selected);

            // Format node ID (truncate to 4 chars max)
            let node_id = node.id.0;
            let id_str = if node_id > 9999 {
                format!("{}", node_id % 10000)
            } else {
                format!("{}", node_id)
            };

            // Build cell: symbol + space + id + padding to NODE_CELL_WIDTH
            let cell_len = 2 + id_str.len(); // symbol (1) + space (1) + id
            let padding = NODE_CELL_WIDTH.saturating_sub(cell_len);
            let padding_str: String = std::iter::repeat(' ').take(padding).collect();

            current_row.push(Span::styled(symbol, style));
            current_row.push(Span::raw(" "));
            current_row.push(Span::styled(id_str, self.theme.dim_style()));
            current_row.push(Span::raw(padding_str));

            // Start new row when full
            if (idx + 1) % nodes_per_row == 0 {
                lines.push(Line::from(current_row.clone()));
                current_row.clear();
            }
        }

        // Push remaining nodes if any
        if !current_row.is_empty() {
            lines.push(Line::from(current_row));
        }

        lines
    }

    /// Build the legend line.
    fn build_legend(&self) -> Line<'a> {
        let online_count = self.app.online_count();
        let offline_count = self.app.offline_count();
        let syncing_count = self.app.syncing_count();
        let unknown_count = self.app.unknown_count();

        Line::from(vec![
            Span::styled(SYMBOL_ONLINE, self.theme.online_style()),
            Span::styled(format!(" Online ({})  ", online_count), self.theme.text_style()),
            Span::styled(SYMBOL_OFFLINE, self.theme.offline_style()),
            Span::styled(format!(" Offline ({})  ", offline_count), self.theme.text_style()),
            Span::styled(SYMBOL_SYNCING, self.theme.syncing_style()),
            Span::styled(format!(" Syncing ({})  ", syncing_count), self.theme.text_style()),
            Span::styled(SYMBOL_UNKNOWN, self.theme.unknown_style()),
            Span::styled(format!(" Unknown ({})", unknown_count), self.theme.text_style()),
        ])
    }
}

impl Widget for NodeGrid<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Build the block
        let title = format!(
            " COMMITTEE ({}/{}) ",
            self.app.online_count(),
            self.app.committee_size()
        );

        let border_style = if self.focused {
            self.theme.border_focus_style()
        } else {
            self.theme.border_style()
        };

        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(border_style);

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 2 {
            return;
        }

        // Build content with dynamic width
        let mut lines = self.build_grid_lines(inner.width);

        // Add empty line before legend if space
        if inner.height > lines.len() as u16 + 2 {
            lines.push(Line::default());
        }

        // Add legend
        lines.push(self.build_legend());

        let paragraph = Paragraph::new(lines);
        paragraph.render(inner, buf);
    }
}

/// Calculate the required height for the node grid.
/// Assumes typical terminal width of ~65 chars for the grid area.
pub fn required_height(node_count: usize) -> u16 {
    // Estimate nodes per row based on typical width
    let typical_width = 65usize;
    let nodes_per_row = typical_width / NODE_CELL_WIDTH;
    let rows = (node_count + nodes_per_row - 1) / nodes_per_row;
    // rows + 2 for legend + 2 for border
    (rows + 4) as u16
}
