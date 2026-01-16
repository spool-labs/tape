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

use crate::app::{HealthStatus, NodeState};
use crate::theme::Theme;

/// Width of each node cell (symbol + space + number + spacing).
/// Format: "● 123 " = 6 chars (symbol + space + number + trailing space)
const NODE_CELL_WIDTH: usize = 6;

/// Node status symbols (Unicode).
const SYMBOL_ONLINE: &str = "●";   // Filled circle
const SYMBOL_OFFLINE: &str = "○";  // Empty circle
const SYMBOL_SYNCING: &str = "◐";  // Half-filled circle
const SYMBOL_UNKNOWN: &str = "◌";  // Dotted circle

/// Widget for rendering the committee node status grid.
pub struct NodeGrid<'a> {
    /// Nodes to display in this grid.
    nodes: &'a [NodeState],
    /// Theme reference.
    theme: &'a Theme,
    /// Title for the panel.
    title: String,
}

impl<'a> NodeGrid<'a> {
    /// Create a new node grid widget.
    pub fn new(theme: &'a Theme) -> Self {
        Self {
            nodes: &[],
            theme,
            title: "COMMITTEE".to_string(),
        }
    }

    /// Set the nodes to display.
    pub fn nodes(mut self, nodes: &'a [NodeState]) -> Self {
        self.nodes = nodes;
        self
    }

    /// Set the panel title.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    /// Get the symbol for a health status.
    fn status_symbol(status: HealthStatus) -> &'static str {
        match status {
            HealthStatus::Online => SYMBOL_ONLINE,
            HealthStatus::Offline => SYMBOL_OFFLINE,
            HealthStatus::Syncing => SYMBOL_SYNCING,
            HealthStatus::Unknown => SYMBOL_UNKNOWN,
        }
    }

    /// Get the style for a node based on its status.
    fn node_style(&self, node: &NodeState) -> Style {
        match node.health {
            HealthStatus::Online => self.theme.online_style(),
            HealthStatus::Offline => self.theme.offline_style(),
            HealthStatus::Syncing => self.theme.syncing_style(),
            HealthStatus::Unknown => self.theme.unknown_style(),
        }
    }

    /// Build the grid lines with node numbers.
    fn build_grid_lines(&self, width: u16) -> Vec<Line<'a>> {
        let mut lines = Vec::new();
        let mut current_row: Vec<Span> = Vec::new();

        // Account for horizontal padding (2 chars)
        let content_width = (width as usize).saturating_sub(2);

        // Calculate nodes per row based on available width
        let nodes_per_row = (content_width / NODE_CELL_WIDTH).max(1);

        for (idx, node) in self.nodes.iter().enumerate() {
            // Add left padding at start of each row
            if current_row.is_empty() {
                current_row.push(Span::raw(" "));
            }

            let symbol = Self::status_symbol(node.health);
            let style = self.node_style(node);

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

    /// Count nodes by health status.
    fn online_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.health == HealthStatus::Online).count()
    }
}

impl Widget for NodeGrid<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Build the title with node count
        let title = format!(
            " {} ({}/{}) ",
            self.title,
            self.online_count(),
            self.nodes.len()
        );

        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 1 {
            return;
        }

        // Build content with dynamic width
        let lines = self.build_grid_lines(inner.width);

        let paragraph = Paragraph::new(lines);
        paragraph.render(inner, buf);
    }
}

/// Calculate the required height for the node grid.
/// Assumes typical terminal width of ~35 chars for the grid area (third of screen).
pub fn required_height(node_count: usize) -> u16 {
    // Estimate nodes per row based on typical width for a 3-column layout
    let typical_width = 35usize;
    let nodes_per_row = (typical_width / NODE_CELL_WIDTH).max(1);
    let rows = (node_count + nodes_per_row - 1) / nodes_per_row;
    // rows + 2 for border
    (rows + 2) as u16
}
