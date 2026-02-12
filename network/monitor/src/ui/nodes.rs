//! Node list view.
//!
//! Full-screen view showing all committee nodes in a sortable table format.

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Widget},
};

use crate::app::{App, HealthStatus, NodeFilter, NodeSortOrder};
use crate::theme::Theme;

/// Widget for rendering the node list view.
pub struct NodeListView<'a> {
    app: &'a App,
    theme: &'a Theme,
}

impl<'a> NodeListView<'a> {
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    fn status_symbol(&self, status: HealthStatus) -> (&'static str, Style) {
        match status {
            HealthStatus::Online => ("●", self.theme.online_style()),
            HealthStatus::Offline => ("○", self.theme.offline_style()),
            HealthStatus::Syncing => ("◐", self.theme.syncing_style()),
            HealthStatus::Unknown => ("◌", self.theme.unknown_style()),
        }
    }

    fn build_header_line(&self) -> Line<'a> {
        let total = self.app.nodes.len();
        let online = self.app.online_count();

        Line::from(vec![
            Span::styled(
                format!(" NODES ({} in committee, {} total) ", online, total),
                self.theme.title_style(),
            ),
        ])
    }

    fn build_hint_line(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled("ESC", self.theme.keybind_style()),
            Span::styled(": Back  ", self.theme.keybind_desc_style()),
            Span::styled("↑↓", self.theme.keybind_style()),
            Span::styled(": Navigate  ", self.theme.keybind_desc_style()),
            Span::styled("Enter", self.theme.keybind_style()),
            Span::styled(": Details", self.theme.keybind_desc_style()),
        ])
    }

    fn build_table(&self, _height: u16) -> Table<'a> {
        // Header with color column
        let header_cells = ["", "ST", "ID", "NAME", "STAKE", "SPOOLS", "ACTION", "LATENCY", "COMM"]
            .iter()
            .map(|h| Cell::from(*h).style(self.theme.header_style()));
        let header = Row::new(header_cells).height(1);

        // Filter and sort nodes, keeping track of original indices for color mapping
        let mut nodes: Vec<(usize, _)> = self.app.nodes.iter().enumerate().collect();

        // Apply filter from app state
        match self.app.node_filter {
            NodeFilter::Online => nodes.retain(|(_, n)| n.health == HealthStatus::Online),
            NodeFilter::Offline => nodes.retain(|(_, n)| n.health == HealthStatus::Offline),
            NodeFilter::All => {}
        }

        // Apply sort from app state (descending for stake/spools, ascending for name/latency/commission)
        match self.app.node_sort {
            NodeSortOrder::Stake => nodes.sort_by(|(_, a), (_, b)| b.stake.cmp(&a.stake)),
            NodeSortOrder::Name => nodes.sort_by(|(_, a), (_, b)| a.name.cmp(&b.name)),
            NodeSortOrder::Latency => nodes.sort_by(|(_, a), (_, b)| {
                let a_lat = a.latency_ms.unwrap_or(u32::MAX);
                let b_lat = b.latency_ms.unwrap_or(u32::MAX);
                a_lat.cmp(&b_lat)
            }),
            NodeSortOrder::Commission => nodes.sort_by(|(_, a), (_, b)| a.commission.0.cmp(&b.commission.0)),
            NodeSortOrder::Spools => nodes.sort_by(|(_, a), (_, b)| b.spool_count.cmp(&a.spool_count)),
        }

        // Build rows
        let rows: Vec<Row> = nodes
            .iter()
            .enumerate()
            .map(|(display_idx, (_orig_idx, node))| {
                let (status_sym, status_style) = self.status_symbol(node.health);
                let is_selected = self.app.selected_node_index == Some(display_idx);

                let row_style = if is_selected {
                    self.theme.highlight_style()
                } else {
                    Style::default()
                };

                let latency_str = match node.latency_ms {
                    Some(ms) => format!("{}ms", ms),
                    None => "TIMEOUT".to_string(),
                };

                // Color block matching the spool distribution (using color slot for golden-ratio distribution)
                let slot = self.app.get_color_slot(node.id).unwrap_or(0);
                let color = Theme::member_color(slot as usize);

                // Color the action based on type
                let action_str = if node.fsm_action.is_empty() { "--" } else { &node.fsm_action };
                let action_style = if node.fsm_action.starts_with("Sync")
                    || node.fsm_action.starts_with("Advance")
                    || node.fsm_action.starts_with("Join")
                    || node.fsm_action.starts_with("AdvEpoch")
                {
                    Style::default().fg(ratatui::style::Color::Green)
                } else if node.fsm_action.starts_with("Wait") {
                    Style::default().fg(ratatui::style::Color::Yellow)
                } else {
                    self.theme.dim_style()
                };

                let cells = vec![
                    Cell::from("█").style(Style::default().fg(color)),
                    Cell::from(status_sym).style(status_style),
                    Cell::from(format!("{}", node.id.0)),
                    Cell::from(truncate_name(&node.name, 16)),
                    Cell::from(format!("{} TAPE", node.stake.as_string())),
                    Cell::from(format!("{}", node.spool_count)),
                    Cell::from(action_str.to_string()).style(action_style),
                    Cell::from(latency_str),
                    Cell::from(node.commission_display()),
                ];

                Row::new(cells).style(row_style)
            })
            .collect();

        Table::new(
            rows,
            [
                Constraint::Length(2),  // Color
                Constraint::Length(3),  // ST
                Constraint::Length(5),  // ID
                Constraint::Length(16), // NAME
                Constraint::Length(18), // STAKE
                Constraint::Length(8),  // SPOOLS
                Constraint::Length(14), // ACTION
                Constraint::Length(9),  // LATENCY
                Constraint::Length(7),  // COMM
            ],
        )
        .header(header)
        .row_highlight_style(self.theme.highlight_style())
    }

    fn build_footer(&self, total_nodes: usize, visible_rows: usize) -> Line<'a> {
        let page = self.app.scroll_offset / visible_rows.max(1) + 1;
        let total_pages = (total_nodes + visible_rows - 1) / visible_rows.max(1);

        Line::from(vec![
            Span::styled(
                format!(
                    "Showing {}-{} of {}",
                    self.app.scroll_offset + 1,
                    (self.app.scroll_offset + visible_rows).min(total_nodes),
                    total_nodes
                ),
                self.theme.dim_style(),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Page {}/{}", page, total_pages.max(1)),
                self.theme.dim_style(),
            ),
            Span::raw("  "),
            Span::styled("PgUp/PgDn", self.theme.keybind_style()),
        ])
    }

    fn build_sort_filter_line(&self) -> Line<'a> {
        Line::from(vec![
            Span::styled("Sort: ", self.theme.text_style()),
            Span::styled("[s]", self.theme.keybind_style()),
            Span::styled("take ", if self.app.node_sort == NodeSortOrder::Stake {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("[n]", self.theme.keybind_style()),
            Span::styled("ame ", if self.app.node_sort == NodeSortOrder::Name {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("[l]", self.theme.keybind_style()),
            Span::styled("atency ", if self.app.node_sort == NodeSortOrder::Latency {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("[c]", self.theme.keybind_style()),
            Span::styled("omm ", if self.app.node_sort == NodeSortOrder::Commission {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("[p]", self.theme.keybind_style()),
            Span::styled("ools  ", if self.app.node_sort == NodeSortOrder::Spools {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("│ Filter: ", self.theme.dim_style()),
            Span::styled("[a]", self.theme.keybind_style()),
            Span::styled("ll ", if self.app.node_filter == NodeFilter::All {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("[o]", self.theme.keybind_style()),
            Span::styled("nline ", if self.app.node_filter == NodeFilter::Online {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
            Span::styled("[f]", self.theme.keybind_style()),
            Span::styled("offline", if self.app.node_filter == NodeFilter::Offline {
                self.theme.highlight_style()
            } else {
                self.theme.dim_style()
            }),
        ])
    }
}

impl Widget for NodeListView<'_> {
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

        // Layout: hint line at top, table in middle, footer at bottom
        let table_height = inner.height.saturating_sub(4);

        // Render hint line
        let hint_area = Rect::new(inner.x, inner.y, inner.width, 1);
        Paragraph::new(self.build_hint_line()).render(hint_area, buf);

        // Render table
        let table_area = Rect::new(inner.x, inner.y + 1, inner.width, table_height);
        self.build_table(table_height).render(table_area, buf);

        // Render footer
        let visible_rows = table_height.saturating_sub(1) as usize; // -1 for header
        let footer_area = Rect::new(inner.x, inner.y + inner.height - 2, inner.width, 1);
        Paragraph::new(self.build_footer(self.app.nodes.len(), visible_rows)).render(footer_area, buf);

        // Render sort/filter line
        let sort_area = Rect::new(inner.x, inner.y + inner.height - 1, inner.width, 1);
        Paragraph::new(self.build_sort_filter_line()).render(sort_area, buf);
    }
}

/// Truncate a name to fit in the given width.
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}

/// Render the full-screen node list view.
pub fn render_node_list(area: Rect, buf: &mut Buffer, app: &App, theme: &Theme) {
    NodeListView::new(app, theme).render(area, buf);
}
