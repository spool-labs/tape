//! Spool distribution visualization widget.
//!
//! Displays 1024 spools as colored dots, where each dot's color represents
//! the committee member that owns that spool.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::Span,
    widgets::{Block, Borders, Widget},
};

use crate::app::{App, TOTAL_SPOOLS};
use crate::theme::Theme;

/// Widget for rendering the spool distribution as colored dots.
pub struct SpoolBar<'a> {
    /// Application state reference.
    app: &'a App,
    /// Theme reference.
    theme: &'a Theme,
}

impl<'a> SpoolBar<'a> {
    /// Create a new spool bar widget.
    pub fn new(app: &'a App, theme: &'a Theme) -> Self {
        Self { app, theme }
    }

    /// Build a mapping from spool index to node index.
    /// Returns a vec where vec[spool_index] = Some(node_index) or None if unassigned.
    fn build_spool_to_node_map(&self) -> Vec<Option<usize>> {
        let mut map = vec![None; TOTAL_SPOOLS as usize];
        for (node_idx, node) in self.app.nodes.iter().enumerate() {
            for &spool in &node.assigned_spools {
                let spool_idx = spool as usize;
                if spool_idx < map.len() {
                    map[spool_idx] = Some(node_idx);
                }
            }
        }
        map
    }
}

impl Widget for SpoolBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = format!(" SPOOL DISTRIBUTION ({} spools) ", TOTAL_SPOOLS);
        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 1 || inner.width < 10 {
            return;
        }

        // Build spool-to-node mapping
        let spool_map = self.build_spool_to_node_map();

        // Calculate how many spools per row
        // Use full block (█) which takes 1 cell width
        let spools_per_row = inner.width as usize;
        let rows_needed = (TOTAL_SPOOLS as usize + spools_per_row - 1) / spools_per_row;
        let rows_to_show = (inner.height as usize).min(rows_needed);

        // Render each row
        for row in 0..rows_to_show {
            let y = inner.y + row as u16;
            let start_spool = row * spools_per_row;
            let end_spool = (start_spool + spools_per_row).min(TOTAL_SPOOLS as usize);

            for (col, spool_idx) in (start_spool..end_spool).enumerate() {
                let x = inner.x + col as u16;
                if x >= inner.x + inner.width {
                    break;
                }

                let (symbol, style) = match spool_map.get(spool_idx).copied().flatten() {
                    Some(node_idx) => {
                        let color = Theme::member_color(node_idx);
                        ("▌", Style::default().fg(color))
                    }
                    None => {
                        // Unassigned spool - show as dim
                        ("▏", self.theme.dim_style())
                    }
                };

                buf.set_string(x, y, symbol, style);
            }
        }

        // If not all rows fit, show indicator
        if rows_to_show < rows_needed {
            let remaining = TOTAL_SPOOLS as usize - (rows_to_show * spools_per_row);
            let indicator = format!("(+{} more)", remaining);
            let x = inner.x + inner.width.saturating_sub(indicator.len() as u16);
            let y = inner.y + inner.height.saturating_sub(1);
            buf.set_string(x, y, &indicator, self.theme.dim_style());
        }
    }
}

/// Calculate required height for the spool bar.
/// With typical terminal width of 120 chars, 1024 spools fit in ~8 rows.
/// Add 2 for borders = 10 total.
pub fn required_height() -> u16 {
    10
}
