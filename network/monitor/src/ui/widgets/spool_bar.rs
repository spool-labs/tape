//! Spool distribution visualization widget.
//!
//! Displays spools as colored dots, where each dot's color represents
//! the committee member that owns that spool.
//!
//! During syncing phase, highlights spools that changed ownership between
//! the previous and current committee assignments.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::Span,
    widgets::{Block, Borders, Widget},
};

use tape_core::erasure::SPOOL_COUNT;
use crate::app::TOTAL_SPOOLS;
use crate::theme::Theme;

/// How to render spools in the distribution view.
pub enum SpoolHighlight<'a> {
    /// Normal coloring - color all spools by their owning node.
    Normal {
        /// Raw spool assignment array (index -> member index).
        spools: &'a [u8; SPOOL_COUNT],
        /// Color slot for each committee member (member_index -> color slot).
        member_slots: &'a [u8],
    },
    /// Show changes between prev and current spool assignments.
    /// Highlights spools that changed owner, dims unchanged spools.
    ShowChanges {
        /// Previous epoch spool assignments.
        spools_prev: &'a [u8; SPOOL_COUNT],
        /// Current epoch spool assignments.
        spools_current: &'a [u8; SPOOL_COUNT],
        /// Color slot for each committee member (member_index -> color slot).
        member_slots: &'a [u8],
    },
    /// All spools grayed out (no spool data available).
    Unavailable,
}

/// Widget for rendering the spool distribution as colored dots.
pub struct SpoolBar<'a> {
    /// Theme reference.
    theme: &'a Theme,
    /// How to render spools.
    highlight: SpoolHighlight<'a>,
}

impl<'a> SpoolBar<'a> {
    /// Create a new spool bar widget.
    pub fn new(theme: &'a Theme) -> Self {
        Self {
            theme,
            highlight: SpoolHighlight::Unavailable,
        }
    }

    /// Set the highlight mode.
    pub fn highlight(mut self, mode: SpoolHighlight<'a>) -> Self {
        self.highlight = mode;
        self
    }
}

impl Widget for SpoolBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Calculate change count for title
        let (title, _change_count) = match &self.highlight {
            SpoolHighlight::ShowChanges { spools_prev, spools_current, .. } => {
                let changes = spools_prev.iter()
                    .zip(spools_current.iter())
                    .filter(|(prev, curr)| prev != curr)
                    .count();
                (format!(" SPOOL DISTRIBUTION ({} changed) ", changes), Some(changes))
            }
            _ => (format!(" SPOOL DISTRIBUTION ({} spools) ", TOTAL_SPOOLS), None),
        };

        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < 1 || inner.width < 10 {
            return;
        }

        // Account for horizontal padding (1 char each side)
        let content_width = (inner.width as usize).saturating_sub(2);

        // Calculate how many spools per row
        let spools_per_row = content_width;
        let rows_needed = (TOTAL_SPOOLS as usize + spools_per_row - 1) / spools_per_row;
        let rows_to_show = (inner.height as usize).min(rows_needed);

        // Render based on highlight mode
        match &self.highlight {
            SpoolHighlight::Normal { spools, member_slots } => {
                self.render_normal(inner, buf, spools, member_slots, spools_per_row, rows_to_show);
            }
            SpoolHighlight::ShowChanges { spools_prev, spools_current, member_slots } => {
                self.render_changes(inner, buf, spools_prev, spools_current, member_slots, spools_per_row, rows_to_show);
            }
            SpoolHighlight::Unavailable => {
                self.render_unavailable(inner, buf, spools_per_row, rows_to_show);
            }
        }

        // If not all rows fit, show indicator
        if rows_to_show < rows_needed {
            let remaining = TOTAL_SPOOLS as usize - (rows_to_show * spools_per_row);
            let indicator = format!("(+{} more) ", remaining);
            let x = inner.x + inner.width.saturating_sub(indicator.len() as u16);
            let y = inner.y + inner.height.saturating_sub(1);
            buf.set_string(x, y, &indicator, self.theme.dim_style());
        }
    }
}

impl<'a> SpoolBar<'a> {
    /// Render spools with normal coloring (each spool colored by owner's color slot).
    fn render_normal(
        &self,
        inner: Rect,
        buf: &mut Buffer,
        spools: &[u8; SPOOL_COUNT],
        member_slots: &[u8],
        spools_per_row: usize,
        rows_to_show: usize,
    ) {
        for row in 0..rows_to_show {
            let y = inner.y + row as u16;
            let start_spool = row * spools_per_row;
            let end_spool = (start_spool + spools_per_row).min(TOTAL_SPOOLS as usize);

            for (col, spool_idx) in (start_spool..end_spool).enumerate() {
                let x = inner.x + 1 + col as u16;  // +1 for left padding
                if x >= inner.x + inner.width - 1 {
                    break;
                }

                let member_idx = spools[spool_idx] as usize;
                // member_idx of 255 or very high means unassigned
                let (symbol, style) = if member_idx < member_slots.len() {
                    // Use color slot for golden-ratio distributed colors
                    let slot = member_slots[member_idx];
                    let color = Theme::member_color(slot as usize);
                    ("▌", Style::default().fg(color))
                } else {
                    ("▏", self.theme.dim_style())
                };

                buf.set_string(x, y, symbol, style);
            }
        }
    }

    /// Render spools showing changes between prev and current.
    /// Changed spools are highlighted in their new owner's color (by slot).
    /// Unchanged spools are dimmed.
    fn render_changes(
        &self,
        inner: Rect,
        buf: &mut Buffer,
        spools_prev: &[u8; SPOOL_COUNT],
        spools_current: &[u8; SPOOL_COUNT],
        member_slots: &[u8],
        spools_per_row: usize,
        rows_to_show: usize,
    ) {
        for row in 0..rows_to_show {
            let y = inner.y + row as u16;
            let start_spool = row * spools_per_row;
            let end_spool = (start_spool + spools_per_row).min(TOTAL_SPOOLS as usize);

            for (col, spool_idx) in (start_spool..end_spool).enumerate() {
                let x = inner.x + 1 + col as u16;  // +1 for left padding
                if x >= inner.x + inner.width - 1 {
                    break;
                }

                let prev_owner = spools_prev[spool_idx];
                let curr_owner = spools_current[spool_idx];
                let changed = prev_owner != curr_owner;

                let (symbol, style) = if changed {
                    // Highlight changed spools with their new owner's color
                    let member_idx = curr_owner as usize;
                    if member_idx < member_slots.len() {
                        let slot = member_slots[member_idx];
                        let color = Theme::member_color(slot as usize);
                        ("▌", Style::default().fg(color))
                    } else {
                        ("▌", self.theme.warning_style())
                    }
                } else {
                    // Dim unchanged spools (thick, but dimmed)
                    ("▌", self.theme.dim_style())
                };

                buf.set_string(x, y, symbol, style);
            }
        }
    }

    /// Render all spools as unavailable (grayed out).
    fn render_unavailable(
        &self,
        inner: Rect,
        buf: &mut Buffer,
        spools_per_row: usize,
        rows_to_show: usize,
    ) {
        for row in 0..rows_to_show {
            let y = inner.y + row as u16;
            let start_spool = row * spools_per_row;
            let end_spool = (start_spool + spools_per_row).min(TOTAL_SPOOLS as usize);

            for (col, _spool_idx) in (start_spool..end_spool).enumerate() {
                let x = inner.x + 1 + col as u16;  // +1 for left padding
                if x >= inner.x + inner.width - 1 {
                    break;
                }

                buf.set_string(x, y, "▏", self.theme.dim_style());
            }
        }
    }
}

/// Calculate required height for the spool bar.
/// With typical terminal width of 120 chars, 1024 spools fit in ~8 rows.
/// Add 2 for borders = 10 total.
pub fn required_height() -> u16 {
    10
}
