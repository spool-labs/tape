//! Spool distribution visualization widget.
//!
//! Displays spools grouped into blocks of SPOOL_GROUP_SIZE (20), matching the
//! spooler fuzz TUI layout. Each group is rendered as a 7-wide × 3-tall grid
//! of spool glyphs, colored by the owning committee member.
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

use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use crate::theme::Theme;

/// Group display dimensions (matching spooler fuzz TUI).
const GROUP_COLS: usize = 7;
const GROUP_ROWS: usize = 3;
const SPOOL_CHAR: &str = "▌";
const SPOOL_CHAR_DIM: &str = "▏";

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

/// Widget for rendering the spool distribution as grouped blocks.
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

    /// Determine the display style for a single spool.
    fn spool_style(&self, spool_idx: usize) -> (&str, Style) {
        match &self.highlight {
            SpoolHighlight::Normal { spools, member_slots } => {
                let member_idx = spools[spool_idx] as usize;
                if member_idx < member_slots.len() {
                    let slot = member_slots[member_idx];
                    let color = Theme::member_color(slot as usize);
                    (SPOOL_CHAR, Style::default().fg(color))
                } else {
                    (SPOOL_CHAR_DIM, self.theme.dim_style())
                }
            }
            SpoolHighlight::ShowChanges { spools_prev, spools_current, member_slots } => {
                let prev_owner = spools_prev[spool_idx];
                let curr_owner = spools_current[spool_idx];
                if prev_owner != curr_owner {
                    let member_idx = curr_owner as usize;
                    if member_idx < member_slots.len() {
                        let slot = member_slots[member_idx];
                        let color = Theme::member_color(slot as usize);
                        (SPOOL_CHAR, Style::default().fg(color))
                    } else {
                        (SPOOL_CHAR, self.theme.warning_style())
                    }
                } else {
                    (SPOOL_CHAR, self.theme.dim_style())
                }
            }
            SpoolHighlight::Unavailable => {
                (SPOOL_CHAR_DIM, self.theme.dim_style())
            }
        }
    }
}

impl Widget for SpoolBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let (title, _change_count) = match &self.highlight {
            SpoolHighlight::ShowChanges { spools_prev, spools_current, .. } => {
                let changes = spools_prev.iter()
                    .zip(spools_current.iter())
                    .filter(|(prev, curr)| prev != curr)
                    .count();
                (format!(" SPOOLS ({} changed) ", changes), Some(changes))
            }
            _ => (format!(" SPOOLS ({}×{}) ", SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE), None),
        };

        let block = Block::default()
            .title(Span::styled(title, self.theme.header_style()))
            .borders(Borders::ALL)
            .border_style(self.theme.border_style());

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height < GROUP_ROWS as u16 || inner.width < (GROUP_COLS + 2) as u16 {
            return;
        }

        let content_width = (inner.width as usize).saturating_sub(2);
        let groups_per_row = ((content_width + 1) / (GROUP_COLS + 1)).max(1);

        let mut y = inner.y;
        let mut group_start = 0;
        let y_limit = inner.y + inner.height;

        while group_start < SPOOL_GROUP_COUNT && y + (GROUP_ROWS as u16) <= y_limit {
            let band_groups = (SPOOL_GROUP_COUNT - group_start).min(groups_per_row);

            for row in 0..GROUP_ROWS {
                if y >= y_limit {
                    break;
                }
                for g in 0..band_groups {
                    let group = group_start + g;
                    for col in 0..GROUP_COLS {
                        let spool_in_group = row * GROUP_COLS + col;
                        if spool_in_group >= SPOOL_GROUP_SIZE {
                            continue;
                        }
                        let spool_idx = group * SPOOL_GROUP_SIZE + spool_in_group;
                        if spool_idx >= SPOOL_COUNT {
                            continue;
                        }

                        let x = inner.x + 1 + (g * (GROUP_COLS + 1) + col) as u16;
                        if x >= inner.x + inner.width - 1 {
                            break;
                        }

                        let (symbol, style) = self.spool_style(spool_idx);
                        buf.set_string(x, y, symbol, style);
                    }
                }
                y += 1;
            }

            group_start += band_groups;

            // Gap between bands
            if group_start < SPOOL_GROUP_COUNT {
                y += 1;
            }
        }

        // Show indicator if not all groups fit
        if group_start < SPOOL_GROUP_COUNT {
            let remaining = SPOOL_GROUP_COUNT - group_start;
            let indicator = format!("(+{} groups) ", remaining);
            let x = inner.x + inner.width.saturating_sub(indicator.len() as u16);
            let y_pos = y_limit.saturating_sub(1);
            buf.set_string(x, y_pos, &indicator, self.theme.dim_style());
        }
    }
}
