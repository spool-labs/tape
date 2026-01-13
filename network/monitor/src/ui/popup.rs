//! Modal popup widget for node detail view.
//!
//! Displays detailed information about a selected node in a centered overlay.

use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap},
};

use crate::app::{HealthStatus, NodeState};
use crate::theme::Theme;

/// Width of the popup as percentage of terminal width.
const POPUP_WIDTH_PERCENT: u16 = 70;

/// Height of the popup as percentage of terminal height.
const POPUP_HEIGHT_PERCENT: u16 = 80;

/// Minimum popup width.
const MIN_WIDTH: u16 = 60;

/// Minimum popup height.
const MIN_HEIGHT: u16 = 20;

/// Widget for rendering the node detail popup.
pub struct NodeDetailPopup<'a> {
    /// Node state to display.
    node: &'a NodeState,
    /// Theme reference.
    theme: &'a Theme,
}

impl<'a> NodeDetailPopup<'a> {
    /// Create a new node detail popup.
    pub fn new(node: &'a NodeState, theme: &'a Theme) -> Self {
        Self { node, theme }
    }

    /// Calculate the centered popup area.
    pub fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
        let popup_width = (area.width * percent_x / 100).max(MIN_WIDTH).min(area.width);
        let popup_height = (area.height * percent_y / 100).max(MIN_HEIGHT).min(area.height);

        let x = (area.width.saturating_sub(popup_width)) / 2;
        let y = (area.height.saturating_sub(popup_height)) / 2;

        Rect::new(
            area.x + x,
            area.y + y,
            popup_width,
            popup_height,
        )
    }

    /// Build the status line.
    fn build_status_line(&self) -> Line<'a> {
        let (status_text, status_style) = match self.node.health {
            HealthStatus::Online => {
                let latency = self.node.latency_display();
                (format!("* Online ({})", latency), self.theme.online_style())
            }
            HealthStatus::Offline => ("o Offline".to_string(), self.theme.offline_style()),
            HealthStatus::Syncing => ("~ Syncing".to_string(), self.theme.syncing_style()),
            HealthStatus::Unknown => (". Unknown".to_string(), self.theme.unknown_style()),
        };

        Line::from(vec![
            Span::styled("Status:     ", self.theme.text_style()),
            Span::styled(status_text, status_style),
        ])
    }

    /// Build info lines for the popup content.
    fn build_info_lines(&self) -> Vec<Line<'a>> {
        vec![
            self.build_status_line(),
            Line::from(vec![
                Span::styled("Address:    ", self.theme.text_style()),
                Span::styled(self.node.address.clone(), self.theme.text_style()),
            ]),
            Line::from(vec![
                Span::styled("Authority:  ", self.theme.text_style()),
                Span::styled(self.node.authority.clone(), self.theme.dim_style()),
            ]),
            Line::default(),
            Line::from(vec![
                Span::styled("Stake:      ", self.theme.text_style()),
                Span::styled(
                    format!("{} TAPE", self.node.stake_display()),
                    Style::default().fg(self.theme.primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("Spools:     ", self.theme.text_style()),
                Span::styled(
                    format!("{} / 1024 ({:.1}%)",
                        self.node.spool_count,
                        self.node.spool_count as f64 / 1024.0 * 100.0
                    ),
                    self.theme.text_style(),
                ),
            ]),
            Line::from(vec![
                Span::styled("Commission: ", self.theme.text_style()),
                Span::styled(self.node.commission_display(), self.theme.text_style()),
            ]),
        ]
    }

    /// Build metrics section.
    fn build_metrics_section(&self) -> Vec<Line<'a>> {
        // Calculate estimated storage responsibility based on spool allocation
        // Each spool is 1/1024 of total network storage
        let storage_share = self.node.spool_count as f64 / 1024.0 * 100.0;

        vec![
            Line::default(),
            Line::styled("NETWORK SHARE", self.theme.header_style()),
            Line::styled("-".repeat(40), self.theme.dim_style()),
            Line::from(vec![
                Span::styled("Spool share:  ", self.theme.text_style()),
                Span::styled(
                    format!("{:.2}% of network", storage_share),
                    self.theme.text_style(),
                ),
            ]),
            Line::from(vec![
                Span::styled("Last check:   ", self.theme.text_style()),
                Span::styled(
                    format!("{:.1}s ago", self.node.last_check.elapsed().as_secs_f64()),
                    self.theme.dim_style(),
                ),
            ]),
        ]
    }

    /// Build assigned spools section.
    fn build_spools_section(&self) -> Vec<Line<'a>> {
        let mut lines = vec![
            Line::default(),
            Line::styled("ASSIGNED SPOOLS", self.theme.header_style()),
            Line::styled("-".repeat(40), self.theme.dim_style()),
        ];

        if self.node.assigned_spools.is_empty() {
            lines.push(Line::styled("No spools assigned", self.theme.dim_style()));
        } else {
            // Show all spools as a comma-separated list
            let spools_str: String = self.node.assigned_spools
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            lines.push(Line::styled(spools_str, self.theme.text_style()));
        }

        lines
    }
}

impl Widget for NodeDetailPopup<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Calculate popup area
        let popup_area = Self::centered_rect(POPUP_WIDTH_PERCENT, POPUP_HEIGHT_PERCENT, area);

        // Clear the background
        Clear.render(popup_area, buf);

        // Build the block
        let title = format!(" NODE #{} - {} ", self.node.id.0, self.node.name);
        let block = Block::default()
            .title(Span::styled(title, self.theme.title_style()))
            .title_alignment(Alignment::Left)
            .borders(Borders::ALL)
            .border_style(self.theme.border_focus_style());

        let inner = block.inner(popup_area);
        block.render(popup_area, buf);

        // Add close hint
        let close_hint = "[x] ESC";
        if popup_area.width > close_hint.len() as u16 + 4 {
            let hint_x = popup_area.x + popup_area.width - close_hint.len() as u16 - 2;
            buf.set_string(
                hint_x,
                popup_area.y,
                close_hint,
                self.theme.dim_style(),
            );
        }

        // Build content
        let mut lines = self.build_info_lines();
        lines.extend(self.build_metrics_section());
        lines.extend(self.build_spools_section());

        let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
        paragraph.render(inner, buf);
    }
}

/// Render the help popup.
pub fn render_help_popup(area: Rect, buf: &mut Buffer, theme: &Theme) {
    let popup_area = NodeDetailPopup::centered_rect(60, 70, area);

    // Clear background
    Clear.render(popup_area, buf);

    let block = Block::default()
        .title(Span::styled(" HELP ", theme.title_style()))
        .borders(Borders::ALL)
        .border_style(theme.border_focus_style());

    let inner = block.inner(popup_area);
    block.render(popup_area, buf);

    let help_text = vec![
        Line::styled("NAVIGATION", theme.header_style()),
        Line::styled("==========", theme.dim_style()),
        Line::from(vec![
            Span::styled("^/v/</> ", theme.keybind_style()),
            Span::styled("Navigate", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("Enter   ", theme.keybind_style()),
            Span::styled("Select / View details", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("ESC     ", theme.keybind_style()),
            Span::styled("Back / Close popup", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("Tab     ", theme.keybind_style()),
            Span::styled("Next panel", theme.text_style()),
        ]),
        Line::default(),
        Line::styled("VIEWS", theme.header_style()),
        Line::styled("=====", theme.dim_style()),
        Line::from(vec![
            Span::styled("n       ", theme.keybind_style()),
            Span::styled("Node list", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("t       ", theme.keybind_style()),
            Span::styled("Track search", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("e       ", theme.keybind_style()),
            Span::styled("Epoch history", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("s       ", theme.keybind_style()),
            Span::styled("Spool map", theme.text_style()),
        ]),
        Line::default(),
        Line::styled("ACTIONS", theme.header_style()),
        Line::styled("=======", theme.dim_style()),
        Line::from(vec![
            Span::styled("r       ", theme.keybind_style()),
            Span::styled("Force refresh", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("/       ", theme.keybind_style()),
            Span::styled("Search", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("q       ", theme.keybind_style()),
            Span::styled("Quit", theme.text_style()),
        ]),
        Line::default(),
        Line::styled("INDICATORS", theme.header_style()),
        Line::styled("==========", theme.dim_style()),
        Line::from(vec![
            Span::styled("● ", theme.online_style()),
            Span::styled("Online   ", theme.text_style()),
            Span::styled("○ ", theme.offline_style()),
            Span::styled("Offline   ", theme.text_style()),
            Span::styled("◐ ", theme.syncing_style()),
            Span::styled("Syncing   ", theme.text_style()),
            Span::styled("◌ ", theme.unknown_style()),
            Span::styled("Unknown", theme.text_style()),
        ]),
        Line::default(),
        Line::styled("Press any key to close", theme.dim_style()),
    ];

    let paragraph = Paragraph::new(help_text);
    paragraph.render(inner, buf);
}
