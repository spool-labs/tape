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

use crate::app::{HealthStatus, NodeState, TOTAL_SPOOLS};
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
                Span::styled("Spools:     ", self.theme.text_style()),
                Span::styled(
                    format!("{} / {} ({:.1}%)",
                        self.node.spool_count,
                        TOTAL_SPOOLS,
                        self.node.spool_count as f64 / TOTAL_SPOOLS as f64 * 100.0
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

    /// Build stake schedule section.
    fn build_stake_section(&self) -> Vec<Line<'a>> {
        let mut lines = vec![
            Line::default(),
            Line::styled("STAKE", self.theme.header_style()),
            Line::styled("-".repeat(40), self.theme.dim_style()),
            Line::from(vec![
                Span::styled("Committee:    ", self.theme.text_style()),
                Span::styled(
                    format!("{} TAPE", format_tape(self.node.stake.0)),
                    Style::default().fg(self.theme.primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("Pool:         ", self.theme.text_style()),
                Span::styled(
                    format!("{} TAPE", format_tape(self.node.pool_stake.0)),
                    self.theme.text_style(),
                ),
            ]),
        ];

        // Show stake schedule table if there are any scheduled changes
        if !self.node.stake_schedule.is_empty() {
            lines.push(Line::default());
            lines.push(Line::styled("SCHEDULE", self.theme.header_style()));
            // Header row
            lines.push(Line::from(vec![
                Span::styled("Epoch     ", self.theme.dim_style()),
                Span::styled("Incoming      ", self.theme.dim_style()),
                Span::styled("Cancels", self.theme.dim_style()),
            ]));
            lines.push(Line::styled("-".repeat(40), self.theme.dim_style()));

            // Data rows for each scheduled epoch
            for (epoch, entry) in &self.node.stake_schedule {
                let incoming_str = if entry.incoming.0 > 0 {
                    format!("+{}", format_tape(entry.incoming.0))
                } else {
                    "-".to_string()
                };
                let cancels_str = if entry.cancels.0 > 0 {
                    format!("-{}", format_tape(entry.cancels.0))
                } else {
                    "-".to_string()
                };

                lines.push(Line::from(vec![
                    Span::styled(format!("{:<10}", epoch.0), self.theme.text_style()),
                    Span::styled(
                        format!("{:<14}", incoming_str),
                        if entry.incoming.0 > 0 {
                            Style::default().fg(ratatui::style::Color::Green)
                        } else {
                            self.theme.dim_style()
                        },
                    ),
                    Span::styled(
                        cancels_str,
                        if entry.cancels.0 > 0 {
                            Style::default().fg(ratatui::style::Color::Red)
                        } else {
                            self.theme.dim_style()
                        },
                    ),
                ]));
            }
        }

        lines
    }

    /// Build rewards section showing commission and staker rewards.
    fn build_rewards_section(&self) -> Vec<Line<'a>> {
        let mut lines = vec![
            Line::default(),
            Line::styled("REWARDS", self.theme.header_style()),
            Line::styled("-".repeat(40), self.theme.dim_style()),
        ];

        // Commission earned (claimable by operator)
        let commission_str = if self.node.commission_earned.0 > 0 {
            format_tape_amount(self.node.commission_earned.0)
        } else {
            "0 TAPE".to_string()
        };
        lines.push(Line::from(vec![
            Span::styled("Commission:   ", self.theme.text_style()),
            Span::styled(
                commission_str,
                Style::default().fg(self.theme.primary),
            ),
            Span::styled(
                format!(" ({})", self.node.commission_display()),
                self.theme.dim_style(),
            ),
        ]));

        // Rewards pool (distributable to stakers)
        let rewards_str = if self.node.rewards_pool.0 > 0 {
            format_tape_amount(self.node.rewards_pool.0)
        } else {
            "0 TAPE".to_string()
        };
        lines.push(Line::from(vec![
            Span::styled("Staker Pool:  ", self.theme.text_style()),
            Span::styled(
                rewards_str,
                self.theme.text_style(),
            ),
        ]));

        lines
    }

    /// Build metrics section.
    fn build_metrics_section(&self) -> Vec<Line<'a>> {
        // Calculate estimated storage responsibility based on spool allocation
        let storage_share = self.node.spool_count as f64 / TOTAL_SPOOLS as f64 * 100.0;

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

    /// Build node stats section.
    fn build_stats_section(&self) -> Vec<Line<'a>> {
        let mut lines = vec![
            Line::default(),
            Line::styled("STORAGE", self.theme.header_style()),
            Line::styled("-".repeat(40), self.theme.dim_style()),
        ];

        if let Some(stats) = &self.node.stats {
            lines.push(Line::from(vec![
                Span::styled("Slices:       ", self.theme.text_style()),
                Span::styled(
                    format_number(stats.slices_stored),
                    self.theme.text_style(),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Tracks:       ", self.theme.text_style()),
                Span::styled(
                    format_number(stats.tracks_stored),
                    self.theme.text_style(),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Disk used:    ", self.theme.text_style()),
                Span::styled(
                    format_bytes(stats.storage_bytes_used),
                    self.theme.text_style(),
                ),
            ]));
            lines.push(Line::default());
            lines.push(Line::styled("TRAFFIC", self.theme.header_style()));
            lines.push(Line::styled("-".repeat(40), self.theme.dim_style()));
            lines.push(Line::from(vec![
                Span::styled("Uploaded:     ", self.theme.text_style()),
                Span::styled(
                    format_bytes(stats.bytes_uploaded),
                    self.theme.text_style(),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Downloaded:   ", self.theme.text_style()),
                Span::styled(
                    format_bytes(stats.bytes_downloaded),
                    self.theme.text_style(),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Requests:     ", self.theme.text_style()),
                Span::styled(
                    format_number(stats.requests_total),
                    self.theme.text_style(),
                ),
            ]));
        } else {
            lines.push(Line::styled("No stats available", self.theme.dim_style()));
        }

        lines
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
        lines.extend(self.build_stake_section());
        lines.extend(self.build_rewards_section());
        lines.extend(self.build_metrics_section());
        lines.extend(self.build_stats_section());
        lines.extend(self.build_spools_section());

        let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
        paragraph.render(inner, buf);
    }
}

/// Format a number with thousand separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result
}

/// Format bytes with appropriate unit.
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000_000 {
        format!("{:.1} TB", bytes as f64 / 1_000_000_000_000.0)
    } else if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Format TAPE amount (flux units to display with thousand separators).
fn format_tape(flux: u64) -> String {
    let tape = flux / 1_000_000; // Convert from flux (6 decimals)
    format_number(tape)
}

/// Format TAPE amount with unit (handles μTAPE for small amounts).
fn format_tape_amount(flux: u64) -> String {
    if flux >= 1_000_000 {
        // 1 TAPE or more - show in TAPE
        let tape = flux / 1_000_000;
        format!("{} TAPE", format_number(tape))
    } else if flux > 0 {
        // Less than 1 TAPE - show in μTAPE (micro TAPE)
        format!("{} μTAPE", format_number(flux))
    } else {
        "0 TAPE".to_string()
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
            Span::styled("v       ", theme.keybind_style()),
            Span::styled("Event list", theme.text_style()),
        ]),
        Line::from(vec![
            Span::styled("e       ", theme.keybind_style()),
            Span::styled("Epoch history", theme.text_style()),
        ]),
        Line::default(),
        Line::styled("ACTIONS", theme.header_style()),
        Line::styled("=======", theme.dim_style()),
        Line::from(vec![
            Span::styled("r       ", theme.keybind_style()),
            Span::styled("Force refresh", theme.text_style()),
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
