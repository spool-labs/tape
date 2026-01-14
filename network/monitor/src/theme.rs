//! Theme and color definitions for the Tapedrive Network Monitor.
//!
//! Brand colors: White, Yellow (#FFC857), Orange (#FF9F43)

use ratatui::style::{Color, Modifier, Style};

/// Tapedrive brand and UI color palette.
#[derive(Debug, Clone, Copy)]
pub struct Theme {
    /// Dark background color
    pub bg: Color,
    /// Primary foreground (white brand)
    pub fg: Color,

    // Status colors
    /// Online/healthy status (yellow brand)
    pub online: Color,
    /// Offline/error status
    pub offline: Color,
    /// Syncing/in-progress status (orange brand)
    pub syncing: Color,
    /// Unknown status
    pub unknown: Color,

    // Accent colors
    /// Primary accent (yellow brand)
    pub primary: Color,
    /// Secondary accent (orange brand)
    pub secondary: Color,
    /// Warning indicator (orange brand)
    pub warning: Color,
    /// Success indicator (yellow brand)
    pub success: Color,
    /// Error indicator
    pub error: Color,

    // Progress bar
    /// Progress bar foreground (yellow brand)
    pub progress_fg: Color,
    /// Progress bar background
    pub progress_bg: Color,

    // Borders
    /// Normal border color
    pub border: Color,
    /// Focused border color (yellow brand)
    pub border_focus: Color,

    // Text hierarchy
    /// Bright/highlighted text
    pub text_bright: Color,
    /// Normal text
    pub text_normal: Color,
    /// Dimmed text
    pub text_dim: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self::dark()
    }
}

impl Theme {
    /// Dark theme with Tapedrive brand colors.
    /// Uses Color::Reset for background to respect terminal's native colors.
    pub const fn dark() -> Self {
        Self {
            bg: Color::Reset, // Use terminal's native background
            fg: Color::Rgb(245, 245, 250),

            // Status colors
            online: Color::Rgb(255, 200, 87),   // Yellow (brand) - healthy
            offline: Color::Rgb(255, 107, 107), // Soft red - errors
            syncing: Color::Rgb(255, 159, 67),  // Orange (brand) - in progress
            unknown: Color::Rgb(98, 98, 110),   // Gray - unknown

            // Accent colors
            primary: Color::Rgb(255, 200, 87),   // Yellow (brand)
            secondary: Color::Rgb(255, 159, 67), // Orange (brand)
            warning: Color::Rgb(255, 159, 67),   // Orange (brand)
            success: Color::Rgb(255, 200, 87),   // Yellow (brand)
            error: Color::Rgb(255, 107, 107),    // Soft red

            // Progress bar
            progress_fg: Color::Rgb(255, 200, 87), // Yellow (brand)
            progress_bg: Color::Rgb(45, 45, 55),   // Dark gray

            // Borders
            border: Color::Rgb(60, 60, 75),        // Subtle gray
            border_focus: Color::Rgb(255, 200, 87), // Yellow (brand)

            // Text hierarchy
            text_bright: Color::Rgb(255, 255, 255), // Pure white
            text_normal: Color::Rgb(200, 200, 210), // Off-white
            text_dim: Color::Rgb(128, 128, 140),    // Dimmed
        }
    }

    /// Get a distinct color for a committee member (0-127).
    /// Uses HSL color space with varying hue, saturation, and lightness
    /// to generate 128 visually distinct colors.
    pub fn member_color(index: usize) -> Color {
        // Wrap to 128 colors max
        let idx = index % 128;

        // Generate color using golden ratio for better hue distribution
        // This spreads colors more evenly than linear rotation
        let golden_ratio = 0.618033988749895;
        let hue = ((idx as f64 * golden_ratio) % 1.0) * 360.0;

        // Vary saturation and lightness based on index to increase distinctness
        let saturation = match idx % 3 {
            0 => 0.85,
            1 => 0.65,
            _ => 0.75,
        };
        let lightness = match (idx / 3) % 3 {
            0 => 0.55,
            1 => 0.45,
            _ => 0.65,
        };

        hsl_to_rgb(hue, saturation, lightness)
    }

    /// Get a spool color by node index (alias for member_color).
    pub fn spool_color(&self, index: usize) -> Color {
        Self::member_color(index)
    }

    // ========================================================================
    // Style Helpers
    // ========================================================================

    /// Style for the application title.
    pub fn title_style(&self) -> Style {
        Style::default()
            .fg(self.primary)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for section headers.
    pub fn header_style(&self) -> Style {
        Style::default()
            .fg(self.text_bright)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for normal text.
    pub fn text_style(&self) -> Style {
        Style::default().fg(self.text_normal)
    }

    /// Style for dimmed/secondary text.
    pub fn dim_style(&self) -> Style {
        Style::default().fg(self.text_dim)
    }

    /// Style for highlighted/selected items.
    pub fn highlight_style(&self) -> Style {
        Style::default()
            .fg(self.primary)
            .bg(Color::Rgb(65, 65, 75))
            .add_modifier(Modifier::BOLD)
    }

    /// Style for normal borders.
    pub fn border_style(&self) -> Style {
        Style::default().fg(self.border)
    }

    /// Style for focused borders.
    pub fn border_focus_style(&self) -> Style {
        Style::default().fg(self.border_focus)
    }

    /// Style for online status.
    pub fn online_style(&self) -> Style {
        Style::default().fg(self.online)
    }

    /// Style for offline status.
    pub fn offline_style(&self) -> Style {
        Style::default().fg(self.offline)
    }

    /// Style for syncing status.
    pub fn syncing_style(&self) -> Style {
        Style::default().fg(self.syncing)
    }

    /// Style for unknown status.
    pub fn unknown_style(&self) -> Style {
        Style::default().fg(self.unknown)
    }

    /// Style for success messages.
    pub fn success_style(&self) -> Style {
        Style::default().fg(self.success)
    }

    /// Style for error messages.
    pub fn error_style(&self) -> Style {
        Style::default().fg(self.error)
    }

    /// Style for warning messages.
    pub fn warning_style(&self) -> Style {
        Style::default().fg(self.warning)
    }

    /// Style for keybinding hints in the status bar.
    pub fn keybind_style(&self) -> Style {
        Style::default()
            .fg(self.primary)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for keybinding descriptions.
    pub fn keybind_desc_style(&self) -> Style {
        Style::default().fg(self.text_dim)
    }
}

/// Global theme instance.
static THEME: Theme = Theme::dark();

/// Get the current theme.
pub fn theme() -> &'static Theme {
    &THEME
}

/// Convert HSL color to RGB.
/// h: 0-360 (hue in degrees)
/// s: 0-1 (saturation)
/// l: 0-1 (lightness)
fn hsl_to_rgb(h: f64, s: f64, l: f64) -> Color {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let h_prime = h / 60.0;
    let x = c * (1.0 - ((h_prime % 2.0) - 1.0).abs());
    let m = l - c / 2.0;

    let (r1, g1, b1) = if h_prime < 1.0 {
        (c, x, 0.0)
    } else if h_prime < 2.0 {
        (x, c, 0.0)
    } else if h_prime < 3.0 {
        (0.0, c, x)
    } else if h_prime < 4.0 {
        (0.0, x, c)
    } else if h_prime < 5.0 {
        (x, 0.0, c)
    } else {
        (c, 0.0, x)
    };

    Color::Rgb(
        ((r1 + m) * 255.0).round() as u8,
        ((g1 + m) * 255.0).round() as u8,
        ((b1 + m) * 255.0).round() as u8,
    )
}
