use ratatui::style::{Color, Style};
use ratatui::text::Span;

const BRAILLE_SPARK_HEIGHT: u8 = 4;
const BRAILLE_COLOR_STEPS: u8 = 3;
const FUTURE_POINT_SENTINEL_LEVEL: u8 = 1;

pub fn render_braille_sparkline(
    data: &[u64],
    width: usize,
    scale_max: Option<u64>,
) -> Vec<Span<'static>> {
    if width == 0 {
        return vec![];
    }

    let needed = width * 2;
    let start = data.len().saturating_sub(needed);
    let visible = &data[start..];
    let max_color_step = BRAILLE_SPARK_HEIGHT * BRAILLE_COLOR_STEPS;
    let data_max = scale_max
        .unwrap_or_else(|| visible.iter().copied().max().unwrap_or(0))
        .max(1);

    let mut spans: Vec<Span<'static>> = Vec::with_capacity(width);

    for i in 0..width {
        let li = i * 2;
        let ri = li + 1;

        let has_left = li < visible.len();
        let has_right = ri < visible.len();
        let is_future_pair = !has_left && !has_right;

        let lv = if has_left {
            let raw = visible[li];
            quantized_level(raw, data_max)
        } else {
            0
        };
        let rv = if has_right {
            let raw = visible[ri];
            quantized_level(raw, data_max)
        } else {
            0
        };

        let left_level = if is_future_pair {
            FUTURE_POINT_SENTINEL_LEVEL
        } else {
            ((lv + BRAILLE_COLOR_STEPS - 1) / BRAILLE_COLOR_STEPS).min(BRAILLE_SPARK_HEIGHT)
        };
        let right_level = if is_future_pair {
            FUTURE_POINT_SENTINEL_LEVEL
        } else {
            ((rv + BRAILLE_COLOR_STEPS - 1) / BRAILLE_COLOR_STEPS).min(BRAILLE_SPARK_HEIGHT)
        };

        let ch = match (left_level, right_level) {
            (0, 0) => ' ',
            (0, 1) => '⢀',
            (0, 2) => '⢠',
            (0, 3) => '⢰',
            (0, 4) => '⢸',

            (1, 0) => '⡀',
            (1, 1) => '⣀',
            (1, 2) => '⣠',
            (1, 3) => '⣰',
            (1, 4) => '⣸',

            (2, 0) => '⡄',
            (2, 1) => '⣄',
            (2, 2) => '⣤',
            (2, 3) => '⣴',
            (2, 4) => '⣾',

            (3, 0) => '⡆',
            (3, 1) => '⣆',
            (3, 2) => '⣦',
            (3, 3) => '⣶',
            (3, 4) => '⣾',

            (4, 0) => '⡇',
            (4, 1) => '⣇',
            (4, 2) => '⣧',
            (4, 3) => '⣷',
            (4, 4) => '⣿',

            _ => 'X',
        };

        if is_future_pair {
            spans.push(Span::styled(
                ch.to_string(),
                Style::default().fg(Color::Rgb(80, 80, 80)),
            ));
        } else {
            let peak = lv.max(rv).min(max_color_step);
            let color = btop_gradient(peak, max_color_step);
            spans.push(Span::styled(ch.to_string(), Style::default().fg(color)));
        }
    }

    spans
}

pub fn render_node_sparkline(data: &[u64], width: usize, scale_max: u64) -> Vec<Span<'static>> {
    if width == 0 {
        return vec![];
    }

    let visible = &data[data.len().saturating_sub(width)..];
    let glyph_width = (width + 1) / 2;

    render_braille_sparkline(visible, glyph_width, Some(scale_max))
}

fn quantized_level(value: u64, data_max: u64) -> u8 {
    if value == 0 || data_max == 0 {
        return 0;
    }

    let max_color_step = (BRAILLE_SPARK_HEIGHT * BRAILLE_COLOR_STEPS) as f64;
    let raw = (value as f64 / data_max as f64) * max_color_step;
    let level = raw.ceil().clamp(0.0, max_color_step) as u8;
    level.max(1)
}

fn lerp_rgb(a: (u8, u8, u8), b: (u8, u8, u8), t: f64) -> (u8, u8, u8) {
    (
        (a.0 as f64 + (b.0 as f64 - a.0 as f64) * t) as u8,
        (a.1 as f64 + (b.1 as f64 - a.1 as f64) * t) as u8,
        (a.2 as f64 + (b.2 as f64 - a.2 as f64) * t) as u8,
    )
}

fn btop_gradient(value: u8, max: u8) -> Color {
    if value == 0 {
        return Color::Rgb(80, 80, 80);
    }
    let ratio = value as f64 / max as f64;
    let (r, g, b) = if ratio <= 0.5 {
        let t = ratio * 2.0;
        lerp_rgb((0x77, 0xca, 0x9b), (0xcb, 0xc0, 0x6c), t)
    } else {
        let t = (ratio - 0.5) * 2.0;
        lerp_rgb((0xcb, 0xc0, 0x6c), (0xdc, 0x4c, 0x4c), t)
    };
    Color::Rgb(r, g, b)
}
