//! Renders the Whirlwind latency histogram with plotters.
//!
//! A stacked bar per group position, four phases bottom to top (network, proof,
//! attestation, certificate), the q threshold marked, and the free-rider curve
//! overlaid to show it lands in the same latency envelope as the honest round.

use anyhow::{Context, Result};
use plotters::element::DashedPathElement;
use plotters::prelude::*;

use crate::sim::latency::PhaseCurve;

const WIDTH: u32 = 1280;
const HEIGHT: u32 = 720;

const NETWORK: RGBColor = RGBColor(46, 125, 50);
const PROOF: RGBColor = RGBColor(201, 133, 0);
const ATTEST: RGBColor = RGBColor(211, 47, 47);
const CERT: RGBColor = RGBColor(25, 118, 210);
const RECONSTRUCT: RGBColor = RGBColor(244, 0, 153);
const NEARBY: RGBColor = RGBColor(86, 0, 245);
const DEADLINE: RGBColor = RGBColor(90, 90, 90);

/// Render the curve, with the slot time fixing where the schedulable deadlines fall.
pub fn render(curve: &PhaseCurve, path: &str, slot_ms: f64) -> Result<()> {
    let root = BitMapBackend::new(path, (WIDTH, HEIGHT)).into_drawing_area();
    root.fill(&WHITE).context("fill background")?;

    let size = curve.group_size;
    // Keep the tightest schedulable deadline on the chart, since where the
    // curves sit against it is the whole question.
    let max_y = (curve.peak_ms() * 1.12).max(slot_ms * 1.15).max(1.0);
    let caption = format!(
        "Whirlwind Challenge Round Latency (n={}, q={}, {} placements, real pings)",
        size, curve.threshold, curve.rounds
    );

    let mut chart = ChartBuilder::on(&root)
        .caption(caption, ("sans-serif", 26))
        .margin(24)
        .x_label_area_size(56)
        .y_label_area_size(66)
        .build_cartesian_2d(0f64..size as f64, 0f64..max_y)
        .context("build chart")?;

    chart
        .configure_mesh()
        .x_desc("Spools reached, fastest first [% of group]")
        .y_desc("Latency [ms]")
        .x_label_formatter(&|value| format!("{:.0}", value / size as f64 * 100.0))
        .axis_desc_style(("sans-serif", 18))
        .label_style(("sans-serif", 14))
        .draw()
        .context("draw mesh")?;

    // Stacked bars: draw each phase as its own series so the legend labels it.
    let mut base = vec![0.0f64; size];
    let phases: [(&str, RGBColor, &Vec<f64>); 4] = [
        ("Network", NETWORK, &curve.network_ms),
        ("Proof", PROOF, &curve.proof_ms),
        ("Attestation", ATTEST, &curve.attest_ms),
        ("Certificate", CERT, &curve.cert_ms),
    ];
    for (name, color, values) in phases {
        // Name the phase's size, because at these scales the compute phases are
        // a sliver against the network and read as missing from the legend.
        // Judge on the peak, since the certificate is zero below q.
        let peak = values.iter().copied().fold(0.0, f64::max);
        let name = if peak < max_y * 0.01 {
            format!("{name} ({peak:.2} ms, too small to see)")
        } else {
            name.to_string()
        };
        let rectangles: Vec<Rectangle<(f64, f64)>> = (0..size)
            .map(|position| {
                let x0 = position as f64 + 0.08;
                let x1 = position as f64 + 0.92;
                let y0 = base[position];
                let y1 = base[position] + values[position];
                Rectangle::new([(x0, y0), (x1, y1)], color.filled())
            })
            .collect();
        for position in 0..size {
            base[position] += values[position];
        }
        chart
            .draw_series(rectangles)
            .with_context(|| format!("draw {name} bars"))?
            .label(name)
            .legend(move |(x, y)| Rectangle::new([(x, y - 6), (x + 12, y + 6)], color.filled()));
    }

    // The schedulable deadlines. A curve passing under the intersection of a
    // deadline and the q line still certifies, which is how the chart answers
    // who the deadline can actually separate.
    for slots in 1..=3u64 {
        let deadline = slots as f64 * slot_ms;
        if deadline > max_y {
            break;
        }
        let style = DEADLINE.mix(0.9).stroke_width(2);
        chart
            .draw_series(std::iter::once(DashedPathElement::new(
                vec![(0.0, deadline), (size as f64, deadline)],
                8,
                6,
                style,
            )))
            .context("draw deadline")?
            .label(format!("{slots} slot deadline ({deadline:.0} ms)"))
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 14, y)], DEADLINE.stroke_width(2))
            });
    }

    // The q threshold: the certificate exists from this many attestations on.
    let threshold_x = (curve.threshold.saturating_sub(1)) as f64;
    chart
        .draw_series(std::iter::once(PathElement::new(
            vec![(threshold_x, 0.0), (threshold_x, max_y)],
            BLACK.stroke_width(2),
        )))
        .context("draw threshold")?
        .label(format!("q = {}", curve.threshold))
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 14, y)], BLACK.stroke_width(2)));

    // Reconstruction free-rider: rebuilds from group helpers, one global round
    // trip above honest, so at global scale it clears the honest envelope.
    chart
        .draw_series(LineSeries::new(
            (0..size).map(|position| (position as f64 + 0.5, curve.freerider_total_ms[position])),
            RECONSTRUCT.stroke_width(3),
        ))
        .context("draw reconstruct free-rider")?
        .label("Free-rider (reconstructs from group)")
        .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 14, y)], RECONSTRUCT.stroke_width(3)));

    // Nearby-fetch free-rider: reads the public bytes from a metro edge, so it
    // sits inside the honest envelope and no deadline separates it.
    chart
        .draw_series(LineSeries::new(
            (0..size).map(|position| (position as f64 + 0.5, curve.nearby_total_ms[position])),
            NEARBY.stroke_width(3),
        ))
        .context("draw nearby free-rider")?
        .label("Free-rider (nearby fetch)")
        .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 14, y)], NEARBY.stroke_width(3)));

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.9))
        .border_style(BLACK.mix(0.4))
        .label_font(("sans-serif", 14))
        .draw()
        .context("draw legend")?;

    // Say what a bar is, since sorted order statistics read as node identities.
    root.draw_text(
        "Each bar is the k-th fastest peer averaged over placements, not one node. A curve under the q line and a deadline still certifies.",
        &("sans-serif", 13).into_font().color(&BLACK.mix(0.55)),
        (28, HEIGHT as i32 - 16),
    )
    .context("draw footnote")?;

    root.present().context("write png")?;
    Ok(())
}
