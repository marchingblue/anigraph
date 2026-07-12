//! Shared progress-indicator helper for item-stream pipeline phases.
//!
//! Enumeration (`enumerate.rs`) walks the AniList ID space and reports progress
//! against the ID range; the enrichment / cross-ref phases here process a known
//! number of input entries. Both share the same visual style:
//!
//! ```text
//! tvdb [####------] 42.0% | 4200/10000 (5800 left) | with_ids=1200 failures=3 | ETA 3m12s
//! ```

use std::time::{Duration, Instant};

/// Format a `Duration` compactly: `1h23m`, `12m34s`, or `45s`.
pub fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

/// Render a 20-cell progress bar for a 0..=100 percentage.
fn progress_bar(pct: f64) -> String {
    let filled = ((pct / 100.0) * 20.0).round() as usize;
    format!(
        "[{}{}]",
        "#".repeat(filled),
        "-".repeat(20usize.saturating_sub(filled))
    )
}

/// Log a one-line progress update for a non-paginated (item-stream) phase.
///
/// `done`/`total` drive the percentage and bar. `start`, if provided, powers
/// a rough ETA from the observed per-item throughput. `extras` appends
/// `key=value` metrics (e.g. `failures=3`).
pub fn log_progress(
    name: &str,
    done: u64,
    total: u64,
    start: Option<Instant>,
    extras: &[(&str, u64)],
) {
    let total = total.max(1);
    let done = done.min(total);
    let pct = 100.0 * done as f64 / total as f64;
    let left = total.saturating_sub(done);

    let eta = match start {
        Some(s) if done > 0 => {
            let per = s
                .elapsed()
                .checked_div(done as u32)
                .unwrap_or(Duration::ZERO);
            fmt_duration(per.saturating_mul(left as u32))
        }
        _ => "?".to_string(),
    };

    let extra = extras
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");

    if extra.is_empty() {
        tracing::info!(
            "{name} {} {pct:.1}% | {done}/{total} ({left} left) | ETA {eta}",
            progress_bar(pct),
        );
    } else {
        tracing::info!(
            "{name} {} {pct:.1}% | {done}/{total} ({left} left) | {extra} | ETA {eta}",
            progress_bar(pct),
        );
    }
}
