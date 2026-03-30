use dioxus::prelude::*;

use crate::state::AppState;

/// Severity level of a run-log line.
/// All variants used in Phase 5 live log streaming; Phase 1 demo uses a subset.
#[derive(Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum Level {
    Cmd,
    Info,
    Stat,
    Ok,
    Warn,
    Err,
}

impl Level {
    fn tag(self) -> &'static str {
        match self {
            Level::Cmd => "  $",
            Level::Info => "INF",
            Level::Stat => "DAT",
            Level::Ok => " OK",
            Level::Warn => "WRN",
            Level::Err => "ERR",
        }
    }

    fn data_attr(self) -> &'static str {
        match self {
            Level::Cmd => "cmd",
            Level::Info => "info",
            Level::Stat => "stat",
            Level::Ok => "ok",
            Level::Warn => "warn",
            Level::Err => "err",
        }
    }
}

/// A single log entry in the run-log drawer.
struct LogLine {
    /// Timestamp in `MM:SS.mmm` format.
    timestamp: &'static str,
    level: Level,
    message: &'static str,
}

/// Returns the hardcoded demo log content for Phase 1.
fn demo_log() -> Vec<LogLine> {
    vec![
        LogLine { timestamp: "00:00.000", level: Level::Cmd,  message: "$ clinker run customer_etl.yaml --verbose" },
        LogLine { timestamp: "00:00.012", level: Level::Info, message: "clinker v0.1.0 — loading pipeline config" },
        LogLine { timestamp: "00:00.018", level: Level::Info, message: "schema: clinker-pipeline-schema-3.json loaded" },
        LogLine { timestamp: "00:00.021", level: Level::Info, message: "source: csv_reader — pattern customers_*.csv" },
        LogLine { timestamp: "00:00.025", level: Level::Stat, message: "source matched 3 files (customers_2023.csv, customers_2024.csv, …)" },
        LogLine { timestamp: "00:00.030", level: Level::Info, message: "─── PASS 1: SCAN ───" },
        LogLine { timestamp: "00:00.180", level: Level::Stat, message: "scan complete — 47,283 rows · 12 columns · 3 files" },
        LogLine { timestamp: "00:00.182", level: Level::Info, message: "─── PASS 2: TRANSFORM ───" },
        LogLine { timestamp: "00:00.215", level: Level::Stat, message: "filter active_only: 47,283 → 39,104 rows (Δ -8,179)" },
        LogLine { timestamp: "00:00.302", level: Level::Stat, message: "map enrich: 39,104 → 39,104 rows (no change)" },
        LogLine { timestamp: "00:00.418", level: Level::Ok,   message: "pipeline complete — 39,104 rows written in 418ms" },
    ]
}

/// Collapsible bottom drawer showing `clinker run` output.
///
/// Streaming output (via `tokio::process::Command`) is implemented in Phase 5.
/// Phase 1 shows static demo log content.
///
/// Collapsed height: 28 px (tab bar only).
/// Expanded height: 220 px (tab bar + scrollable log content).
/// Transition: `height 300ms ease-out` via CSS `data-expanded` attribute.
///
/// Doc: spec §7 — Run Log.
#[component]
pub fn RunLogDrawer() -> Element {
    let state = use_context::<AppState>();

    rsx! {
        div {
            class: "kiln-run-log",
            "data-expanded": if (state.run_log_expanded)() { "true" } else { "false" },

            // ─── Tab bar (always visible) ────────────────────────────────
            div {
                class: "kiln-run-log-tab",
                onclick: move |_| {
                    // Separate let bindings: release the immutable peek borrow
                    // before taking the mutable borrow for set() (borrow rules).
                    let mut exp = state.run_log_expanded;
                    let current = *exp.peek();
                    exp.set(!current);
                },

                // Status LED
                span { class: "kiln-run-log-led" }
                // Label
                span { class: "kiln-run-log-label", "RUN LOG" }
                // Expand / collapse chevron
                span {
                    class: "kiln-run-log-chevron",
                    if (state.run_log_expanded)() { "▲" } else { "▼" }
                }
            }

            // ─── Log content (visible when expanded) ─────────────────────
            div {
                class: "kiln-run-log-content",

                for (i, line) in demo_log().iter().enumerate() {
                    div {
                        key: "log-{i}",
                        class: "kiln-log-line",
                        "data-level": line.level.data_attr(),

                        span { class: "kiln-log-ts",  "{line.timestamp}" }
                        span { class: "kiln-log-tag", "{line.level.tag()}" }
                        span { class: "kiln-log-msg", "{line.message}" }
                    }
                }
            }
        }
    }
}
