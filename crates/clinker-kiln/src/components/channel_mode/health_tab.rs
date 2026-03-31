//! Health tab — override validation and stale detection.
//!
//! Loads each override file for the selected channel, validates against
//! base pipeline, reports errors/warnings. Detects stale overrides by
//! comparing file modification times.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use dioxus::prelude::*;

use crate::state::TabManagerState;

/// Severity level for health check issues.
#[derive(Clone, Debug, PartialEq, Eq)]
enum IssueSeverity {
    Error,
    Warning,
    Info,
}

impl IssueSeverity {
    fn icon(&self) -> &'static str {
        match self {
            Self::Error => "\u{2717}",   // ✗
            Self::Warning => "\u{26a0}", // ⚠
            Self::Info => "\u{2139}",    // ℹ
        }
    }

    fn class_suffix(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
            Self::Info => "info",
        }
    }
}

/// A single health check issue.
#[derive(Clone, Debug)]
struct HealthIssue {
    severity: IssueSeverity,
    message: String,
    file: PathBuf,
}

/// Health report for a single pipeline override.
#[derive(Clone, Debug)]
struct PipelineHealth {
    pipeline_stem: String,
    override_path: PathBuf,
    issues: Vec<HealthIssue>,
    is_stale: bool,
    stale_info: Option<String>,
}

impl PipelineHealth {
    fn status_icon(&self) -> &'static str {
        if self.issues.iter().any(|i| i.severity == IssueSeverity::Error) {
            "\u{2717}" // ✗
        } else if self.issues.iter().any(|i| i.severity == IssueSeverity::Warning)
            || self.is_stale
        {
            "\u{26a0}" // ⚠
        } else {
            "\u{25cf}" // ●
        }
    }

    fn status_class(&self) -> &'static str {
        if self.issues.iter().any(|i| i.severity == IssueSeverity::Error) {
            "kiln-health-entry--error"
        } else if self.issues.iter().any(|i| i.severity == IssueSeverity::Warning)
            || self.is_stale
        {
            "kiln-health-entry--warning"
        } else {
            "kiln-health-entry--ok"
        }
    }
}

/// Health tab root component.
#[component]
pub fn HealthTab() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let cs = (tab_mgr.channel_state)();
    let Some(ref channel_state) = cs else {
        return rsx! {};
    };

    let Some(ref active_id) = channel_state.active_channel else {
        return rsx! {
            div { class: "kiln-health-tab__empty",
                "Select a channel to run health checks."
            }
        };
    };

    let channel = channel_state.channels.iter().find(|c| c.id == *active_id);
    let Some(channel) = channel else {
        return rsx! {};
    };

    // Scan for override files and run health checks
    let channel_dir = channel_state
        .workspace_root
        .join(&channel_state.channels_dir)
        .join(&channel.id);
    let ws_root = &channel_state.workspace_root;

    let reports = run_health_checks(&channel_dir, ws_root);

    let total_issues: usize = reports.iter().map(|r| r.issues.len()).sum();
    let stale_count = reports.iter().filter(|r| r.is_stale).count();
    let error_count = reports
        .iter()
        .flat_map(|r| &r.issues)
        .filter(|i| i.severity == IssueSeverity::Error)
        .count();

    rsx! {
        div { class: "kiln-health-tab",
            // Summary header
            div { class: "kiln-health-tab__summary",
                h3 { class: "kiln-health-tab__title",
                    "Health Check \u{2014} {channel.name}"
                }
                div { class: "kiln-health-tab__stats",
                    span { class: "kiln-health-stat",
                        "{reports.len()} overrides checked"
                    }
                    if error_count > 0 {
                        span { class: "kiln-health-stat kiln-health-stat--error",
                            "{error_count} errors"
                        }
                    }
                    if total_issues > error_count {
                        span { class: "kiln-health-stat kiln-health-stat--warning",
                            "{total_issues - error_count} warnings"
                        }
                    }
                    if stale_count > 0 {
                        span { class: "kiln-health-stat kiln-health-stat--stale",
                            "{stale_count} stale"
                        }
                    }
                }
            }

            // Results
            div { class: "kiln-health-tab__results",
                if reports.is_empty() {
                    div { class: "kiln-health-tab__empty",
                        "No override files found for this channel."
                    }
                }

                for report in &reports {
                    div {
                        key: "{report.pipeline_stem}",
                        class: "kiln-health-entry {report.status_class()}",

                        div { class: "kiln-health-entry__header",
                            span { class: "kiln-health-entry__icon", "{report.status_icon()}" }
                            span { class: "kiln-health-entry__pipeline",
                                "{report.pipeline_stem}.yaml"
                            }
                            if report.is_stale {
                                if let Some(ref info) = report.stale_info {
                                    span { class: "kiln-health-entry__stale",
                                        "\u{2299} stale ({info})"
                                    }
                                }
                            }
                        }

                        if !report.issues.is_empty() {
                            div { class: "kiln-health-entry__issues",
                                for (idx, issue) in report.issues.iter().enumerate() {
                                    div {
                                        key: "{idx}",
                                        class: "kiln-health-issue kiln-health-issue--{issue.severity.class_suffix()}",
                                        span { class: "kiln-health-issue__icon",
                                            "{issue.severity.icon()}"
                                        }
                                        span { class: "kiln-health-issue__msg",
                                            "{issue.message}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Run health checks for all override files in a channel directory.
fn run_health_checks(channel_dir: &Path, ws_root: &Path) -> Vec<PipelineHealth> {
    let mut reports = Vec::new();

    let Ok(entries) = std::fs::read_dir(channel_dir) else {
        return reports;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".channel.yaml") || name == "channel.yaml" {
            continue;
        }

        let pipeline_stem = name
            .strip_suffix(".channel.yaml")
            .unwrap_or(name)
            .to_string();

        let mut issues = Vec::new();

        // Check 1: Does the base pipeline exist?
        let base_candidates = [
            ws_root.join(format!("pipelines/{pipeline_stem}.yaml")),
            ws_root.join(format!("{pipeline_stem}.yaml")),
        ];
        let base_path = base_candidates.iter().find(|p| p.exists());

        if base_path.is_none() {
            issues.push(HealthIssue {
                severity: IssueSeverity::Error,
                message: format!(
                    "Base pipeline '{pipeline_stem}.yaml' not found in workspace"
                ),
                file: path.clone(),
            });
        }

        // Check 2: Can the override file be parsed?
        match clinker_channel::channel_override::ChannelOverride::load(&path, &[]) {
            Ok(Some(_override)) => {
                // Override parsed successfully - further validation would require
                // loading and parsing the base pipeline, which we defer to Phase 4
            }
            Ok(None) => {
                // File doesn't exist (shouldn't happen since we found it)
            }
            Err(e) => {
                issues.push(HealthIssue {
                    severity: IssueSeverity::Error,
                    message: format!("Parse error: {e}"),
                    file: path.clone(),
                });
            }
        }

        // Stale detection: compare override mtime vs base mtime
        let (is_stale, stale_info) = if let Some(base) = base_path {
            check_staleness(&path, base)
        } else {
            (false, None)
        };

        reports.push(PipelineHealth {
            pipeline_stem,
            override_path: path,
            issues,
            is_stale,
            stale_info,
        });
    }

    reports.sort_by(|a, b| a.pipeline_stem.cmp(&b.pipeline_stem));
    reports
}

/// Check if an override file is stale relative to its base pipeline.
fn check_staleness(override_path: &Path, base_path: &Path) -> (bool, Option<String>) {
    let override_mtime = std::fs::metadata(override_path)
        .and_then(|m| m.modified())
        .ok();
    let base_mtime = std::fs::metadata(base_path)
        .and_then(|m| m.modified())
        .ok();

    match (override_mtime, base_mtime) {
        (Some(ovr), Some(base)) if base > ovr => {
            let elapsed = base
                .duration_since(ovr)
                .unwrap_or_default();
            let days = elapsed.as_secs() / 86400;
            let info = if days > 30 {
                format!("{} months", days / 30)
            } else if days > 0 {
                format!("{days} days")
            } else {
                "today".to_string()
            };
            (true, Some(info))
        }
        _ => (false, None),
    }
}
