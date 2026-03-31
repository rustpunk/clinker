//! Overrides tab — three sub-views: By Channel, By Pipeline, Matrix.
//!
//! Shows which pipelines a channel overrides, how channels map to pipelines,
//! and a coverage matrix (pipelines x channels heatmap).

use std::collections::HashMap;
use std::path::PathBuf;

use dioxus::prelude::*;

use crate::state::TabManagerState;

/// Sub-view within the Overrides tab.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum OverrideView {
    #[default]
    ByChannel,
    ByPipeline,
    Matrix,
}

impl OverrideView {
    fn label(self) -> &'static str {
        match self {
            Self::ByChannel => "By Channel",
            Self::ByPipeline => "By Pipeline",
            Self::Matrix => "Matrix",
        }
    }
}

const OVERRIDE_VIEWS: [OverrideView; 3] = [
    OverrideView::ByChannel,
    OverrideView::ByPipeline,
    OverrideView::Matrix,
];

/// Discovered override file info (pipeline stem + path).
#[derive(Clone, Debug)]
struct OverrideFile {
    pipeline_stem: String,
    path: PathBuf,
}

/// Overrides tab root component.
#[component]
pub fn OverridesTab() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut view_mode = use_signal(|| OverrideView::ByChannel);
    let current_view = (view_mode)();

    let cs = (tab_mgr.channel_state)();
    let Some(ref _channel_state) = cs else {
        return rsx! {};
    };

    rsx! {
        div { class: "kiln-overrides-tab",
            // View mode toggle
            div { class: "kiln-overrides-tab__views",
                for view in OVERRIDE_VIEWS {
                    button {
                        class: if current_view == view {
                            "kiln-overrides-view kiln-overrides-view--active"
                        } else {
                            "kiln-overrides-view"
                        },
                        onclick: move |_| view_mode.set(view),
                        "{view.label()}"
                    }
                }
            }

            div { class: "kiln-overrides-tab__content",
                match current_view {
                    OverrideView::ByChannel => rsx! { ByChannelView {} },
                    OverrideView::ByPipeline => rsx! { ByPipelineView {} },
                    OverrideView::Matrix => rsx! { MatrixView {} },
                }
            }
        }
    }
}

/// By Channel view — for the selected channel, list all pipelines it overrides.
#[component]
fn ByChannelView() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let cs = (tab_mgr.channel_state)();
    let Some(ref channel_state) = cs else {
        return rsx! {};
    };

    let Some(ref active_id) = channel_state.active_channel else {
        return rsx! {
            div { class: "kiln-overrides__empty", "Select a channel to see its overrides." }
        };
    };

    let channel = channel_state.channels.iter().find(|c| c.id == *active_id);
    let Some(channel) = channel else {
        return rsx! {};
    };

    // Scan channel directory for .channel.yaml files
    let channel_dir = channel_state
        .workspace_root
        .join(&channel_state.channels_dir)
        .join(&channel.id);
    let overrides = discover_override_files(&channel_dir);

    rsx! {
        div { class: "kiln-overrides-by-channel",
            h3 { class: "kiln-overrides__title",
                "{channel.name} — {overrides.len()} pipeline overrides"
            }

            if overrides.is_empty() {
                div { class: "kiln-overrides__empty",
                    "No override files found for this channel."
                }
            } else {
                div { class: "kiln-overrides__list",
                    for ovr in &overrides {
                        div {
                            key: "{ovr.pipeline_stem}",
                            class: "kiln-override-entry",

                            span { class: "kiln-override-entry__icon", "⟲" }
                            span { class: "kiln-override-entry__pipeline", "{ovr.pipeline_stem}.yaml" }
                            span { class: "kiln-override-entry__path",
                                "{ovr.path.file_name().unwrap_or_default().to_string_lossy()}"
                            }
                        }
                    }
                }
            }
        }
    }
}

/// By Pipeline view — group by pipeline, show channels that override each.
#[component]
fn ByPipelineView() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let cs = (tab_mgr.channel_state)();
    let Some(ref channel_state) = cs else {
        return rsx! {};
    };

    // Build pipeline -> channels map by scanning all channel directories
    let mut pipeline_channels: HashMap<String, Vec<String>> = HashMap::new();
    for channel in &channel_state.channels {
        let channel_dir = channel_state
            .workspace_root
            .join(&channel_state.channels_dir)
            .join(&channel.id);
        for ovr in discover_override_files(&channel_dir) {
            pipeline_channels
                .entry(ovr.pipeline_stem.clone())
                .or_default()
                .push(channel.id.clone());
        }
    }

    let mut pipelines: Vec<(String, Vec<String>)> = pipeline_channels.into_iter().collect();
    pipelines.sort_by(|a, b| a.0.cmp(&b.0));

    rsx! {
        div { class: "kiln-overrides-by-pipeline",
            h3 { class: "kiln-overrides__title",
                "{pipelines.len()} pipelines with channel overrides"
            }

            if pipelines.is_empty() {
                div { class: "kiln-overrides__empty",
                    "No channel overrides found in the workspace."
                }
            } else {
                div { class: "kiln-overrides__list",
                    for (pipeline, channels) in &pipelines {
                        div {
                            key: "{pipeline}",
                            class: "kiln-override-pipeline",

                            div { class: "kiln-override-pipeline__header",
                                span { class: "kiln-override-pipeline__name",
                                    "{pipeline}.yaml"
                                }
                                span { class: "kiln-override-pipeline__count",
                                    "{channels.len()} channels"
                                }
                            }
                            div { class: "kiln-override-pipeline__channels",
                                for ch in channels {
                                    span {
                                        key: "{ch}",
                                        class: "kiln-override-pipeline__channel-badge",
                                        "⟲ {ch}"
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

/// Matrix view — pipelines as columns, channels as rows, override indicators.
#[component]
fn MatrixView() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let cs = (tab_mgr.channel_state)();
    let Some(ref channel_state) = cs else {
        return rsx! {};
    };

    // Build the matrix: channel_id -> set of pipeline_stems
    let mut channel_overrides: HashMap<String, Vec<String>> = HashMap::new();
    let mut all_pipelines: Vec<String> = Vec::new();

    for channel in &channel_state.channels {
        let channel_dir = channel_state
            .workspace_root
            .join(&channel_state.channels_dir)
            .join(&channel.id);
        let stems: Vec<String> = discover_override_files(&channel_dir)
            .into_iter()
            .map(|o| o.pipeline_stem)
            .collect();
        for stem in &stems {
            if !all_pipelines.contains(stem) {
                all_pipelines.push(stem.clone());
            }
        }
        channel_overrides.insert(channel.id.clone(), stems);
    }
    all_pipelines.sort();

    let total_overrides: usize = channel_overrides.values().map(|v| v.len()).sum();

    rsx! {
        div { class: "kiln-overrides-matrix",
            // Summary
            div { class: "kiln-overrides-matrix__summary",
                "{channel_state.channels.len()} channels \u{00b7} {all_pipelines.len()} pipelines \u{00b7} {total_overrides} overrides"
            }

            if all_pipelines.is_empty() {
                div { class: "kiln-overrides__empty",
                    "No channel overrides found."
                }
            } else {
                // Matrix table
                div { class: "kiln-overrides-matrix__table",
                    // Header row
                    div { class: "kiln-overrides-matrix__row kiln-overrides-matrix__row--header",
                        div { class: "kiln-overrides-matrix__cell kiln-overrides-matrix__cell--corner" }
                        for pipeline in &all_pipelines {
                            div {
                                key: "h-{pipeline}",
                                class: "kiln-overrides-matrix__cell kiln-overrides-matrix__cell--header",
                                title: "{pipeline}.yaml",
                                "{pipeline}"
                            }
                        }
                    }

                    // Data rows
                    for channel in &channel_state.channels {
                        {
                            let overrides = channel_overrides
                                .get(&channel.id)
                                .cloned()
                                .unwrap_or_default();
                            rsx! {
                                div {
                                    key: "r-{channel.id}",
                                    class: "kiln-overrides-matrix__row",

                                    div { class: "kiln-overrides-matrix__cell kiln-overrides-matrix__cell--label",
                                        "{channel.id}"
                                    }

                                    for pipeline in &all_pipelines {
                                        {
                                            let has_override = overrides.contains(pipeline);
                                            rsx! {
                                                div {
                                                    key: "c-{channel.id}-{pipeline}",
                                                    class: if has_override {
                                                        "kiln-overrides-matrix__cell kiln-overrides-matrix__cell--active"
                                                    } else {
                                                        "kiln-overrides-matrix__cell kiln-overrides-matrix__cell--empty"
                                                    },
                                                    if has_override { "\u{27f2}" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Legend
                div { class: "kiln-overrides-matrix__legend",
                    "\u{27f2} overridden \u{00a0}\u{00a0} (empty) base only"
                }
            }
        }
    }
}

/// Discover `.channel.yaml` override files in a channel/group directory.
fn discover_override_files(dir: &std::path::Path) -> Vec<OverrideFile> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.ends_with(".channel.yaml")
                && name != "channel.yaml"
            {
                let stem = name
                    .strip_suffix(".channel.yaml")
                    .unwrap_or(name)
                    .to_string();
                files.push(OverrideFile {
                    pipeline_stem: stem,
                    path,
                });
            }
        }
    }
    files.sort_by(|a, b| a.pipeline_stem.cmp(&b.pipeline_stem));
    files
}
