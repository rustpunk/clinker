use dioxus::prelude::*;

use crate::state::use_app_state;

use super::cxl_input::CxlInput;
use super::drawer_bar::{ActiveDrawer, DrawerToggleBar};
use super::drawer_docs::DrawerDocs;
use super::drawer_notes::DrawerNotes;
use super::drawer_run::DrawerRun;
use super::scoped_yaml::ScopedYaml;
use super::stage_header::StageHeader;

/// Four-concern inspector panel: Config (always visible) + Run/Docs/Notes drawer.
///
/// Keyed on `stage_id` in the parent so selection changes cause a full remount
/// with fresh signals (drawer state resets on selection change).
///
/// Spec §A2.1: Panel structure (Config + toggle bar + drawer).
#[component]
pub fn InspectorPanel(stage_id: String) -> Element {
    let state = use_app_state();
    let mut active_drawer = use_signal(|| ActiveDrawer::None);

    let pipeline_guard = (state.pipeline).read();
    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! {};
    };

    // Phase 16b Task 16b.5: dispatch inspector content on the
    // `PipelineNode` variant tag. Every variant is handled explicitly so
    // adding a new one is a compile break here.
    use clinker_core::config::PipelineNode;
    let Some(node_spanned) = config.nodes.iter().find(|n| n.value.name() == stage_id) else {
        return rsx! {};
    };
    let (kind_label, kind_attr, subtitle, cxl_source) = match &node_spanned.value {
        PipelineNode::Source { config: body, .. } => {
            ("SOURCE", "source", body.source.path.clone(), None)
        }
        PipelineNode::Transform { config: body, .. } => (
            "TRANSFORM",
            "transform",
            String::new(),
            Some(body.cxl.as_ref().to_string()),
        ),
        PipelineNode::Aggregate { config: body, .. } => {
            let subtitle = if body.group_by.is_empty() {
                String::new()
            } else {
                format!("group_by: {}", body.group_by.join(", "))
            };
            (
                "AGGREGATE",
                "aggregate",
                subtitle,
                Some(body.cxl.as_ref().to_string()),
            )
        }
        PipelineNode::Route { config: body, .. } => {
            let subtitle = format!(
                "{} branch{} → {}",
                body.conditions.len(),
                if body.conditions.len() == 1 { "" } else { "es" },
                body.default
            );
            ("ROUTE", "route", subtitle, None)
        }
        PipelineNode::Merge { header, .. } => (
            "MERGE",
            "merge",
            format!("{} inputs", header.inputs.len()),
            None,
        ),
        PipelineNode::Output { config: body, .. } => {
            ("OUTPUT", "output", body.output.path.clone(), None)
        }
        PipelineNode::Composition { config: body, .. } => (
            "COMPOSITION",
            "composition",
            format!("use: {} (Phase 16c)", body.r#use.display()),
            None,
        ),
    };
    let is_source_or_output = matches!(
        &node_spanned.value,
        PipelineNode::Source { .. } | PipelineNode::Output { .. }
    );
    let drawer_open = (active_drawer)() != ActiveDrawer::None;

    rsx! {
        div {
            class: "kiln-inspector",
            onmousedown: move |e: MouseEvent| e.stop_propagation(),

            // ── Stage header ──────────────────────────────────────────────
            StageHeader {
                stage_id: stage_id.clone(),
                kind_label,
                kind_attr,
                label: stage_id.clone(),
            }

            // ── Config section (upper, always visible) ────────────────────
            div {
                class: "kiln-inspector-config",
                "data-compressed": if drawer_open { "true" } else { "false" },

                div {
                    class: "kiln-inspector-section",

                    div {
                        class: "kiln-section-header",
                        span { class: "kiln-diamond", "\u{25C6}" }
                        span { class: "kiln-section-title", "CONFIGURATION" }
                        span { class: "kiln-section-rule" }
                    }

                    if !subtitle.is_empty() {
                        div {
                            class: "kiln-cxl-field",
                            label { class: "kiln-cxl-label",
                                if is_source_or_output { "PATH" } else { "DESCRIPTION" }
                            }
                            div {
                                class: "kiln-inspector-value",
                                "{subtitle}"
                            }
                        }
                    }

                    if let Some(ref cxl) = cxl_source {
                        CxlInput {
                            key: "{stage_id}-cxl",
                            label: "cxl",
                            initial_value: cxl.clone(),
                        }
                    }
                }

                ScopedYaml {
                    stage_id: stage_id.clone(),
                }
            }

            // ── Drawer toggle bar (always visible) ────────────────────────
            DrawerToggleBar {
                active: (active_drawer)(),
                on_toggle: move |drawer: ActiveDrawer| {
                    active_drawer.set(drawer);
                },
            }

            // ── Drawer region (expandable) ────────────────────────────────
            div {
                class: "kiln-drawer-region",
                "data-open": if drawer_open { "true" } else { "false" },

                match (active_drawer)() {
                    ActiveDrawer::Run => rsx! { DrawerRun {} },
                    ActiveDrawer::Docs => rsx! { DrawerDocs { stage_id: stage_id.clone() } },
                    ActiveDrawer::Notes => rsx! { DrawerNotes { stage_id: stage_id.clone() } },
                    ActiveDrawer::None => rsx! {},
                }
            }
        }
    }
}
