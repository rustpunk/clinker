use dioxus::prelude::*;

use crate::state::use_app_state;

use super::cxl_input::CxlInput;
use super::drawer_bar::{ActiveDrawer, DrawerToggleBar};
use super::drawer_docs::DrawerDocs;
use super::drawer_notes::DrawerNotes;
use super::drawer_run::DrawerRun;
use super::override_diff::OverrideDiff;
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

    // Determine stage kind and extract data
    let input = config.inputs.iter().find(|i| i.name == stage_id);
    let transform = config.transformations.iter().find(|t| t.name == stage_id);
    let output = config.outputs.iter().find(|o| o.name == stage_id);

    let (kind_label, accent, subtitle) = if input.is_some() {
        ("SOURCE", "#43B3AE", input.unwrap().path.clone())
    } else if let Some(t) = transform {
        ("TRANSFORM", "#C75B2A", t.description.clone().unwrap_or_default())
    } else if output.is_some() {
        ("OUTPUT", "#B7410E", output.unwrap().path.clone())
    } else {
        return rsx! {};
    };

    let cxl_source = transform.map(|t| t.cxl.clone());
    let drawer_open = (active_drawer)() != ActiveDrawer::None;

    // Channel override info for this stage
    let channel_resolution = (state.channel_pipeline)();
    let override_info = channel_resolution.as_ref().and_then(|cr| {
        cr.overrides_applied
            .iter()
            .find(|o| o.target_name == stage_id)
            .cloned()
    });

    // Get base CXL for diff comparison
    let base_cxl = cxl_source.clone().unwrap_or_default();

    // Get resolved CXL from channel resolution
    let resolved_cxl = channel_resolution
        .as_ref()
        .and_then(|cr| {
            cr.resolved_config
                .transforms()
                .find(|t| t.name == stage_id)
                .map(|t| t.cxl.clone())
        })
        .unwrap_or_default();

    rsx! {
        div {
            class: "kiln-inspector",
            onmousedown: move |e: MouseEvent| e.stop_propagation(),

            // ── Stage header ──────────────────────────────────────────────
            StageHeader {
                stage_id: stage_id.clone(),
                kind_label,
                accent,
                label: stage_id.clone(),
            }

            // ── Override diff (when channel override applies to this stage) ──
            if let Some(applied) = override_info {
                OverrideDiff {
                    applied: applied.clone(),
                    base_cxl: base_cxl.clone(),
                    resolved_cxl: resolved_cxl.clone(),
                }
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
                                if input.is_some() || output.is_some() { "PATH" } else { "DESCRIPTION" }
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
                    accent,
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
