use dioxus::prelude::*;

use crate::demo::demo_pipeline;

use super::cxl_input::CxlInput;
use super::scoped_yaml::ScopedYaml;
use super::stage_header::StageHeader;

/// Slide-in inspector panel showing the selected stage's configuration.
///
/// **Keyed on `stage_id`** in the parent (`app.rs`) so that selection changes
/// cause a full remount with fresh local signals. This avoids stale-state bugs
/// when switching between stages.
///
/// Layout (vertical):
///   ┌─ border-top (3px, accent)
///   │ ◆ FILTER  active_only  [×]
///   ├─ CONFIGURATION ────────────
///   │ EXPR [status == "active"  ]
///   │      ✗ parse error...
///   ├─ STAGE YAML ───────────────
///   │   - step: filter
///   │     expr: '...'
///   └────────────────────────────
///
/// Doc: spec §5 — Node Inspector.
#[component]
pub fn InspectorPanel(stage_id: &'static str) -> Element {
    let stages = demo_pipeline();
    let Some(stage) = stages.into_iter().find(|s| s.id == stage_id) else {
        return rsx! {};
    };

    rsx! {
        div {
            class: "kiln-inspector",
            // Prevent mousedown on inspector from triggering canvas pan
            onmousedown: move |e: MouseEvent| e.stop_propagation(),

            // ── Stage header ──────────────────────────────────────────────
            StageHeader { stage: stage.clone() }

            // ── Configuration section ─────────────────────────────────────
            if !stage.expr_fields.is_empty() {
                div {
                    class: "kiln-inspector-section",

                    div {
                        class: "kiln-section-header",
                        span { class: "kiln-diamond", "\u{25C6}" }
                        span { class: "kiln-section-title", "CONFIGURATION" }
                        span { class: "kiln-section-rule" }
                    }

                    for field in stage.expr_fields.iter() {
                        CxlInput {
                            key: "{stage_id}-{field.label}",
                            label: field.label,
                            initial_value: field.expr,
                        }
                    }
                }
            }

            // ── Scoped YAML ───────────────────────────────────────────────
            ScopedYaml { stage: stage.clone() }
        }
    }
}
