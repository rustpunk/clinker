use dioxus::prelude::*;

use crate::components::{
    canvas::CanvasPanel,
    inspector::InspectorPanel,
    run_log::RunLogDrawer,
    title_bar::TitleBar,
    yaml_sidebar::YamlSidebar,
};
use crate::state::{AppState, LayoutPreset};

const KILN_CSS: Asset = asset!("/assets/kiln.css");

/// Root application component.
///
/// Owns all top-level reactive signals. Creates them as hooks (unconditionally,
/// at top level — never inside conditionals), bundles them into `AppState`, and
/// provides the bundle as context so descendants can consume individual signals
/// without triggering unnecessary sibling re-renders.
///
/// Layout (column flex):
///   ┌─ TitleBar ─────────────────────────────────────────────────────────┐
///   │  CanvasPanel  │  InspectorPanel?  │  YamlSidebar                   │
///   │  (flex-main)  │  (if selected)    │  (360 px fixed)                │
///   ├─ RunLogDrawer ─────────────────────────────────────────────────────┤
///   └────────────────────────────────────────────────────────────────────┘
///
/// The `data-layout` attribute on `.kiln-main` drives CSS widths for the three
/// layout presets (canvas-focus / hybrid / editor-focus).
#[component]
pub fn App() -> Element {
    // ── Hooks: all unconditional, at top level (AP-3 compliance) ──────────
    let layout = use_signal(|| LayoutPreset::Hybrid);
    let run_log_expanded = use_signal(|| false);
    let selected_stage = use_signal(|| None::<&'static str>);
    let inspector_width = use_signal(|| 340.0_f32);

    // ── Provide context: struct-of-signals, not Signal<struct> (AP-4) ─────
    use_context_provider(|| AppState {
        layout,
        run_log_expanded,
        selected_stage,
        inspector_width,
    });

    rsx! {
        // Inject the rustpunk stylesheet via the document head.
        document::Stylesheet { href: KILN_CSS }
        document::Title { "clinker kiln" }

        div {
            class: "kiln-app",

            // ── Custom title bar ──────────────────────────────────────────
            TitleBar {}

            // ── Main panel area ───────────────────────────────────────────
            div {
                class: "kiln-main",
                // Direct signal read in RSX attribute — correct subscription
                // pattern (avoids the memo-in-attribute reactivity gotcha).
                "data-layout": layout.read().as_data_attr(),

                CanvasPanel {}

                // Inspector panel — keyed on stage_id so that selection
                // changes cause a full remount with fresh local signals.
                if let Some(stage_id) = (selected_stage)() {
                    InspectorPanel {
                        key: "{stage_id}",
                        stage_id,
                    }
                }

                YamlSidebar {}
            }

            // ── Run-log drawer ────────────────────────────────────────────
            RunLogDrawer {}
        }
    }
}
