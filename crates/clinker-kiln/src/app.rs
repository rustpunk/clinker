use dioxus::prelude::*;

use crate::components::{
    canvas::CanvasPanel,
    inspector::InspectorPanel,
    run_log::RunLogDrawer,
    title_bar::TitleBar,
    yaml_sidebar::YamlSidebar,
};
use crate::demo::DEFAULT_YAML;
use crate::state::{AppState, LayoutPreset};
use crate::sync::{parse_yaml, serialize_yaml, EditSource};

const KILN_CSS: Asset = asset!("/assets/kiln.css");

/// Root application component.
///
/// Owns all top-level reactive signals including the pipeline model.
/// The bidirectional sync engine runs via `use_effect`: when YAML text
/// changes (from the editor), it parses to PipelineConfig; when the
/// pipeline changes (from the inspector), it serializes to YAML text.
#[component]
pub fn App() -> Element {
    // ── Hooks: all unconditional, at top level (AP-3 compliance) ──────────
    let layout = use_signal(|| LayoutPreset::Hybrid);
    let run_log_expanded = use_signal(|| false);
    let selected_stage = use_signal(|| None::<String>);
    let inspector_width = use_signal(|| 340.0_f32);

    // ── Phase 2b: Pipeline model signals ──────────────────────────────────
    let mut yaml_text = use_signal(|| DEFAULT_YAML.to_string());
    let mut pipeline = use_signal(|| parse_yaml(DEFAULT_YAML).ok());
    let mut parse_errors = use_signal(|| Vec::<String>::new());
    let edit_source = use_signal(|| EditSource::None);

    // ── Provide context ───────────────────────────────────────────────────
    use_context_provider(|| AppState {
        layout,
        run_log_expanded,
        selected_stage,
        inspector_width,
        yaml_text,
        pipeline,
        parse_errors,
        edit_source,
    });

    // ── Sync: YAML text → pipeline model (when YAML editor is the source) ─
    use_effect(move || {
        let source = (edit_source)();
        let text = (yaml_text)();

        if source != EditSource::Yaml {
            return;
        }

        match parse_yaml(&text) {
            Ok(config) => {
                pipeline.set(Some(config));
                parse_errors.set(Vec::new());
            }
            Err(errors) => {
                // Keep last valid pipeline config; just show errors
                parse_errors.set(errors);
            }
        }
    });

    // ── Sync: pipeline model → YAML text (when inspector/notes is the source) ─
    use_effect(move || {
        let source = (edit_source)();
        let pipeline_val = (pipeline)();

        if source != EditSource::Inspector {
            return;
        }

        if let Some(ref config) = pipeline_val {
            let yaml = serialize_yaml(config);
            yaml_text.set(yaml);
            parse_errors.set(Vec::new());
        }
    });

    rsx! {
        document::Stylesheet { href: KILN_CSS }
        document::Title { "clinker kiln" }

        div {
            class: "kiln-app",

            TitleBar {}

            div {
                class: "kiln-main",
                "data-layout": layout.read().as_data_attr(),

                CanvasPanel {}

                if let Some(ref stage_id) = (selected_stage)() {
                    InspectorPanel {
                        key: "{stage_id}",
                        stage_id: stage_id.clone(),
                    }
                }

                YamlSidebar {}
            }

            RunLogDrawer {}
        }
    }
}
