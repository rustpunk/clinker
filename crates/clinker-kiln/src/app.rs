/// Root application shell: owns global state + tab manager.
///
/// `AppState` is provided as `Signal<AppState>` in context. When the active
/// tab changes, the signal is updated with the new tab's signal handles.
/// Downstream components call `use_app_state()` to read through the signal.

use dioxus::prelude::*;

use crate::components::{
    canvas::CanvasPanel,
    inspector::InspectorPanel,
    run_log::RunLogDrawer,
    schematics::SchematicsPanel,
    tab_bar::TabBar,
    title_bar::TitleBar,
    toast::ToastOverlay,
    welcome_screen::WelcomeScreen,
    yaml_sidebar::YamlSidebar,
};
use crate::components::confirm_dialog::{ConfirmAction, ConfirmDialog, PendingConfirm};
use crate::components::toast::ToastState;
use crate::keyboard::handle_keyboard;
use crate::recent_files::load_recent_files;
use crate::state::{AppState, LayoutPreset, TabManagerState, use_app_state};
use crate::sync::{parse_yaml, serialize_yaml, EditSource};
use crate::tab::{TabEntry, TabId};

const KILN_CSS: Asset = asset!("/assets/kiln.css");

#[component]
pub fn AppShell() -> Element {
    // ── Global signals (shared across all tabs) ──────────────────────────
    let layout = use_signal(|| LayoutPreset::Hybrid);
    let run_log_expanded = use_signal(|| false);
    let inspector_width = use_signal(|| 340.0_f32);

    // ── Tab management ───────────────────────────────────────────────────
    let tabs: Signal<Vec<TabEntry>> = use_signal(Vec::new);
    let active_tab_id: Signal<Option<TabId>> = use_signal(|| None);
    let recent_files = use_signal(load_recent_files);
    let workspace = use_signal(|| None);

    // ── Toast + confirm dialog ───────────────────────────────────────────
    let toast_message: Signal<Option<ToastState>> = use_signal(|| None);
    let pending_confirm: Signal<Option<PendingConfirm>> = use_signal(|| None);

    // ── Fallback per-tab signals (used when no tab is active) ────────────
    let fallback_yaml = use_signal(String::new);
    let fallback_pipeline = use_signal(|| None);
    let fallback_errors = use_signal(Vec::new);
    let fallback_edit = use_signal(|| EditSource::None);
    let fallback_selected = use_signal(|| None::<String>);

    // ── Build AppState from active tab ───────────────────────────────────
    // This runs on every render. The Signal<AppState> is provided as context
    // and updated here so children always see the active tab's signals.
    let active_id = (active_tab_id)();
    let active_tab_index = active_id.and_then(|id| {
        tabs.read().iter().position(|t| t.id == id)
    });

    let (yaml_text, pipeline, parse_errors, edit_source, selected_stage) =
        if let Some(idx) = active_tab_index {
            let t = &tabs.read()[idx];
            (t.yaml_text, t.pipeline, t.parse_errors, t.edit_source, t.selected_stage)
        } else {
            (fallback_yaml, fallback_pipeline, fallback_errors, fallback_edit, fallback_selected)
        };

    let current_app_state = AppState {
        layout,
        run_log_expanded,
        selected_stage,
        inspector_width,
        yaml_text,
        pipeline,
        parse_errors,
        edit_source,
    };

    // Provide AppState as a Signal so it can be updated on tab switch.
    // use_context_provider runs once; we update the signal's value every render.
    let mut app_state_signal = use_signal(|| current_app_state);

    // Update the signal with current tab's state on every render
    // (this is cheap — Signal handles are Copy)
    *app_state_signal.write() = current_app_state;

    // ── Provide contexts ─────────────────────────────────────────────────
    use_context_provider(|| app_state_signal);
    use_context_provider(|| TabManagerState {
        tabs,
        active_tab_id,
        recent_files,
        workspace,
    });
    use_context_provider(move || toast_message);
    use_context_provider(move || pending_confirm);

    // ── Keyboard handler ─────────────────────────────────────────────────
    let mut kb_tab_mgr = TabManagerState {
        tabs,
        active_tab_id,
        recent_files,
        workspace,
    };

    // ── Sync effects: YAML ↔ pipeline model ──────────────────────────────
    {
        let mut pipeline = pipeline;
        let mut parse_errors = parse_errors;

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
                    parse_errors.set(errors);
                }
            }
        });
    }

    {
        let mut yaml_text = yaml_text;
        let mut parse_errors = parse_errors;

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
    }

    rsx! {
        document::Stylesheet { href: KILN_CSS }
        document::Title { "clinker kiln" }

        div {
            class: "kiln-app",
            tabindex: "0",
            onkeydown: move |e: KeyboardEvent| {
                if handle_keyboard(&e, &mut kb_tab_mgr) {
                    e.prevent_default();
                }
            },

            TitleBar {}
            TabBar {}

            if active_tab_index.is_some() {
                ActiveTabContent {
                    key: "{active_id.map(|id| id.to_string()).unwrap_or_default()}",
                }
            } else {
                WelcomeScreen {}
            }

            RunLogDrawer {}
            ToastOverlay {}

            if let Some(pending) = (pending_confirm)() {
                ConfirmDialog {
                    pending: pending.clone(),
                    on_action: move |action: ConfirmAction| {
                        let mut confirm = pending_confirm;
                        let tab_id = pending.tab_id;
                        match action {
                            ConfirmAction::Save => {
                                crate::keyboard::save_and_close_tab(
                                    &mut kb_tab_mgr, tab_id,
                                );
                                confirm.set(None);
                            }
                            ConfirmAction::Discard => {
                                crate::keyboard::force_close_tab(
                                    &mut kb_tab_mgr, tab_id,
                                );
                                confirm.set(None);
                            }
                            ConfirmAction::Cancel => {
                                confirm.set(None);
                            }
                        }
                    },
                }
            }
        }
    }
}

/// Active tab's content area — keyed on tab ID for clean remount.
#[component]
fn ActiveTabContent() -> Element {
    let state = use_app_state();
    let layout = state.layout;
    let selected_stage = state.selected_stage;

    rsx! {
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
            SchematicsPanel {}
        }
    }
}
