/// Root application shell: owns global state + tab manager.
///
/// The architecture splits into two components:
///
/// - `AppShell` — top-level owner of global signals (layout, run log)
///   and tab management (open tabs, active tab, recent files). Provides
///   `AppState` context built from the active tab's signals so that
///   TitleBar, RunLogDrawer, and other shell-level components can read it.
///
/// - `ActiveTabContent` — keyed on the active tab's ID, so Dioxus
///   remounts it on tab switch. Contains the YAML↔model sync effects.
///   Renders canvas, inspector, YAML sidebar, and schematics.

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
use crate::demo::DEFAULT_YAML;
use crate::keyboard::handle_keyboard;
use crate::recent_files::load_recent_files;
use crate::state::{AppState, LayoutPreset, TabManagerState};
use crate::sync::{parse_yaml, serialize_yaml, EditSource};
use crate::tab::{TabEntry, TabId};

const KILN_CSS: Asset = asset!("/assets/kiln.css");

/// Root application component.
///
/// Owns global signals and the tab collection. Provides both `TabManagerState`
/// and `AppState` contexts — `AppState` is built from the active tab's signals
/// so shell-level components (TitleBar, RunLogDrawer) always have access.
#[component]
pub fn AppShell() -> Element {
    // ── Global signals (shared across all tabs) ──────────────────────────
    let layout = use_signal(|| LayoutPreset::Hybrid);
    let run_log_expanded = use_signal(|| false);
    let inspector_width = use_signal(|| 340.0_f32);

    // ── Tab management ───────────────────────────────────────────────────
    let tabs = use_signal(|| {
        vec![TabEntry::new_demo(DEFAULT_YAML)]
    });
    let active_tab_id = use_signal(|| {
        Some(tabs.peek().first().map(|t| t.id).unwrap_or_else(TabId::new))
    });
    let recent_files = use_signal(load_recent_files);
    let workspace = use_signal(|| None);

    // ── Toast notifications ──────────────────────────────────────────────
    let toast_message: Signal<Option<ToastState>> = use_signal(|| None);

    // ── Confirmation dialog state ────────────────────────────────────────
    let pending_confirm: Signal<Option<PendingConfirm>> = use_signal(|| None);

    // ── Fallback per-tab signals (used when no tab is active) ────────────
    let fallback_yaml = use_signal(String::new);
    let fallback_pipeline = use_signal(|| None);
    let fallback_errors = use_signal(Vec::new);
    let fallback_edit = use_signal(|| EditSource::None);
    let fallback_selected = use_signal(|| None::<String>);

    // ── Find the active tab's signals ────────────────────────────────────
    let active_id = (active_tab_id)();
    let active_tab_index = active_id.and_then(|id| {
        tabs.read().iter().position(|t| t.id == id)
    });

    // Build AppState from active tab or fallbacks
    let (yaml_text, pipeline, parse_errors, edit_source, selected_stage) =
        if let Some(idx) = active_tab_index {
            let t = &tabs.read()[idx];
            (t.yaml_text, t.pipeline, t.parse_errors, t.edit_source, t.selected_stage)
        } else {
            (fallback_yaml, fallback_pipeline, fallback_errors, fallback_edit, fallback_selected)
        };

    // ── Provide global contexts ──────────────────────────────────────────
    use_context_provider(|| TabManagerState {
        tabs,
        active_tab_id,
        recent_files,
        workspace,
    });

    use_context_provider(move || AppState {
        layout,
        run_log_expanded,
        selected_stage,
        inspector_width,
        yaml_text,
        pipeline,
        parse_errors,
        edit_source,
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
    // These run at AppShell level using the active tab's signals.
    // When the active tab changes, the signals change, triggering re-sync.
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

            // Confirmation dialog (rendered above everything when pending)
            if let Some(pending) = (pending_confirm)() {
                ConfirmDialog {
                    pending: pending.clone(),
                    on_action: move |action: ConfirmAction| {
                        let mut confirm = pending_confirm;
                        let tab_id = pending.tab_id;
                        match action {
                            ConfirmAction::Save => {
                                // Save first, then close
                                crate::keyboard::save_and_close_tab(
                                    &mut kb_tab_mgr, tab_id,
                                );
                                confirm.set(None);
                            }
                            ConfirmAction::Discard => {
                                // Close without saving
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
///
/// Reads `AppState` from context (provided by AppShell with the active tab's
/// signals). Renders canvas, inspector, YAML sidebar, and schematics.
#[component]
fn ActiveTabContent() -> Element {
    let state = use_context::<AppState>();
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
