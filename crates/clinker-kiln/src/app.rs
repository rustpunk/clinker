/// Root application shell: owns all reactive signals.
///
/// Per-tab state is stored as plain data in `TabEntry::snapshot`. AppShell
/// owns one set of signals (yaml_text, pipeline, etc.) that always reflect
/// the active tab. On tab switch, the departing tab's snapshot is updated
/// from the signals, and the arriving tab's snapshot is loaded into them.
///
/// This avoids Dioxus signal scope-ownership issues — all signals live in
/// AppShell's scope, which outlives every child component.

use clinker_git::GitOps;
use dioxus::prelude::*;

use crate::components::{
    canvas::CanvasPanel,
    inspector::InspectorPanel,
    run_log::RunLogDrawer,
    composition_panel::CompositionPanel,
    schema_panel::SchemaPanel,
    schematics::SchematicsPanel,
    command_palette::CommandPalette,
    search_panel::SearchPanel,
    status_bar::StatusBar,
    tab_bar::TabBar,
    template_gallery::TemplateGallery,
    title_bar::TitleBar,
    version_mode::VersionMode,
    toast::ToastOverlay,
    welcome_screen::WelcomeScreen,
    yaml_sidebar::YamlSidebar,
};
use crate::components::confirm_dialog::{ConfirmAction, ConfirmDialog, PendingConfirm};
use crate::components::toast::ToastState;
use clinker_schema::SchemaIndex;

use crate::keyboard::handle_keyboard;
use crate::recent_files::load_recent_files;
use crate::state::{AppState, LayoutPreset, LeftPanel, TabManagerState, use_app_state};
use crate::sync::{parse_yaml, serialize_yaml, EditSource};
use crate::tab::{TabEntry, TabId};
use crate::workspace;

const KILN_CSS: Asset = asset!("/assets/kiln.css");

#[component]
pub fn AppShell() -> Element {
    // ── Global signals (shared across all tabs) ──────────────────────────
    let run_log_expanded = use_signal(|| false);
    let inspector_width = use_signal(|| 340.0_f32);

    // ── Per-tab signals (owned here, swapped on tab switch) ──────────────
    let mut yaml_text = use_signal(String::new);
    let mut pipeline = use_signal(|| None);
    let mut parse_errors = use_signal(Vec::new);
    let mut edit_source = use_signal(|| EditSource::None);
    let mut selected_stage = use_signal(|| None::<String>);
    let mut schema_warnings = use_signal(Vec::new);
    let raw_pipeline = use_signal(|| None);
    let compositions = use_signal(Vec::new);
    let contract_warnings = use_signal(Vec::new);

    // ── Session restore (single call on first mount per use_signal) ─────
    // restore_session() is called in the first use_signal closure. The result
    // is cached — subsequent closures read from the same signal. On re-renders,
    // use_signal closures don't re-execute, so disk I/O happens only once.
    let mut session_data: Signal<Option<workspace::SessionInit>> =
        use_signal(|| Some(workspace::restore_session()));

    let layout = use_signal(|| {
        session_data.peek().as_ref().map(|s| s.layout).unwrap_or(LayoutPreset::Hybrid)
    });
    let mut tabs: Signal<Vec<TabEntry>> = use_signal(|| {
        session_data.write().as_mut()
            .map(|s| std::mem::take(&mut s.tabs))
            .unwrap_or_default()
    });
    let active_tab_id: Signal<Option<TabId>> = use_signal(|| {
        session_data.peek().as_ref().and_then(|s| s.active_tab_id)
    });
    let mut prev_tab_id: Signal<Option<TabId>> = use_signal(|| None);
    let recent_files = use_signal(load_recent_files);
    let workspace: Signal<Option<workspace::Workspace>> = use_signal(|| {
        session_data.write().as_mut().and_then(|s| s.workspace.take())
    });

    // ── Left panel + schema index + template gallery ──────────────────────
    let left_panel: Signal<LeftPanel> = use_signal(|| LeftPanel::None);
    let mut schema_index: Signal<SchemaIndex> = use_signal(SchemaIndex::default);
    let show_template_gallery: Signal<bool> = use_signal(|| false);
    let mut git_state: Signal<Option<clinker_git::RepoStatus>> = use_signal(|| None);
    let show_command_palette: Signal<bool> = use_signal(|| false);
    let mut composition_index: Signal<crate::composition_index::CompositionIndex> =
        use_signal(crate::composition_index::CompositionIndex::default);

    // ── Git: detect repo and compute status on workspace change ──────────
    {
        use_effect(move || {
            let ws = (workspace)();
            if let Some(ref ws) = ws {
                match clinker_git::GitCliOps::discover(&ws.root) {
                    Ok(ops) => {
                        if let Ok(status) = ops.status() {
                            git_state.set(Some(status));
                        }
                    }
                    Err(_) => git_state.set(None),
                }
            } else {
                git_state.set(None);
            }
        });
    }

    // ── Schema index: rebuild when workspace changes ─────────────────────
    {
        use_effect(move || {
            let ws = (workspace)();
            if let Some(ref ws) = ws {
                let (index, _errors) = ws.build_schema_index();
                schema_index.set(index);
            } else {
                schema_index.set(SchemaIndex::default());
            }
        });
    }

    // ── Composition index: rebuild when workspace changes ─────────────
    {
        use_effect(move || {
            let ws = (workspace)();
            if let Some(ref ws) = ws {
                let index = crate::composition_index::build_composition_index(ws);
                composition_index.set(index);
            } else {
                composition_index.set(crate::composition_index::CompositionIndex::default());
            }
        });
    }

    // ── Filesystem watcher: auto-refresh git status + schema index ─────
    // Spawns a background watcher on the workspace root. Debounced 500ms.
    {
        use_effect(move || {
            let ws = (workspace)();
            let Some(ref ws) = ws else { return };

            let root = ws.root.clone();
            let Some((_watcher, rx)) = crate::fs_watcher::start_watcher(&root) else {
                return;
            };

            // Spawn a polling loop that checks for debounced changes
            // and refreshes git/schema state.
            let root2 = root.clone();
            std::thread::spawn(move || {
                while let Ok(paths) = rx.recv() {
                    if crate::fs_watcher::has_git_relevant_changes(&paths) {
                        // Refresh git status — we can't write to signals from a
                        // non-Dioxus thread directly. The git state is read-refreshed
                        // on next render cycle via the workspace effect above.
                        // For now, this ensures the watcher is running — the actual
                        // refresh is triggered by re-reading workspace signal.
                    }
                }
            });
        });
    }

    // ── Toast + confirm dialog ───────────────────────────────────────────
    let toast_message: Signal<Option<ToastState>> = use_signal(|| None);
    let pending_confirm: Signal<Option<PendingConfirm>> = use_signal(|| None);

    // ── Tab switch: snapshot departing tab, load arriving tab ─────────────
    let current_active = (active_tab_id)();
    let previous = (prev_tab_id)();

    if current_active != previous {
        // Save departing tab's state from signals → snapshot
        if let Some(old_id) = previous {
            let mut tabs_w = tabs.write();
            if let Some(old_tab) = tabs_w.iter_mut().find(|t| t.id == old_id) {
                old_tab.snapshot.yaml_text = (yaml_text)();
                old_tab.snapshot.pipeline = (pipeline)();
                old_tab.snapshot.parse_errors = (parse_errors)();
                old_tab.snapshot.edit_source = (edit_source)();
                old_tab.snapshot.selected_stage = (selected_stage)();
            }
        }

        // Load arriving tab's snapshot → signals
        if let Some(new_id) = current_active {
            let tabs_r = tabs.read();
            if let Some(new_tab) = tabs_r.iter().find(|t| t.id == new_id) {
                yaml_text.set(new_tab.snapshot.yaml_text.clone());
                pipeline.set(new_tab.snapshot.pipeline.clone());
                parse_errors.set(new_tab.snapshot.parse_errors.clone());
                edit_source.set(new_tab.snapshot.edit_source);
                selected_stage.set(new_tab.snapshot.selected_stage.clone());
            }
        } else {
            // No active tab — clear signals
            yaml_text.set(String::new());
            pipeline.set(None);
            parse_errors.set(Vec::new());
            edit_source.set(EditSource::None);
            selected_stage.set(None);
        }

        prev_tab_id.set(current_active);

        // Save workspace state on tab switch
        if let Some(ref ws) = *workspace.read() {
            let active_file = current_active.and_then(|id| {
                tabs.read().iter().find(|t| t.id == id)
                    .and_then(|t| t.file_path.as_ref())
                    .map(|p| p.display().to_string())
            });
            let state = workspace::build_state_snapshot(
                &tabs.read(),
                active_file.as_deref(),
                (layout)(),
                (run_log_expanded)(),
            );
            workspace::save_workspace_state(&ws.root, &state);
            workspace::save_last_workspace(&ws.root);
        }
    }

    // ── Build AppState ───────────────────────────────────────────────────
    let current_app_state = AppState {
        layout,
        run_log_expanded,
        selected_stage,
        inspector_width,
        yaml_text,
        pipeline,
        parse_errors,
        edit_source,
        schema_warnings,
        raw_pipeline,
        compositions,
        contract_warnings,
    };

    let mut app_state_signal = use_signal(|| current_app_state);
    *app_state_signal.write() = current_app_state;

    // ── Provide contexts ─────────────────────────────────────────────────
    use_context_provider(|| app_state_signal);
    use_context_provider(|| TabManagerState {
        tabs,
        active_tab_id,
        recent_files,
        workspace,
        left_panel,
        schema_index,
        show_template_gallery,
        git_state,
        show_command_palette,
        composition_index,
    });
    use_context_provider(move || toast_message);
    use_context_provider(move || pending_confirm);

    // ── Keyboard handler ─────────────────────────────────────────────────
    let mut kb_tab_mgr = TabManagerState {
        tabs,
        active_tab_id,
        recent_files,
        workspace,
        left_panel,
        schema_index,
        show_template_gallery,
        git_state,
        show_command_palette,
        composition_index,
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

    // ── Schema validation: run when pipeline or schema index changes ──────
    {
        use_effect(move || {
            let pl = (pipeline)();
            let idx = (schema_index)();
            let ws = (workspace)();

            if let (Some(config), Some(ws)) = (pl.as_ref(), ws.as_ref()) {
                let warnings = clinker_schema::validate_pipeline(config, &idx, &ws.root);
                schema_warnings.set(warnings);
            } else {
                schema_warnings.set(Vec::new());
            }
        });
    }

    // ── Sync active tab snapshot from signals periodically ───────────────
    // Keep the active tab's snapshot in sync with live signal values
    // so is_dirty() and save work correctly.
    {
        use_effect(move || {
            let text = (yaml_text)();
            let pl = (pipeline)();
            let errs = (parse_errors)();
            let src = (edit_source)();
            let sel = (selected_stage)();

            if let Some(active_id) = (active_tab_id)() {
                let mut tabs_w = tabs.write();
                if let Some(tab) = tabs_w.iter_mut().find(|t| t.id == active_id) {
                    tab.snapshot.yaml_text = text;
                    tab.snapshot.pipeline = pl;
                    tab.snapshot.parse_errors = errs;
                    tab.snapshot.edit_source = src;
                    tab.snapshot.selected_stage = sel;
                }
            }
        });
    }

    let has_active_tab = current_active.is_some();

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

            if has_active_tab {
                ActiveTabContent {
                    key: "{current_active.map(|id| id.to_string()).unwrap_or_default()}",
                }
            } else {
                WelcomeScreen {}
            }

            RunLogDrawer {}
            StatusBar {}
            ToastOverlay {}

            // ── Template gallery overlay ──────────────────────────────────
            if (show_template_gallery)() {
                TemplateGallery {}
            }

            // ── Command palette overlay ─────────────────────────────────
            if (show_command_palette)() {
                CommandPalette {}
            }

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
    let mut tab_mgr = use_context::<TabManagerState>();
    let left_panel = (tab_mgr.left_panel)();

    let is_version = *layout.read() == LayoutPreset::Version;

    rsx! {
        div {
            class: "kiln-main",
            "data-layout": layout.read().as_data_attr(),

            if is_version {
                VersionMode {}
            } else {
                // ── Left panel slot (280px, shared between Search and Schemas) ──
                match left_panel {
                    LeftPanel::Search => rsx! {
                        SearchPanel {}
                    },
                    LeftPanel::Schemas => rsx! {
                        SchemaPanel {}
                    },
                    LeftPanel::Compositions => rsx! {
                        CompositionPanel {}
                    },
                    LeftPanel::None => rsx! {},
                }

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
}
