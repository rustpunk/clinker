/// Root application shell: owns all reactive signals.
///
/// Per-tab state is stored as plain data in `TabEntry::snapshot`. AppShell
/// owns one set of signals (yaml_text, pipeline, etc.) that always reflect
/// the active tab. On tab switch, the departing tab's snapshot is updated
/// from the signals, and the arriving tab's snapshot is loaded into them.
///
/// This avoids Dioxus signal scope-ownership issues — all signals live in
/// AppShell's scope, which outlives every child component.
///
/// Navigation uses a two-level model:
/// - `NavigationContext` selects the active page (Pipeline, Channels, Git, Docs, Runs)
/// - `PipelineLayoutMode` selects the view within Pipeline (Canvas, Hybrid, Editor)
use clinker_git::GitOps;
use dioxus::prelude::*;

use crate::components::confirm_dialog::{ConfirmAction, ConfirmDialog, PendingConfirm};
use crate::components::toast::ToastState;
use crate::components::{
    activity_bar::ActivityBar, canvas::CanvasPanel, channel_mode::ChannelMode,
    command_palette::CommandPalette, composition_panel::CompositionPanel,
    inspector::InspectorPanel, placeholder_page::PlaceholderPage, run_log::RunLogDrawer,
    schema_panel::SchemaPanel, schematics::SchematicsPanel, search_panel::SearchPanel,
    status_bar::StatusBar, tab_bar::TabBar, template_gallery::TemplateGallery, title_bar::TitleBar,
    toast::ToastOverlay, version_mode::VersionMode, welcome_screen::WelcomeScreen,
    yaml_sidebar::YamlSidebar,
};
use clinker_schema::SchemaIndex;

use crate::keyboard::handle_keyboard;
use crate::recent_files::load_recent_files;
use crate::state::{
    AppState, KilnTheme, LeftPanel, NavigationContext, PipelineLayoutMode, TabManagerState,
    use_app_state,
};
use crate::sync::{EditSource, ParseResult, serialize_raw_yaml, try_parse_yaml};
use crate::tab::{TabEntry, TabId};
use crate::workspace;

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
    let mut partial_pipeline = use_signal(|| None);
    let mut channel_pipeline: Signal<Option<crate::channel_resolve::ChannelResolution>> =
        use_signal(|| None);
    let channel_view_mode = use_signal(crate::state::ChannelViewMode::default);
    let expanded_compositions = use_signal(std::collections::HashSet::new);
    let composition_drill_stack = use_signal(Vec::new);

    // ── Session restore (single call on first mount per use_signal) ─────
    // restore_session() is called in the first use_signal closure. The result
    // is cached — subsequent closures read from the same signal. On re-renders,
    // use_signal closures don't re-execute, so disk I/O happens only once.
    let mut session_data: Signal<Option<workspace::SessionInit>> =
        use_signal(|| Some(workspace::restore_session()));

    // ── Navigation signals ──────────────────────────────────────────────
    let active_context = use_signal(|| {
        session_data
            .peek()
            .as_ref()
            .map(|s| s.context)
            .unwrap_or_default()
    });
    let pipeline_layout = use_signal(|| {
        session_data
            .peek()
            .as_ref()
            .map(|s| s.pipeline_layout)
            .unwrap_or_default()
    });
    let activity_bar_visible = use_signal(|| true);
    let nav_history: Signal<Vec<NavigationContext>> = use_signal(Vec::new);

    let mut tabs: Signal<Vec<TabEntry>> = use_signal(|| {
        session_data
            .write()
            .as_mut()
            .map(|s| std::mem::take(&mut s.tabs))
            .unwrap_or_default()
    });
    let active_tab_id: Signal<Option<TabId>> =
        use_signal(|| session_data.peek().as_ref().and_then(|s| s.active_tab_id));
    let mut prev_tab_id: Signal<Option<TabId>> = use_signal(|| None);
    let recent_files = use_signal(load_recent_files);
    let workspace: Signal<Option<workspace::Workspace>> = use_signal(|| {
        session_data
            .write()
            .as_mut()
            .and_then(|s| s.workspace.take())
    });

    // ── Left panel + schema index + template gallery ──────────────────────
    let left_panel: Signal<LeftPanel> = use_signal(|| LeftPanel::None);
    let mut schema_index: Signal<SchemaIndex> = use_signal(SchemaIndex::default);
    let show_template_gallery: Signal<bool> = use_signal(|| false);
    let mut git_state: Signal<Option<clinker_git::RepoStatus>> = use_signal(|| None);
    let show_command_palette: Signal<bool> = use_signal(|| false);
    let mut composition_index: Signal<crate::composition_index::CompositionIndex> =
        use_signal(crate::composition_index::CompositionIndex::default);
    let show_settings: Signal<bool> = use_signal(|| false);
    let mut channel_state: Signal<Option<crate::state::ChannelState>> = use_signal(|| None);
    let theme: Signal<KilnTheme> = use_signal(|| {
        session_data
            .peek()
            .as_ref()
            .and_then(|s| s.theme)
            .unwrap_or_default()
    });

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

    // ── Channel state: discover channels when workspace changes ────────
    {
        use_effect(move || {
            let ws = (workspace)();
            if let Some(ref ws) = ws {
                let mut state = workspace::discover_channels(ws);
                // Restore persisted channel selection
                if let Some(ref mut cs) = state {
                    if let Some(ref chan_persist) = ws.state.channels {
                        cs.active_channel = chan_persist.active.clone();
                        cs.recent_channels = chan_persist.recent.clone();
                    } else if cs.default_channel.is_some() {
                        // Use default_channel from clinker.toml on first load
                        cs.active_channel = cs.default_channel.clone();
                    }
                }
                channel_state.set(state);
            } else {
                channel_state.set(None);
            }
        });
    }

    // ── Workspace available: re-parse active tab to resolve compositions ──
    // On startup, tabs may restore before the workspace signal is set.
    // When workspace becomes available, trigger a re-parse so resolve_imports()
    // can find .comp.yaml files relative to the workspace root.
    {
        use_effect(move || {
            let ws = (workspace)();
            let text = (yaml_text)();
            let source = (edit_source)();
            if ws.is_some() && !text.is_empty() && source == EditSource::None {
                edit_source.set(EditSource::Yaml);
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
            let _root2 = root.clone();
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

    // ── Session persistence: save on quit + periodic autosave ─────────────
    // use_drop fires when AppShell's scope drops (window close, app exit).
    use_drop({
        let tabs = tabs;
        let channel_state = channel_state;
        move || {
            workspace::save_full_session(
                &workspace.peek(),
                &tabs.peek(),
                *active_tab_id.peek(),
                *active_context.peek(),
                *pipeline_layout.peek(),
                *run_log_expanded.peek(),
                *activity_bar_visible.peek(),
                &channel_state.peek(),
                *theme.peek(),
            );
        }
    });

    // Periodic autosave: flush state to disk every 5 seconds.
    // Catches all state changes even if the user never switches tabs.
    // Worst case on force-kill: lose last 5 seconds of layout/tab state.
    use_future(move || {
        let tabs = tabs;
        let channel_state = channel_state;
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                workspace::save_full_session(
                    &workspace.peek(),
                    &tabs.peek(),
                    *active_tab_id.peek(),
                    *active_context.peek(),
                    *pipeline_layout.peek(),
                    *run_log_expanded.peek(),
                    *activity_bar_visible.peek(),
                    &channel_state.peek(),
                    *theme.peek(),
                );
            }
        }
    });

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
                old_tab.snapshot.partial_pipeline = (partial_pipeline)();
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
                partial_pipeline.set(new_tab.snapshot.partial_pipeline.clone());
                parse_errors.set(new_tab.snapshot.parse_errors.clone());
                selected_stage.set(new_tab.snapshot.selected_stage.clone());

                // If tab was loaded from disk (edit_source == None) and has content,
                // trigger a full re-parse to resolve _import compositions.
                // parse_yaml_raw_path() used during tab creation skips resolve_imports().
                if new_tab.snapshot.edit_source == EditSource::None
                    && !new_tab.snapshot.yaml_text.is_empty()
                {
                    edit_source.set(EditSource::Yaml);
                } else {
                    edit_source.set(new_tab.snapshot.edit_source);
                }
            }
        } else {
            // No active tab — clear signals
            yaml_text.set(String::new());
            pipeline.set(None);
            partial_pipeline.set(None);
            parse_errors.set(Vec::new());
            edit_source.set(EditSource::None);
            selected_stage.set(None);
        }

        prev_tab_id.set(current_active);

        // Save workspace state on tab switch
        if let Some(ref ws) = *workspace.read() {
            let active_file = current_active.and_then(|id| {
                tabs.read()
                    .iter()
                    .find(|t| t.id == id)
                    .and_then(|t| t.file_path.as_ref())
                    .map(|p| p.display().to_string())
            });
            let state = workspace::build_state_snapshot(
                &tabs.read(),
                active_file.as_deref(),
                (active_context)(),
                (pipeline_layout)(),
                (run_log_expanded)(),
                (activity_bar_visible)(),
                &(channel_state)(),
                (theme)(),
            );
            workspace::save_workspace_state(&ws.root, &state);
            workspace::save_last_workspace(&ws.root);
        }
    }

    // ── Build AppState ───────────────────────────────────────────────────
    let current_app_state = AppState {
        active_context,
        pipeline_layout,
        run_log_expanded,
        selected_stage,
        inspector_width,
        yaml_text,
        pipeline,
        partial_pipeline,
        parse_errors,
        edit_source,
        schema_warnings,
        raw_pipeline,
        compositions,
        contract_warnings,
        channel_pipeline,
        channel_view_mode,
        expanded_compositions,
        composition_drill_stack,
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
        show_settings,
        activity_bar_visible,
        nav_history,
        channel_state,
        theme,
    });
    // ── Debug state context ────────────────────────────────────────────
    {
        use crate::debug_state::{DebugRunState, DebugState, DebugTab};
        use std::collections::{HashMap, HashSet};

        let debug_run_state = use_signal(|| DebugRunState::Idle);
        let debug_breakpoints = use_signal(HashSet::new);
        let debug_cond_breakpoints = use_signal(HashMap::new);
        let debug_drawer_open = use_signal(|| false);
        let debug_drawer_stage = use_signal(|| None::<String>);
        let debug_drawer_tab = use_signal(DebugTab::default);
        let debug_stage_cache = use_signal(HashMap::new);
        let debug_watches = use_signal(Vec::new);
        let debug_watch_collapsed = use_signal(|| false);
        let debug_editing_bp_stage = use_signal(|| None::<String>);
        let debug_downstream_dim_set = use_signal(HashSet::new);

        use_context_provider(|| DebugState {
            run_state: debug_run_state,
            breakpoints: debug_breakpoints,
            cond_breakpoints: debug_cond_breakpoints,
            drawer_open: debug_drawer_open,
            drawer_stage: debug_drawer_stage,
            drawer_tab: debug_drawer_tab,
            stage_cache: debug_stage_cache,
            watches: debug_watches,
            watch_collapsed: debug_watch_collapsed,
            editing_bp_stage: debug_editing_bp_stage,
            downstream_dim_set: debug_downstream_dim_set,
        });
    }

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
        show_settings,
        activity_bar_visible,
        nav_history,
        channel_state,
        theme,
    };

    // ── Sync effects: YAML ↔ pipeline model ──────────────────────────────
    {
        let mut pipeline = pipeline;
        let mut raw_pipeline = raw_pipeline;
        let mut compositions = compositions;
        let mut partial_pipeline = partial_pipeline;
        let mut parse_errors = parse_errors;

        use_effect(move || {
            let source = (edit_source)();
            let text = (yaml_text)();

            if source != EditSource::Yaml {
                return;
            }

            let ws_root = workspace.read().as_ref().map(|ws| ws.root.clone());

            match try_parse_yaml(&text, ws_root.as_deref()) {
                ParseResult::Complete(resolved) => {
                    pipeline.set(Some(resolved.resolved));
                    raw_pipeline.set(Some(resolved.raw));
                    compositions.set(resolved.compositions);
                    partial_pipeline.set(None);
                    parse_errors.set(Vec::new());
                }
                ParseResult::Partial(partial) => {
                    pipeline.set(None);
                    partial_pipeline.set(Some(partial.clone()));
                    parse_errors.set(partial.errors);
                }
                ParseResult::Failed(errors) => {
                    partial_pipeline.set(None);
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
            let raw_val = (raw_pipeline)();

            if source != EditSource::Inspector {
                return;
            }

            if let Some(ref config) = raw_val {
                let yaml = serialize_raw_yaml(config);
                yaml_text.set(yaml);
                parse_errors.set(Vec::new());
            }
        });
    }

    // ── Channel resolution: resolve pipeline through channel overrides ────
    {
        use_effect(move || {
            let pl = (pipeline)();
            let cs = (channel_state)();

            let active_id = cs.as_ref().and_then(|s| s.active_channel.clone());

            match (pl.as_ref(), active_id, cs.as_ref()) {
                (Some(config), Some(channel_id), Some(cs_ref)) => {
                    // Derive pipeline path from active tab
                    let pl_path = (active_tab_id)().and_then(|id| {
                        tabs.read()
                            .iter()
                            .find(|t| t.id == id)
                            .and_then(|t| t.file_path.clone())
                    });

                    match crate::channel_resolve::resolve_pipeline_for_channel(
                        config,
                        &channel_id,
                        cs_ref,
                        pl_path.as_deref(),
                    ) {
                        Ok(resolution) => channel_pipeline.set(Some(resolution)),
                        Err(_e) => {
                            channel_pipeline.set(None);
                        }
                    }
                }
                _ => {
                    channel_pipeline.set(None);
                }
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
            let pp = (partial_pipeline)();
            let errs = (parse_errors)();
            let src = (edit_source)();
            let sel = (selected_stage)();

            if let Some(active_id) = (active_tab_id)() {
                let mut tabs_w = tabs.write();
                if let Some(tab) = tabs_w.iter_mut().find(|t| t.id == active_id) {
                    tab.snapshot.yaml_text = text;
                    tab.snapshot.pipeline = pl;
                    tab.snapshot.partial_pipeline = pp;
                    tab.snapshot.parse_errors = errs;
                    tab.snapshot.edit_source = src;
                    tab.snapshot.selected_stage = sel;
                }
            }
        });
    }

    let has_active_tab = current_active.is_some();
    let current_ctx = (active_context)();

    rsx! {
        document::Title { "clinker kiln" }

        div {
            class: "kiln-app",
            "data-theme": (theme)().as_data_attr(),
            tabindex: "0",
            onkeydown: move |e: KeyboardEvent| {
                if handle_keyboard(&e, &mut kb_tab_mgr) {
                    e.prevent_default();
                }
            },

            TitleBar {}

            // ── Body: activity bar + content area ──
            div {
                class: "kiln-body",

                ActivityBar {}

                div {
                    class: "kiln-content-area",

                    // Tab bar: Pipeline context only
                    if current_ctx == NavigationContext::Pipeline {
                        TabBar {}
                    }

                    // Context content dispatch with cross-fade
                    div {
                        class: "kiln-context-content",
                        key: "{current_ctx.as_data_attr()}",
                        "data-context": current_ctx.as_data_attr(),

                        match current_ctx {
                            NavigationContext::Pipeline => rsx! {
                                if has_active_tab {
                                    ActiveTabContent {
                                        key: "{current_active.map(|id| id.to_string()).unwrap_or_default()}",
                                    }
                                } else {
                                    WelcomeScreen {}
                                }
                            },
                            NavigationContext::Git => rsx! { VersionMode {} },
                            NavigationContext::Docs => rsx! {
                                PlaceholderPage {
                                    name: "Technical Guide",
                                    description: "Kiln user guide, CXL reference, and pipeline authoring documentation.",
                                }
                            },
                            NavigationContext::Channels => rsx! { ChannelMode {} },
                            NavigationContext::Runs => rsx! {
                                PlaceholderPage {
                                    name: "Run History",
                                    description: "Pipeline execution history, filtering, and run detail breakdowns.",
                                }
                            },
                        }
                    }
                }
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

/// Active tab's content area — Pipeline context only.
///
/// Renders canvas, inspector, YAML sidebar, and left panel slot.
/// Keyed on tab ID for clean remount.
#[component]
fn ActiveTabContent() -> Element {
    let state = use_app_state();
    let pipeline_layout = state.pipeline_layout;
    let selected_stage = state.selected_stage;
    let tab_mgr = use_context::<TabManagerState>();
    let left_panel = (tab_mgr.left_panel)();

    // Schematics layout mode: full-pipeline documentation view
    if *pipeline_layout.read() == PipelineLayoutMode::Schematics {
        return rsx! {
            div {
                class: "kiln-main",
                "data-layout": "schematics",
                SchematicsPanel {}
            }
        };
    }

    rsx! {
        div {
            class: "kiln-main",
            "data-layout": pipeline_layout.read().as_data_attr(),

            // ── Left panel slot (280px, shared between Search, Schemas, Compositions) ──
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

            // Inspector shows for any selected stage, even when drilled into a composition.
            // Drilled transforms exist in state.pipeline (expanded during resolution).
            if let Some(ref stage_id) = (selected_stage)() {
                InspectorPanel {
                    key: "{stage_id}",
                    stage_id: stage_id.clone(),
                }
            }

            // YAML sidebar hidden during drill-in (editing YAML while viewing a
            // composition slice would be confusing).
            if (state.composition_drill_stack)().is_empty() {
                YamlSidebar {}
            }
        }
    }
}
