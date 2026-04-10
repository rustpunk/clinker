/// Global keyboard shortcut handler.
///
/// Context switching: Ctrl+Shift+E/C/G/R (Pipeline/Channels/Git/Runs).
/// Pipeline panel toggles: Alt+F/E/C (Search/Schemas/Compositions).
/// Layout modes (Pipeline only): Ctrl+Shift+1/2/3/4 (Canvas/Hybrid/Editor/Schematics).
/// Ctrl+Shift+D — Switch to Schematics layout mode (Pipeline autodoc).
/// Focus mode: F11 (toggle activity bar).
/// Navigation back: Ctrl+Alt+Left.
/// File ops: Ctrl+N/O/S/W/Q, Ctrl+Shift+S/O.
/// Tab switching: Ctrl+1-9 (Pipeline context only), Ctrl+Tab/Shift+Tab.
/// Overlays: Ctrl+Shift+P (command palette), Ctrl+Shift+N (templates), Ctrl+, (settings).
/// Theme: Ctrl+Shift+T (toggle Oxide/Enamel).
///
/// Attached at the AppShell level to capture shortcuts regardless of focus.
use dioxus::prelude::*;

use crate::components::activity_bar::{navigate_back, switch_context};
use crate::components::confirm_dialog::PendingConfirm;
use crate::components::toast::{ToastState, toast_error, toast_success};
use crate::file_ops;
use crate::state::{AppState, LeftPanel, NavigationContext, PipelineLayoutMode, TabManagerState};
use crate::sync::serialize_yaml;
use crate::tab::{TabEntry, TabId};
use crate::workspace;

/// Handle a global keyboard event. Returns `true` if the event was consumed.
pub fn handle_keyboard(event: &KeyboardEvent, tab_mgr: &mut TabManagerState) -> bool {
    let key = event.key();
    let ctrl = event.modifiers().ctrl();
    let shift = event.modifiers().shift();
    let alt = event.modifiers().alt();

    // Get app state for context-aware shortcuts
    let app_state_sig = use_context::<Signal<AppState>>();
    let mut app = *app_state_sig.read();
    let current_context = (app.active_context)();

    // ── F11 — Toggle focus mode (no modifiers required) ──────────────────
    if matches!(key, Key::F11) && !ctrl && !alt {
        let visible = (tab_mgr.activity_bar_visible)();
        tab_mgr.activity_bar_visible.set(!visible);
        return true;
    }

    // ── Alt+Letter — Pipeline panel toggles (Pipeline context only) ──────
    if alt
        && !ctrl
        && !shift
        && current_context == NavigationContext::Pipeline
        && let Key::Character(ref c) = key
    {
        match c.as_str() {
            "f" => {
                let current = (tab_mgr.left_panel)();
                tab_mgr.left_panel.set(if current == LeftPanel::Search {
                    LeftPanel::None
                } else {
                    LeftPanel::Search
                });
                return true;
            }
            "e" => {
                let current = (tab_mgr.left_panel)();
                tab_mgr.left_panel.set(if current == LeftPanel::Schemas {
                    LeftPanel::None
                } else {
                    LeftPanel::Schemas
                });
                return true;
            }
            "c" => {
                let current = (tab_mgr.left_panel)();
                tab_mgr
                    .left_panel
                    .set(if current == LeftPanel::Compositions {
                        LeftPanel::None
                    } else {
                        LeftPanel::Compositions
                    });
                return true;
            }
            _ => {}
        }
    }

    // ── Ctrl+Alt+Left — Navigate back in context history ─────────────────
    if ctrl && alt && !shift && matches!(key, Key::ArrowLeft) {
        navigate_back(&app, tab_mgr);
        return true;
    }

    // ── All remaining shortcuts require Ctrl ─────────────────────────────
    if !ctrl {
        return false;
    }

    match key {
        // ── Ctrl+Shift — Context switching + overlays ────────────────────
        // Ctrl+Shift+P — Command palette (unchanged, highest priority)
        Key::Character(ref c) if c == "P" && shift => {
            let current = (tab_mgr.show_command_palette)();
            tab_mgr.show_command_palette.set(!current);
            true
        }

        // Ctrl+Shift+E — Switch to Pipeline context
        Key::Character(ref c) if c == "E" && shift => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            true
        }

        // Ctrl+Shift+C — Switch to Channels context
        Key::Character(ref c) if c == "C" && shift => {
            switch_context(&app, tab_mgr, NavigationContext::Channels);
            true
        }

        // Ctrl+Shift+G — Switch to Git context
        Key::Character(ref c) if c == "G" && shift => {
            switch_context(&app, tab_mgr, NavigationContext::Git);
            true
        }

        // Ctrl+Shift+D — Switch to Schematics layout (Pipeline context)
        Key::Character(ref c) if c == "D" && shift => {
            if current_context != NavigationContext::Pipeline {
                switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            }
            app.pipeline_layout.set(PipelineLayoutMode::Schematics);
            true
        }

        // Ctrl+Shift+R — Switch to Runs context
        Key::Character(ref c) if c == "R" && shift => {
            switch_context(&app, tab_mgr, NavigationContext::Runs);
            true
        }

        // Ctrl+Shift+K — Channel switcher (toggle)
        Key::Character(ref c) if c == "K" && shift => {
            // Navigate to Channels context as a toggle
            if current_context == NavigationContext::Channels {
                switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            } else {
                switch_context(&app, tab_mgr, NavigationContext::Channels);
            }
            true
        }

        // Ctrl+Shift+N — Template gallery
        Key::Character(ref c) if c == "N" && shift => {
            let current = (tab_mgr.show_template_gallery)();
            tab_mgr.show_template_gallery.set(!current);
            true
        }

        // Ctrl+Shift+T — Toggle theme (Oxide ↔ Enamel)
        Key::Character(ref c) if c == "T" && shift => {
            let current = (tab_mgr.theme)();
            tab_mgr.theme.set(current.toggle());
            true
        }

        // Ctrl+Shift+1 — Canvas layout mode (Pipeline only)
        Key::Character(ref c) if c == "!" && shift => {
            if current_context == NavigationContext::Pipeline {
                app.pipeline_layout.set(PipelineLayoutMode::Canvas);
            }
            true
        }

        // Ctrl+Shift+2 — Hybrid layout mode (Pipeline only)
        Key::Character(ref c) if c == "@" && shift => {
            if current_context == NavigationContext::Pipeline {
                app.pipeline_layout.set(PipelineLayoutMode::Hybrid);
            }
            true
        }

        // Ctrl+Shift+3 — Editor layout mode (Pipeline only)
        Key::Character(ref c) if c == "#" && shift => {
            if current_context == NavigationContext::Pipeline {
                app.pipeline_layout.set(PipelineLayoutMode::Editor);
            }
            true
        }

        // Ctrl+Shift+4 — Schematics layout mode (Pipeline only)
        Key::Character(ref c) if c == "$" && shift => {
            if current_context == NavigationContext::Pipeline {
                app.pipeline_layout.set(PipelineLayoutMode::Schematics);
            }
            true
        }

        // ── Ctrl+, — Settings overlay ────────────────────────────────────
        Key::Character(ref c) if c == "," && !shift => {
            let current = (tab_mgr.show_settings)();
            tab_mgr.show_settings.set(!current);
            true
        }

        // ── Ctrl+Q — Graceful quit ──────────────────────────────────────
        Key::Character(ref c) if c == "q" && !shift => {
            dioxus::desktop::window().close();
            true
        }

        // ── Ctrl+N — New untitled tab ───────────────────────────────────
        Key::Character(ref c) if c == "n" && !shift => {
            // Switch to Pipeline context if not already there
            if current_context != NavigationContext::Pipeline {
                switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            }
            let new_tab = TabEntry::new_untitled(&tab_mgr.tabs.read());
            let new_id = new_tab.id;
            tab_mgr.tabs.write().push(new_tab);
            tab_mgr.active_tab_id.set(Some(new_id));
            true
        }

        // ── Ctrl+O — Open file ──────────────────────────────────────────
        Key::Character(ref c) if c == "o" && !shift => {
            open_file(tab_mgr);
            // Switch to Pipeline context after opening
            if current_context != NavigationContext::Pipeline {
                switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            }
            true
        }

        // ── Ctrl+Shift+O — Open workspace ───────────────────────────────
        Key::Character(ref c) if c == "O" && shift => {
            open_workspace(tab_mgr);
            true
        }

        // ── Ctrl+S — Save active tab ────────────────────────────────────
        Key::Character(ref c) if c == "s" && !shift => {
            save_active_tab(tab_mgr, false);
            true
        }

        // ── Ctrl+Shift+S — Save As ─────────────────────────────────────
        Key::Character(ref c) if c == "S" && shift => {
            save_active_tab(tab_mgr, true);
            true
        }

        // ── Ctrl+W — Close active tab ───────────────────────────────────
        Key::Character(ref c) if c == "w" && !shift => {
            if let Some(active_id) = (tab_mgr.active_tab_id)() {
                request_close_tab(tab_mgr, active_id);
            }
            true
        }

        // ── Ctrl+Tab — Next tab (Pipeline context only) ─────────────────
        Key::Tab if !shift => {
            if current_context == NavigationContext::Pipeline {
                cycle_tab(tab_mgr, 1);
            }
            true
        }

        // ── Ctrl+Shift+Tab — Previous tab (Pipeline context only) ───────
        Key::Tab if shift => {
            if current_context == NavigationContext::Pipeline {
                cycle_tab(tab_mgr, -1);
            }
            true
        }

        // ── Ctrl+1-9 — Switch to tab N (Pipeline context only) ──────────
        Key::Character(ref c) if c.len() == 1 && !shift => {
            if let Some(digit) = c.chars().next().and_then(|ch| ch.to_digit(10))
                && (1..=9).contains(&digit)
                && current_context == NavigationContext::Pipeline
            {
                let idx = (digit - 1) as usize;
                let tabs = tab_mgr.tabs.read();
                if let Some(tab) = tabs.get(idx) {
                    tab_mgr.active_tab_id.set(Some(tab.id));
                }
                return true;
            }
            false
        }

        _ => false,
    }
}

/// Open a workspace via native directory picker.
///
/// Loads the workspace manifest + state, restores tabs from state.
pub fn open_workspace(tab_mgr: &mut TabManagerState) {
    if let Some(ws) = workspace::open_workspace_dialog() {
        // Restore tabs from workspace state
        let (restored_tabs, active_path) = workspace::restore_tabs(&ws.state);

        // Close all existing tabs
        tab_mgr.tabs.write().clear();
        tab_mgr.active_tab_id.set(None);

        // Load restored tabs
        let active_id = if restored_tabs.is_empty() {
            None
        } else {
            let id = active_path
                .as_ref()
                .and_then(|ap| {
                    restored_tabs
                        .iter()
                        .find(|t| {
                            t.file_path
                                .as_ref()
                                .map(|p| p.display().to_string())
                                .as_deref()
                                == Some(ap)
                        })
                        .map(|t| t.id)
                })
                .or_else(|| restored_tabs.first().map(|t| t.id));

            let mut tabs_w = tab_mgr.tabs.write();
            for tab in restored_tabs {
                tabs_w.push(tab);
            }
            id
        };

        tab_mgr.active_tab_id.set(active_id);
        // Immediately persist this as the last-used workspace
        workspace::save_last_workspace(&ws.root);
        tab_mgr.workspace.set(Some(ws));
    }
}

/// Open a file via native dialog.
///
/// Defaults the file explorer to the workspace root if a workspace is active,
/// otherwise falls back to the OS default (usually ~/).
pub fn open_file(tab_mgr: &mut TabManagerState) {
    let starting_dir = tab_mgr.workspace.peek().as_ref().map(|ws| ws.root.clone());
    if let Some(path) = file_ops::open_file_dialog(starting_dir.as_deref()) {
        match file_ops::read_pipeline_file(&path) {
            Ok(yaml) => {
                // Check if already open
                let already_open = tab_mgr.tabs.read().iter().find_map(|t| {
                    if t.file_path.as_ref() == Some(&path) {
                        Some(t.id)
                    } else {
                        None
                    }
                });

                if let Some(existing_id) = already_open {
                    tab_mgr.active_tab_id.set(Some(existing_id));
                } else {
                    // Detect workspace from file location
                    if let Some(ws_root) = workspace::detect_workspace(&path)
                        && tab_mgr.workspace.peek().is_none()
                        && let Some(ws) = workspace::load_workspace(&ws_root)
                    {
                        // Immediately persist as last-used workspace
                        workspace::save_last_workspace(&ws.root);
                        tab_mgr.workspace.set(Some(ws));
                    }

                    let new_tab = TabEntry::from_file(path, yaml);
                    let new_id = new_tab.id;
                    tab_mgr.tabs.write().push(new_tab);
                    tab_mgr.active_tab_id.set(Some(new_id));
                }
            }
            Err(e) => {
                let mut toast: Signal<Option<ToastState>> = use_context();
                toast_error(&mut toast, e);
            }
        }
    }
}

/// Save the active tab to disk. Handles save-as for untitled tabs.
/// Auto-creates workspace (kiln.toml) on first save per §F4.3.
pub fn save_active_tab(tab_mgr: &mut TabManagerState, force_save_as: bool) {
    let active_id = match (tab_mgr.active_tab_id)() {
        Some(id) => id,
        None => return,
    };

    save_tab_by_id(tab_mgr, active_id, force_save_as);
}

/// Save a specific tab by ID.
fn save_tab_by_id(tab_mgr: &mut TabManagerState, tab_id: TabId, force_save_as: bool) {
    let mut tabs = tab_mgr.tabs;

    // Get current YAML + file path from snapshot
    let (yaml, current_path) = {
        let tabs_read = tabs.read();
        let Some(tab) = tabs_read.iter().find(|t| t.id == tab_id) else {
            return;
        };

        let yaml = match tab.snapshot.pipeline {
            Some(ref config) => serialize_yaml(config),
            None => tab.snapshot.yaml_text.clone(),
        };

        (yaml, tab.file_path.clone())
    };

    // Determine save path
    let save_path = if force_save_as || current_path.is_none() {
        let suggested = current_path
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "untitled.yaml".to_string());

        let starting_dir = current_path.as_ref().and_then(|p| p.parent());
        file_ops::save_file_dialog(&suggested, starting_dir)
    } else {
        current_path.clone()
    };

    let Some(path) = save_path else {
        return; // User cancelled
    };

    // Write to disk
    match file_ops::write_pipeline_file(&path, &yaml) {
        Ok(()) => {
            // Mark tab as saved
            let mut tabs_write = tabs.write();
            if let Some(tab) = tabs_write.iter_mut().find(|t| t.id == tab_id) {
                tab.mark_saved(path.clone(), &yaml);
            }

            // Auto-create workspace if needed (§F4.3)
            if let Some(parent) = path.parent()
                && workspace::detect_workspace(&path).is_none()
                && workspace::auto_create_workspace(parent)
            {
                let mut toast: Signal<Option<ToastState>> = use_context();
                toast_success(&mut toast, "Workspace created \u{00B7} kiln.toml");
            }
        }
        Err(e) => {
            let mut toast: Signal<Option<ToastState>> = use_context();
            toast_error(&mut toast, e);
        }
    }
}

/// Request to close a tab — shows confirm dialog if dirty.
pub fn request_close_tab(tab_mgr: &mut TabManagerState, tab_id: TabId) {
    let is_dirty = tab_mgr
        .tabs
        .read()
        .iter()
        .find(|t| t.id == tab_id)
        .map(|t| t.is_dirty())
        .unwrap_or(false);

    if is_dirty {
        // Show confirmation dialog
        let filename = tab_mgr
            .tabs
            .read()
            .iter()
            .find(|t| t.id == tab_id)
            .map(|t| t.display_name())
            .unwrap_or_else(|| "untitled.yaml".to_string());

        let mut confirm: Signal<Option<PendingConfirm>> = use_context();
        confirm.set(Some(PendingConfirm { tab_id, filename }));
    } else {
        force_close_tab(tab_mgr, tab_id);
    }
}

/// Save a tab then close it (called from confirm dialog "Save" action).
pub fn save_and_close_tab(tab_mgr: &mut TabManagerState, tab_id: TabId) {
    save_tab_by_id(tab_mgr, tab_id, false);
    force_close_tab(tab_mgr, tab_id);
}

/// Close a tab unconditionally (no dirty check).
pub fn force_close_tab(tab_mgr: &mut TabManagerState, tab_id: TabId) {
    let mut tabs = tab_mgr.tabs;
    let mut active = tab_mgr.active_tab_id;

    let current_active = (active)();

    let Some(idx) = tabs.read().iter().position(|t| t.id == tab_id) else {
        return;
    };

    tabs.write().remove(idx);

    if current_active == Some(tab_id) {
        let remaining = tabs.read().len();
        if remaining == 0 {
            active.set(None);
        } else {
            let new_idx = idx.min(remaining - 1);
            let new_id = tabs.read()[new_idx].id;
            active.set(Some(new_id));
        }
    }
}

/// Cycle through tabs (direction: +1 = next, -1 = previous).
fn cycle_tab(tab_mgr: &mut TabManagerState, direction: i32) {
    let tabs = tab_mgr.tabs.read();
    let count = tabs.len();
    if count <= 1 {
        return;
    }

    let active_id = (tab_mgr.active_tab_id)();
    let current_idx = active_id
        .and_then(|id| tabs.iter().position(|t| t.id == id))
        .unwrap_or(0);

    let new_idx = ((current_idx as i32 + direction).rem_euclid(count as i32)) as usize;
    tab_mgr.active_tab_id.set(Some(tabs[new_idx].id));
}
