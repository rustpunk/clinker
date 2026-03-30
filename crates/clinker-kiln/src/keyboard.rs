/// Global keyboard shortcut handler.
///
/// Spec §F6: Ctrl+N/O/S/Shift+S/W/Q/Tab/Shift+Tab/1-9.
/// Spec §S5.3: Ctrl+Shift+F (search), Ctrl+Shift+E (schemas), Ctrl+Shift+N (templates).
/// Attached at the AppShell level to capture shortcuts regardless of focus.

use dioxus::prelude::*;

use crate::components::confirm_dialog::PendingConfirm;
use crate::components::toast::{toast_error, toast_success, ToastState};
use crate::file_ops;
use crate::state::{LeftPanel, TabManagerState};
use crate::sync::serialize_yaml;
use crate::tab::{TabEntry, TabId};
use crate::workspace;

/// Handle a global keyboard event. Returns `true` if the event was consumed.
pub fn handle_keyboard(event: &KeyboardEvent, tab_mgr: &mut TabManagerState) -> bool {
    if !event.modifiers().ctrl() {
        return false;
    }

    let key = event.key();

    match key {
        // Ctrl+Shift+F — Toggle search panel
        Key::Character(ref c) if c == "F" && event.modifiers().shift() => {
            let current = (tab_mgr.left_panel)();
            tab_mgr.left_panel.set(if current == LeftPanel::Search {
                LeftPanel::None
            } else {
                LeftPanel::Search
            });
            true
        }

        // Ctrl+Shift+E — Toggle schema panel
        Key::Character(ref c) if c == "E" && event.modifiers().shift() => {
            let current = (tab_mgr.left_panel)();
            tab_mgr.left_panel.set(if current == LeftPanel::Schemas {
                LeftPanel::None
            } else {
                LeftPanel::Schemas
            });
            true
        }

        // Ctrl+Shift+N — Template gallery
        Key::Character(ref c) if c == "N" && event.modifiers().shift() => {
            let current = (tab_mgr.show_template_gallery)();
            tab_mgr.show_template_gallery.set(!current);
            true
        }

        // Ctrl+N — New untitled tab
        Key::Character(ref c) if c == "n" && !event.modifiers().shift() => {
            let new_tab = TabEntry::new_untitled(&tab_mgr.tabs.read());
            let new_id = new_tab.id;
            tab_mgr.tabs.write().push(new_tab);
            tab_mgr.active_tab_id.set(Some(new_id));
            true
        }

        // Ctrl+O — Open file
        Key::Character(ref c) if c == "o" && !event.modifiers().shift() => {
            open_file(tab_mgr);
            true
        }

        // Ctrl+Shift+O — Open workspace
        Key::Character(ref c) if c == "O" && event.modifiers().shift() => {
            open_workspace(tab_mgr);
            true
        }

        // Ctrl+S — Save active tab
        Key::Character(ref c) if c == "s" && !event.modifiers().shift() => {
            save_active_tab(tab_mgr, false);
            true
        }

        // Ctrl+Shift+S — Save As
        Key::Character(ref c) if c == "S" && event.modifiers().shift() => {
            save_active_tab(tab_mgr, true);
            true
        }

        // Ctrl+W — Close active tab (with dirty confirmation)
        Key::Character(ref c) if c == "w" && !event.modifiers().shift() => {
            if let Some(active_id) = (tab_mgr.active_tab_id)() {
                request_close_tab(tab_mgr, active_id);
            }
            true
        }

        // Ctrl+Tab — Next tab
        Key::Tab if !event.modifiers().shift() => {
            cycle_tab(tab_mgr, 1);
            true
        }

        // Ctrl+Shift+Tab — Previous tab
        Key::Tab if event.modifiers().shift() => {
            cycle_tab(tab_mgr, -1);
            true
        }

        // Ctrl+1-9 — Switch to tab N
        Key::Character(ref c) if c.len() == 1 => {
            if let Some(digit) = c.chars().next().and_then(|ch| ch.to_digit(10)) {
                if digit >= 1 && digit <= 9 {
                    let idx = (digit - 1) as usize;
                    let tabs = tab_mgr.tabs.read();
                    if let Some(tab) = tabs.get(idx) {
                        tab_mgr.active_tab_id.set(Some(tab.id));
                    }
                    return true;
                }
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
            let id = active_path.as_ref().and_then(|ap| {
                restored_tabs.iter().find(|t| {
                    t.file_path.as_ref().map(|p| p.display().to_string()).as_deref() == Some(ap)
                }).map(|t| t.id)
            }).or_else(|| restored_tabs.first().map(|t| t.id));

            let mut tabs_w = tab_mgr.tabs.write();
            for tab in restored_tabs {
                tabs_w.push(tab);
            }
            id
        };

        tab_mgr.active_tab_id.set(active_id);
        tab_mgr.workspace.set(Some(ws));
    }
}

/// Open a file via native dialog.
pub fn open_file(tab_mgr: &mut TabManagerState) {
    if let Some(path) = file_ops::open_file_dialog(None) {
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
                    if let Some(ws_root) = workspace::detect_workspace(&path) {
                        if tab_mgr.workspace.peek().is_none() {
                            if let Some(ws) = workspace::load_workspace(&ws_root) {
                                tab_mgr.workspace.set(Some(ws));
                            }
                        }
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
            if let Some(parent) = path.parent() {
                if workspace::detect_workspace(&path).is_none() {
                    if workspace::auto_create_workspace(parent) {
                        let mut toast: Signal<Option<ToastState>> = use_context();
                        toast_success(&mut toast, "Workspace created \u{00B7} kiln.toml");
                    }
                }
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
        confirm.set(Some(PendingConfirm {
            tab_id,
            filename,
        }));
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
