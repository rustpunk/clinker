/// Global keyboard shortcut handler.
///
/// Spec §F6: Ctrl+N/O/S/Shift+S/W/Q/Tab/Shift+Tab/1-9.
/// Attached at the AppShell level to capture shortcuts regardless of focus.

use dioxus::prelude::*;

use crate::file_ops;
use crate::state::TabManagerState;
use crate::sync::serialize_yaml;
use crate::tab::TabEntry;

/// Handle a global keyboard event. Returns `true` if the event was consumed.
pub fn handle_keyboard(event: &KeyboardEvent, tab_mgr: &mut TabManagerState) -> bool {
    if !event.modifiers().ctrl() {
        return false;
    }

    let key = event.key();

    match key {
        // Ctrl+N — New untitled tab
        Key::Character(ref c) if c == "n" && !event.modifiers().shift() => {
            let new_tab = TabEntry::new_untitled();
            let new_id = new_tab.id;
            tab_mgr.tabs.write().push(new_tab);
            tab_mgr.active_tab_id.set(Some(new_id));
            true
        }

        // Ctrl+O — Open file
        Key::Character(ref c) if c == "o" && !event.modifiers().shift() => {
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
                            let new_tab = TabEntry::from_file(path, yaml);
                            let new_id = new_tab.id;
                            tab_mgr.tabs.write().push(new_tab);
                            tab_mgr.active_tab_id.set(Some(new_id));
                        }
                    }
                    Err(_e) => {
                        // TODO: show error toast
                    }
                }
            }
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

        // Ctrl+W — Close active tab
        Key::Character(ref c) if c == "w" && !event.modifiers().shift() => {
            if let Some(active_id) = (tab_mgr.active_tab_id)() {
                close_tab(tab_mgr, active_id);
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

/// Save the active tab to disk.
fn save_active_tab(tab_mgr: &mut TabManagerState, force_save_as: bool) {
    let active_id = match (tab_mgr.active_tab_id)() {
        Some(id) => id,
        None => return,
    };

    let mut tabs = tab_mgr.tabs;

    // Find the active tab and get its current YAML + file path
    let (yaml, current_path) = {
        let tabs_read = tabs.read();
        let Some(tab) = tabs_read.iter().find(|t| t.id == active_id) else {
            return;
        };

        // Serialize from pipeline model if available, otherwise use raw YAML
        let yaml = match (tab.pipeline)() {
            Some(ref config) => serialize_yaml(config),
            None => (tab.yaml_text)(),
        };

        (yaml, tab.file_path.clone())
    };

    // Determine the save path
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
            // Update tab state: mark saved with new path
            let mut tabs_write = tabs.write();
            if let Some(tab) = tabs_write.iter_mut().find(|t| t.id == active_id) {
                tab.mark_saved(path);
            }
        }
        Err(_e) => {
            // TODO: show error toast
        }
    }
}

/// Close a tab by ID. TODO: dirty confirmation in Phase 2.5c.
fn close_tab(tab_mgr: &mut TabManagerState, tab_id: crate::tab::TabId) {
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
