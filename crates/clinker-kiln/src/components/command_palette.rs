//! Command palette — searchable overlay for all Kiln operations.
//!
//! Built on `dioxus-nox-cmdk` for fuzzy search, keyboard navigation,
//! and accessible command execution.
//! Spec: clinker-kiln-git-addendum.md §G4, navigation addendum §N8.

use dioxus::prelude::*;
use dioxus_nox_cmdk::*;

use crate::commands::{CommandGroup, all_commands};
use crate::components::activity_bar::switch_context;
use crate::state::{LeftPanel, NavigationContext, PipelineLayoutMode, TabManagerState};

/// Command palette overlay component.
#[component]
pub fn CommandPalette() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let has_git = (tab_mgr.git_state)().is_some();
    let commands = all_commands();

    let visible_commands: Vec<_> = commands
        .iter()
        .filter(|c| !c.requires_git || has_git)
        .cloned()
        .collect();

    let groups = [
        CommandGroup::Navigation,
        CommandGroup::File,
        CommandGroup::Layout,
        CommandGroup::Search,
        CommandGroup::Composition,
        CommandGroup::Template,
        CommandGroup::Settings,
        CommandGroup::Git,
    ];

    // Build grouped command data before RSX
    let mut grouped: Vec<(CommandGroup, Vec<crate::commands::Command>)> = Vec::new();
    for group in groups {
        let cmds: Vec<_> = visible_commands
            .iter()
            .filter(|c| c.group == group)
            .cloned()
            .collect();
        if !cmds.is_empty() {
            grouped.push((group, cmds));
        }
    }

    let _open_signal = use_signal(|| true);

    rsx! {
        // Backdrop
        div {
            class: "kiln-palette-backdrop",
            onclick: move |_| tab_mgr.show_command_palette.set(false),
        }

        // Palette container
        div {
            class: "kiln-palette",
            onclick: move |e: MouseEvent| e.stop_propagation(),

            CommandRoot {
                on_select: move |val: String| {
                    execute_command(&val, &mut tab_mgr);
                    tab_mgr.show_command_palette.set(false);
                },

                CommandInput {
                    placeholder: "Type a command...",
                    autofocus: true,
                }

                CommandList {
                    CommandEmpty { "No matching commands." }

                    for (group, cmds) in &grouped {
                        {
                            let group_id = format!("{:?}", group);
                            let group_label = group.label().to_string();

                            rsx! {
                                CommandGroup {
                                    id: group_id,
                                    heading: group_label,

                                    for cmd in cmds {
                                        {
                                            let id = cmd.id.to_string();
                                            let label = cmd.label.to_string();
                                            let desc = cmd.description.to_string();
                                            let shortcut = cmd.shortcut.map(|s| s.to_string());

                                            rsx! {
                                                CommandItem {
                                                    id: id.clone(),
                                                    label: label.clone(),
                                                    value: id.clone(),
                                                    keywords: vec![desc.clone()],

                                                    div { class: "kiln-palette-item",
                                                        span { class: "kiln-palette-item__label", "{label}" }
                                                        span { class: "kiln-palette-item__desc", "{desc}" }
                                                        if let Some(ref sc) = shortcut {
                                                            span { class: "kiln-palette-item__shortcut", "{sc}" }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Execute a command by its ID.
fn execute_command(id: &str, tab_mgr: &mut TabManagerState) {
    let app_state_sig = use_context::<Signal<crate::state::AppState>>();
    let mut app = *app_state_sig.read();

    match id {
        // ── Navigation commands ────────────────────────────────
        "nav.pipeline" => switch_context(&app, tab_mgr, NavigationContext::Pipeline),
        "nav.channels" => switch_context(&app, tab_mgr, NavigationContext::Channels),
        "nav.git" => switch_context(&app, tab_mgr, NavigationContext::Git),
        "nav.docs" => switch_context(&app, tab_mgr, NavigationContext::Docs),
        "nav.runs" => switch_context(&app, tab_mgr, NavigationContext::Runs),

        // ── Layout commands (switch to Pipeline + set mode) ────
        "layout.canvas" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            app.pipeline_layout.set(PipelineLayoutMode::Canvas);
        }
        "layout.hybrid" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            app.pipeline_layout.set(PipelineLayoutMode::Hybrid);
        }
        "layout.editor" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            app.pipeline_layout.set(PipelineLayoutMode::Editor);
        }
        "layout.schematics" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            app.pipeline_layout.set(PipelineLayoutMode::Schematics);
        }

        // ── Search/panel commands (switch to Pipeline first) ───
        "search.text" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            let current = (tab_mgr.left_panel)();
            tab_mgr.left_panel.set(if current == LeftPanel::Search {
                LeftPanel::None
            } else {
                LeftPanel::Search
            });
        }
        "search.schemas" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            let current = (tab_mgr.left_panel)();
            tab_mgr.left_panel.set(if current == LeftPanel::Schemas {
                LeftPanel::None
            } else {
                LeftPanel::Schemas
            });
        }
        "composition.browse" => {
            switch_context(&app, tab_mgr, NavigationContext::Pipeline);
            let current = (tab_mgr.left_panel)();
            tab_mgr
                .left_panel
                .set(if current == LeftPanel::Compositions {
                    LeftPanel::None
                } else {
                    LeftPanel::Compositions
                });
        }

        // ── Template ───────────────────────────────────────────
        "template.new" => {
            tab_mgr.show_template_gallery.set(true);
        }

        // ── Settings ───────────────────────────────────────────
        "settings.open" => {
            let current = (tab_mgr.show_settings)();
            tab_mgr.show_settings.set(!current);
        }

        // ── Git commands — switch to Git context first ─────────
        "git.commit" | "git.commit_all" | "git.stage_file" | "git.push" | "git.pull"
        | "git.fetch" | "git.switch_branch" | "git.create_branch" | "git.view_log"
        | "git.view_diff" => {
            switch_context(&app, tab_mgr, NavigationContext::Git);
            // Individual git command execution will be wired later
        }

        _ => {}
    }
}
