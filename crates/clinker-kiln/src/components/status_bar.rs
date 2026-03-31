//! Status bar — 22px persistent bottom bar.
//!
//! Context-aware information display. Adapts content per active context:
//! - Pipeline: branch, file changes, cursor, encoding, language
//! - Git: branch, sync state, change counts (prominent)
//! - Docs: pipeline name, stage count
//! - Channels/Runs: minimal (workspace label)
//!
//! Branch segment clickable → opens branch switcher dropdown.
//! Changes segment clickable → switches to Git context.
//! Spec: clinker-kiln-git-addendum.md §G3, navigation addendum §N5.

use dioxus::prelude::*;

use clinker_git::GitOps;

use crate::components::activity_bar::switch_context;
use crate::components::toast::{ToastState, toast_error, toast_success};
use crate::state::{KilnTheme, NavigationContext, TabManagerState, use_app_state};

/// Status bar component — anchored to viewport bottom.
#[component]
pub fn StatusBar() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let state = use_app_state();
    let current_ctx = (state.active_context)();
    let git = (tab_mgr.git_state)();
    let mut show_branch_switcher = use_signal(|| false);
    let is_switcher_open = (show_branch_switcher)();

    rsx! {
        div {
            class: "kiln-status-bar",

            // ── Context indicator ──────────────────────────────────────
            div {
                class: "kiln-status-segment kiln-status-segment--context",
                "clinker ●"
            }
            div { class: "kiln-status-divider" }
            div {
                class: "kiln-status-segment",
                match current_ctx {
                    NavigationContext::Pipeline => {
                        let mode = (state.pipeline_layout)();
                        format!("Pipeline · {}", mode.label())
                    },
                    other => other.label().to_string(),
                }
            }
            div { class: "kiln-status-divider" }

            // ── Git segments (shown in Pipeline + Git contexts) ─────────
            if matches!(current_ctx, NavigationContext::Pipeline | NavigationContext::Git) {
                if let Some(ref status) = git {
                    // Branch segment (clickable → branch switcher)
                    div {
                        class: "kiln-status-segment kiln-status-segment--branch kiln-status-segment--clickable",
                        onclick: move |_| show_branch_switcher.set(!is_switcher_open),
                        span { class: "kiln-status__branch-icon", "⑂" }
                        span { class: "kiln-status__branch-name",
                            {
                                if status.branch.len() > 20 {
                                    format!("{}…", &status.branch[..19])
                                } else {
                                    status.branch.clone()
                                }
                            }
                        }
                        if status.ahead > 0 {
                            span { class: "kiln-status__ahead", "↑{status.ahead}" }
                        }
                        if status.behind > 0 {
                            span { class: "kiln-status__behind", "↓{status.behind}" }
                        }
                    }

                    div { class: "kiln-status-divider" }

                    // Changes segment (clickable → Git context)
                    if status.has_changes() {
                        div {
                            class: "kiln-status-segment kiln-status-segment--changes kiln-status-segment--clickable",
                            onclick: move |_| {
                                switch_context(&state, &tab_mgr, NavigationContext::Git);
                            },
                            if status.added > 0 {
                                span { class: "kiln-status__added", "+{status.added}" }
                            }
                            if status.modified > 0 {
                                span { class: "kiln-status__modified", "~{status.modified}" }
                            }
                            if status.deleted > 0 {
                                span { class: "kiln-status__deleted", "−{status.deleted}" }
                            }
                            if status.untracked > 0 {
                                span { class: "kiln-status__untracked", "?{status.untracked}" }
                            }
                        }
                        div { class: "kiln-status-divider" }
                    }
                }
            }

            // ── Pipeline context: cursor, encoding, language ────────────
            if current_ctx == NavigationContext::Pipeline {
                div {
                    class: "kiln-status-segment kiln-status-segment--cursor",
                    "Ln 1, Col 1"
                }
                div { class: "kiln-status-divider" }
                div {
                    class: "kiln-status-segment kiln-status-segment--encoding",
                    "UTF-8"
                }
                div { class: "kiln-status-divider" }
                div {
                    class: "kiln-status-segment kiln-status-segment--lang",
                    "YAML"
                }
            }

            // ── Schematics layout: pipeline stage count ─────────────────
            if current_ctx == NavigationContext::Pipeline
                && (state.pipeline_layout)() == crate::state::PipelineLayoutMode::Schematics
            {
                if let Some(ref config) = (state.pipeline)() {
                    div {
                        class: "kiln-status-segment",
                        "{config.transformations.len()} stages"
                    }
                }
            }

            // ── Spacer ─────────────────────────────────────────────────
            div { class: "kiln-status-spacer" }

            // ── Theme toggle ───────────────────────────────────────────
            {
                let current_theme = (tab_mgr.theme)();
                let theme_label = match current_theme {
                    KilnTheme::Oxide => "\u{25D1} OXIDE",  // ◑
                    KilnTheme::Enamel => "\u{25D0} ENAMEL", // ◐
                };
                rsx! {
                    button {
                        class: "kiln-status-segment kiln-status-segment--theme kiln-status-segment--clickable",
                        title: "Toggle theme (Ctrl+Shift+T)",
                        onclick: move |_| {
                            tab_mgr.theme.set(current_theme.toggle());
                        },
                        "{theme_label}"
                    }
                    div { class: "kiln-status-divider" }
                }
            }

            // ── Git engine indicator ────────────────────────────────────
            if git.is_some() {
                div {
                    class: "kiln-status-segment kiln-status-segment--engine",
                    "git"
                }
            }
        }

        // ── Branch switcher dropdown ────────────────────────────────────
        if is_switcher_open {
            BranchSwitcher {
                on_close: move |_| show_branch_switcher.set(false),
            }
        }
    }
}

/// Branch switcher dropdown — opens above the status bar.
/// Spec: clinker-kiln-git-addendum.md §G3.4.
#[component]
fn BranchSwitcher(on_close: EventHandler<()>) -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let mut search = use_signal(String::new);
    let mut branches = use_signal(Vec::new);
    let mut loaded = use_signal(|| false);
    let mut new_branch_input = use_signal(|| false);
    let mut new_branch_name = use_signal(String::new);

    // Load branches on first render
    if !(loaded)() {
        let ws = (tab_mgr.workspace)();
        if let Some(ws) = ws
            && let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root)
            && let Ok(b) = ops.branches()
        {
            branches.set(b);
        }
        loaded.set(true);
    }

    let branch_list = (branches)();
    let query = (search)();
    let show_new = (new_branch_input)();

    let filtered: Vec<_> = branch_list
        .iter()
        .filter(|b| query.is_empty() || b.name.to_lowercase().contains(&query.to_lowercase()))
        .collect();

    rsx! {
        // Backdrop
        div {
            class: "kiln-branch-switcher-backdrop",
            onclick: move |_| on_close.call(()),
        }

        // Dropdown
        div {
            class: "kiln-branch-switcher",
            onclick: move |e: MouseEvent| e.stop_propagation(),

            // Search input
            input {
                class: "kiln-branch-switcher__search",
                r#type: "text",
                placeholder: "Search branches...",
                value: "{query}",
                autofocus: true,
                oninput: move |e: FormEvent| search.set(e.value()),
            }

            // Branch list
            div { class: "kiln-branch-switcher__list",
                for branch in filtered {
                    {
                        let name = branch.name.clone();
                        let is_current = branch.is_current;
                        let ahead = branch.ahead;
                        let behind = branch.behind;

                        rsx! {
                            div {
                                class: if is_current {
                                    "kiln-branch-switcher__item kiln-branch-switcher__item--current"
                                } else {
                                    "kiln-branch-switcher__item"
                                },
                                onclick: {
                                    let name = name.clone();
                                    move |_| {
                                        if !is_current {
                                            switch_branch(&mut tab_mgr, &name);
                                            on_close.call(());
                                        }
                                    }
                                },

                                if is_current {
                                    span { class: "kiln-branch-switcher__dot", "●" }
                                }
                                span { class: "kiln-branch-switcher__name", "{name}" }
                                if ahead > 0 {
                                    span { class: "kiln-status__ahead", "↑{ahead}" }
                                }
                                if behind > 0 {
                                    span { class: "kiln-status__behind", "↓{behind}" }
                                }
                            }
                        }
                    }
                }
            }

            // New branch section
            div { class: "kiln-branch-switcher__footer",
                if show_new {
                    div { class: "kiln-branch-switcher__new-row",
                        input {
                            class: "kiln-branch-switcher__new-input",
                            r#type: "text",
                            placeholder: "New branch name...",
                            value: "{new_branch_name}",
                            autofocus: true,
                            oninput: move |e: FormEvent| new_branch_name.set(e.value()),
                            onkeydown: move |e: KeyboardEvent| {
                                if e.key() == Key::Enter {
                                    let name = (new_branch_name)();
                                    if !name.is_empty() {
                                        create_and_switch(&mut tab_mgr, &name);
                                        on_close.call(());
                                    }
                                }
                            },
                        }
                    }
                } else {
                    button {
                        class: "kiln-branch-switcher__new-btn",
                        onclick: move |_| new_branch_input.set(true),
                        "+ New Branch"
                    }
                }
            }
        }
    }
}

/// Switch to a different branch.
fn switch_branch(tab_mgr: &mut TabManagerState, name: &str) {
    let ws = (tab_mgr.workspace)();
    let Some(ws) = ws else { return };

    let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root) else {
        return;
    };
    let mut toast: Signal<Option<ToastState>> = use_context();

    // Check for dirty state
    let status = ops.status();
    if let Ok(ref s) = status
        && s.has_changes()
    {
        toast_error(
            &mut toast,
            "Stash or commit changes before switching branches".to_string(),
        );
        return;
    }

    let result = std::process::Command::new("git")
        .args(["switch", name])
        .current_dir(&ws.root)
        .output();

    match result {
        Ok(output) if output.status.success() => {
            toast_success(&mut toast, format!("Switched to {name}"));
            if let Ok(new_status) = ops.status() {
                tab_mgr.git_state.set(Some(new_status));
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            toast_error(&mut toast, format!("switch failed: {stderr}"));
        }
        Err(e) => {
            toast_error(&mut toast, format!("switch failed: {e}"));
        }
    }
}

/// Create a new branch and switch to it.
fn create_and_switch(tab_mgr: &mut TabManagerState, name: &str) {
    let ws = (tab_mgr.workspace)();
    let Some(ws) = ws else { return };

    let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root) else {
        return;
    };
    let mut toast: Signal<Option<ToastState>> = use_context();

    let result = std::process::Command::new("git")
        .args(["switch", "-c", name])
        .current_dir(&ws.root)
        .output();

    match result {
        Ok(output) if output.status.success() => {
            toast_success(&mut toast, format!("Created and switched to {name}"));
            if let Ok(new_status) = ops.status() {
                tab_mgr.git_state.set(Some(new_status));
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            toast_error(&mut toast, format!("create branch failed: {stderr}"));
        }
        Err(e) => {
            toast_error(&mut toast, format!("create branch failed: {e}"));
        }
    }
}
