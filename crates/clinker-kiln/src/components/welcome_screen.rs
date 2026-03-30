/// Welcome screen shown when no tabs are open.
///
/// Spec §F7: brand badge, recent files, open/new buttons, shortcut hints.

use dioxus::prelude::*;

use crate::state::TabManagerState;
use crate::tab::TabEntry;

/// Welcome / start screen component.
#[component]
pub fn WelcomeScreen() -> Element {
    let mut tab_mgr: TabManagerState = use_context();

    rsx! {
        div {
            class: "kiln-welcome",

            // Brand badge (stacked)
            div {
                class: "kiln-welcome-brand",
                div { class: "kiln-welcome-brand-top", "CLINKER" }
                div { class: "kiln-welcome-brand-bottom", "KILN" }
            }

            // Subtitle
            div {
                class: "kiln-welcome-subtitle",
                "pipeline configuration IDE"
            }

            // Rust line divider
            hr { class: "kiln-rust-line" }

            // Action buttons
            div {
                class: "kiln-welcome-actions",

                button {
                    class: "kiln-welcome-btn",
                    onclick: move |_| {
                        // Open file dialog
                        if let Some(path) = crate::file_ops::open_file_dialog(None) {
                            match crate::file_ops::read_pipeline_file(&path) {
                                Ok(yaml) => {
                                    let new_tab = TabEntry::from_file(path, yaml);
                                    let new_id = new_tab.id;
                                    tab_mgr.tabs.write().push(new_tab);
                                    tab_mgr.active_tab_id.set(Some(new_id));
                                }
                                Err(_e) => {
                                    // TODO: show error toast
                                }
                            }
                        }
                    },
                    "Open File"
                }

                button {
                    class: "kiln-welcome-btn",
                    onclick: move |_| {
                        let new_tab = TabEntry::new_untitled();
                        let new_id = new_tab.id;
                        tab_mgr.tabs.write().push(new_tab);
                        tab_mgr.active_tab_id.set(Some(new_id));
                    },
                    "New Pipeline"
                }
            }

            // Shortcut hints
            div {
                class: "kiln-welcome-shortcuts",
                div { span { class: "kiln-welcome-key", "Ctrl+O" } " open file" }
                div { span { class: "kiln-welcome-key", "Ctrl+N" } " new pipeline" }
            }
        }
    }
}
