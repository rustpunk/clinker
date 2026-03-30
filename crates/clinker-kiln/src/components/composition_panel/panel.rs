//! Composition panel — left-side slide-in (280px) showing all discovered compositions.
//!
//! Compositions are `.comp.yaml` files containing reusable transformation groups.
//! Each card shows name, version, transform count, contract status, and pipeline usage.

use dioxus::prelude::*;

use crate::composition_index::CompositionIndex;
use crate::state::{LeftPanel, TabManagerState};

use super::composition_card::CompositionCard;

/// Composition browser panel component.
///
/// Displays all discovered compositions sorted by name with search filtering.
/// Follows the same layout pattern as SchemaPanel.
#[component]
pub fn CompositionPanel() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let index = (tab_mgr.composition_index)();
    let mut search_text = use_signal(String::new);

    let search = (search_text)();

    // Filter compositions by search text
    let compositions: Vec<_> = index
        .compositions
        .iter()
        .filter(|entry| {
            if search.is_empty() {
                return true;
            }
            let q = search.to_lowercase();
            entry.meta.name.to_lowercase().contains(&q)
                || entry
                    .meta
                    .description
                    .as_deref()
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&q)
        })
        .collect();

    rsx! {
        div {
            class: "kiln-schema-panel",

            // ── Header ──────────────────────────────────────────────────
            div { class: "kiln-schema-panel__header",
                span { class: "kiln-schema-panel__title",
                    "COMPOSITIONS — {index.len()}"
                }
                button {
                    class: "kiln-schema-panel__close",
                    onclick: move |_| tab_mgr.left_panel.set(LeftPanel::None),
                    "\u{00d7}"
                }
            }

            // ── Search input ────────────────────────────────────────────
            div { class: "kiln-schema-panel__search",
                input {
                    class: "kiln-schema-panel__search-input",
                    r#type: "text",
                    placeholder: "Search compositions...",
                    value: "{search_text}",
                    oninput: move |e: FormEvent| search_text.set(e.value()),
                }
            }

            // ── Composition list ────────────────────────────────────────
            div { class: "kiln-schema-panel__list",
                if compositions.is_empty() {
                    div { class: "kiln-schema-panel__empty",
                        if index.is_empty() {
                            "No compositions found."
                            br {}
                            "Add .comp.yaml files to compositions/"
                        } else {
                            "No compositions match the current filter."
                        }
                    }
                }

                for entry in &compositions {
                    CompositionCard {
                        key: "{entry.path.display()}",
                        entry: (*entry).clone(),
                    }
                }
            }

            // ── Bottom actions ──────────────────────────────────────────
            div { class: "kiln-schema-panel__actions",
                button {
                    class: "kiln-schema-panel__action-btn",
                    onclick: move |_| {
                        // TODO: create new composition file
                    },
                    "+ New Composition"
                }
            }
        }
    }
}
