//! Individual composition card in the composition panel.
//!
//! Shows: composition diamond icon, name, version badge, transform count,
//! contract status, and pipeline usage count.

use dioxus::prelude::*;

use crate::composition_index::CompositionEntry;

/// Composition card component — one per discovered `.comp.yaml` file.
#[component]
pub fn CompositionCard(entry: CompositionEntry) -> Element {
    let name = &entry.meta.name;
    let version = entry
        .meta
        .version
        .as_deref()
        .unwrap_or("—");
    let transform_count = entry.transform_count;
    let has_contract = entry.contract.is_some();
    let used_by_count = entry.used_by.len();
    let path_display = entry.path.display().to_string();

    let contract_indicator = if has_contract { "\u{2713}" } else { "\u{2014}" };

    let usage_text = if used_by_count == 0 {
        "unused".to_string()
    } else if used_by_count == 1 {
        "Used by 1 pipeline".to_string()
    } else {
        format!("Used by {used_by_count} pipelines")
    };

    rsx! {
        div {
            class: "kiln-schema-card",

            // ── Card header ─────────────────────────────────────────────
            div { class: "kiln-schema-card__header",
                span { class: "kiln-schema-card__diamond", "\u{25c8}" }
                span { class: "kiln-schema-card__name", "{name}" }
                span { class: "kiln-schema-card__count", "v{version}" }
            }

            // ── Card meta ───────────────────────────────────────────────
            div { class: "kiln-schema-card__meta",
                span { class: "kiln-schema-card__path", "{path_display}" }
            }
            div { class: "kiln-schema-card__meta",
                span { class: "kiln-schema-card__format",
                    "{transform_count} transforms"
                }
                span { class: "kiln-schema-card__sep", " \u{00b7} " }
                span { class: "kiln-schema-card__format",
                    "contract {contract_indicator}"
                }
                span { class: "kiln-schema-card__sep", " \u{00b7} " }
                span { class: "kiln-schema-card__usage", "{usage_text}" }
            }

            // ── Description ─────────────────────────────────────────────
            if let Some(ref desc) = entry.meta.description {
                if !desc.is_empty() {
                    div { class: "kiln-schema-card__desc", "{desc}" }
                }
            }
        }
    }
}
