//! Schema panel — left-side slide-in (280px) showing all discovered schemas.
//!
//! Schemas are grouped by format (CSV, JSON, XML) with expandable field lists.
//! Spec §S3.6: format tabs, field search, schema cards.

use dioxus::prelude::*;

use clinker_schema::{FormatCategory, SourceFormat, SourceSchema};

use crate::state::{LeftPanel, TabManagerState};
use crate::tab::TabEntry;

use super::schema_card::SchemaCard;

/// Scaffold YAML for new schema files.
const NEW_SCHEMA_SCAFFOLD: &str = r#"_schema:
  name: new_schema
  format: csv
  description: ""

fields:
  - name: id
    type: int
    nullable: false
  - name: name
    type: string
"#;

/// Active format filter tab.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum FormatFilter {
    #[default]
    All,
    Csv,
    Json,
    Xml,
}

impl FormatFilter {
    fn matches(self, format: SourceFormat) -> bool {
        match self {
            Self::All => true,
            Self::Csv => format.category() == FormatCategory::Csv,
            Self::Json => format.category() == FormatCategory::Json,
            Self::Xml => format.category() == FormatCategory::Xml,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Csv => "CSV",
            Self::Json => "JSON",
            Self::Xml => "XML",
        }
    }
}

const FORMAT_FILTERS: [FormatFilter; 4] = [
    FormatFilter::All,
    FormatFilter::Csv,
    FormatFilter::Json,
    FormatFilter::Xml,
];

/// Schema browser panel component.
///
/// Displays all discovered schemas grouped by format with expandable
/// field lists. Supports format filtering and field search.
#[component]
pub fn SchemaPanel() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let index = (tab_mgr.schema_index)();
    let mut active_filter = use_signal(|| FormatFilter::All);
    let mut search_text = use_signal(String::new);

    let filter = (active_filter)();
    let search = (search_text)();

    // Collect and sort schemas by format then name
    let mut schemas: Vec<&SourceSchema> = index.schemas.values().collect();
    schemas.sort_by(|a, b| {
        a.metadata
            .format
            .label()
            .cmp(b.metadata.format.label())
            .then(a.metadata.name.cmp(&b.metadata.name))
    });

    // Apply format filter
    let schemas: Vec<&SourceSchema> = schemas
        .into_iter()
        .filter(|s| filter.matches(s.metadata.format))
        .filter(|s| {
            if search.is_empty() {
                return true;
            }
            let q = search.to_lowercase();
            s.metadata.name.to_lowercase().contains(&q)
                || s.all_field_names()
                    .iter()
                    .any(|f| f.to_lowercase().contains(&q))
        })
        .collect();

    // Group by format category for section headers
    let mut current_category: Option<FormatCategory> = None;

    rsx! {
        div {
            class: "kiln-schema-panel",

            // ── Header ──────────────────────────────────────────────────
            div { class: "kiln-schema-panel__header",
                span { class: "kiln-schema-panel__title",
                    "SOURCE SCHEMAS — {index.len()}"
                }
                button {
                    class: "kiln-schema-panel__close",
                    onclick: move |_| tab_mgr.left_panel.set(LeftPanel::None),
                    "×"
                }
            }

            // ── Search input ────────────────────────────────────────────
            div { class: "kiln-schema-panel__search",
                input {
                    class: "kiln-schema-panel__search-input",
                    r#type: "text",
                    placeholder: "Search fields...",
                    value: "{search_text}",
                    oninput: move |e: FormEvent| search_text.set(e.value()),
                }
            }

            // ── Format filter tabs ──────────────────────────────────────
            div { class: "kiln-schema-panel__tabs",
                for f in FORMAT_FILTERS {
                    button {
                        class: if filter == f { "kiln-schema-tab kiln-schema-tab--active" } else { "kiln-schema-tab" },
                        onclick: move |_| active_filter.set(f),
                        "{f.label()}"
                    }
                }
            }

            // ── Schema list ─────────────────────────────────────────────
            div { class: "kiln-schema-panel__list",
                if schemas.is_empty() {
                    div { class: "kiln-schema-panel__empty",
                        if index.is_empty() {
                            "No schemas found."
                            br {}
                            "Add .schema.yaml files to schemas/"
                        } else {
                            "No schemas match the current filter."
                        }
                    }
                }

                for schema in &schemas {
                    {
                        let cat = schema.metadata.format.category();
                        let show_header = if filter == FormatFilter::All {
                            let needs = current_category != Some(cat);
                            current_category = Some(cat);
                            needs
                        } else {
                            false
                        };

                        rsx! {
                            if show_header {
                                div { class: "kiln-schema-panel__section-header",
                                    "{schema.metadata.format.label()}"
                                }
                            }
                            SchemaCard {
                                key: "{schema.path.display()}",
                                schema: (*schema).clone(),
                            }
                        }
                    }
                }
            }

            // ── Bottom actions ──────────────────────────────────────────
            div { class: "kiln-schema-panel__actions",
                button {
                    class: "kiln-schema-panel__action-btn",
                    onclick: move |_| {
                        let yaml = NEW_SCHEMA_SCAFFOLD;
                        let new_tab = TabEntry::new_from_yaml(
                            &tab_mgr.tabs.read(),
                            yaml.to_string(),
                        );
                        let new_id = new_tab.id;
                        tab_mgr.tabs.write().push(new_tab);
                        tab_mgr.active_tab_id.set(Some(new_id));
                    },
                    "+ New Schema"
                }
            }
        }
    }
}
