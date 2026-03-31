/// Settings overlay — modal panel for workspace configuration.
///
/// Not a navigation context — opens as a centered overlay on top of the
/// current context. Activity bar does not show Settings as "active."
/// After closing, the user returns to whatever context they were in.
///
/// Displays workspace settings from kiln.toml:
/// - Workspace name
/// - Pipeline discovery paths (include/exclude)
/// - Schema directory
/// - CLI binary path
/// - Environment variables
///
/// Spec: navigation addendum §N3.6.
use dioxus::prelude::*;

use crate::state::{KilnTheme, TabManagerState};

/// Settings overlay component — centered modal.
#[component]
pub fn SettingsOverlay() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let workspace = (tab_mgr.workspace)();

    rsx! {
        // Backdrop
        div {
            class: "kiln-settings-backdrop",
            onclick: move |_| tab_mgr.show_settings.set(false),
        }

        // Overlay panel
        div {
            class: "kiln-settings-overlay",
            onclick: move |e: MouseEvent| e.stop_propagation(),

            // Header
            div {
                class: "kiln-settings-header",
                h2 { class: "kiln-settings-title", "Settings" }
                button {
                    class: "kiln-settings-close",
                    onclick: move |_| tab_mgr.show_settings.set(false),
                    "✕"
                }
            }

            // Content
            div {
                class: "kiln-settings-content",

                // ── Appearance ─────────────────────────────────────
                SettingsSection {
                    label: "Appearance",
                    div {
                        class: "kiln-settings-theme-row",
                        span { class: "kiln-settings-field__label", "Theme" }
                        div { class: "kiln-settings-theme-toggle",
                            {
                                let current_theme = (tab_mgr.theme)();
                                rsx! {
                                    for variant in [KilnTheme::Oxide, KilnTheme::Enamel] {
                                        button {
                                            class: if current_theme == variant {
                                                "kiln-settings-theme-btn kiln-settings-theme-btn--active"
                                            } else {
                                                "kiln-settings-theme-btn"
                                            },
                                            onclick: move |_| tab_mgr.theme.set(variant),
                                            "{variant.label()}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(ref ws) = workspace {
                    // Workspace name
                    SettingsSection {
                        label: "Workspace",
                        SettingsField {
                            label: "Name",
                            value: ws.display_name(),
                        }
                        SettingsField {
                            label: "Root",
                            value: ws.root.display().to_string(),
                        }
                    }

                    // Pipeline discovery
                    SettingsSection {
                        label: "Pipelines",
                        SettingsField {
                            label: "Include globs",
                            value: if ws.pipeline_include_globs().is_empty() {
                                "(default: all YAML files)".to_string()
                            } else {
                                ws.pipeline_include_globs().join(", ")
                            },
                        }
                        SettingsField {
                            label: "Exclude globs",
                            value: if ws.pipeline_exclude_globs().is_empty() {
                                "(none)".to_string()
                            } else {
                                ws.pipeline_exclude_globs().join(", ")
                            },
                        }
                    }

                    // Schema configuration
                    SettingsSection {
                        label: "Schemas",
                        SettingsField {
                            label: "Directory",
                            value: ws.schema_dir(),
                        }
                        SettingsField {
                            label: "Inference sample rows",
                            value: ws.manifest.schema.as_ref()
                                .map(|s| s.infer_sample_rows.to_string())
                                .unwrap_or_else(|| "1000 (default)".to_string()),
                        }
                    }

                    // CLI configuration
                    SettingsSection {
                        label: "CLI",
                        SettingsField {
                            label: "Binary path",
                            value: ws.manifest.cli.as_ref()
                                .and_then(|c| c.binary.clone())
                                .unwrap_or_else(|| "(auto-detect)".to_string()),
                        }
                        if let Some(ref cli) = ws.manifest.cli {
                            if !cli.env.is_empty() {
                                for (key, val) in &cli.env {
                                    SettingsField {
                                        label: format!("env: {key}"),
                                        value: val.clone(),
                                    }
                                }
                            }
                        }
                    }

                    // Compositions
                    SettingsSection {
                        label: "Compositions",
                        SettingsField {
                            label: "Directory",
                            value: ws.manifest.compositions.as_ref()
                                .map(|c| c.directory.clone())
                                .unwrap_or_else(|| "compositions (default)".to_string()),
                        }
                    }
                } else {
                    div {
                        class: "kiln-settings-empty",
                        "No workspace loaded. Open a workspace to configure settings."
                    }
                }
            }

            // Footer
            div {
                class: "kiln-settings-footer",
                span {
                    class: "kiln-settings-hint",
                    "Edit kiln.toml directly for advanced configuration."
                }
            }
        }
    }
}

/// Settings section with a label header.
#[component]
fn SettingsSection(label: &'static str, children: Element) -> Element {
    rsx! {
        div {
            class: "kiln-settings-section",
            h3 { class: "kiln-settings-section__label", "{label}" }
            {children}
        }
    }
}

/// A single settings field (label + value).
#[component]
fn SettingsField(label: String, value: String) -> Element {
    rsx! {
        div {
            class: "kiln-settings-field",
            span { class: "kiln-settings-field__label", "{label}" }
            span { class: "kiln-settings-field__value", "{value}" }
        }
    }
}
