//! Override diff component — shows base vs channel CXL for overridden stages.
//!
//! Rendered in the inspector panel when the selected stage has a channel override.

use dioxus::prelude::*;

use crate::channel_resolve::{AppliedOverride, OverrideKind, OverrideSource};

/// Override diff section displayed in the inspector.
#[component]
pub fn OverrideDiff(applied: AppliedOverride, base_cxl: String, resolved_cxl: String) -> Element {
    let kind_label = match &applied.kind {
        OverrideKind::Modified => "Modified",
        OverrideKind::Added => "Added",
        OverrideKind::Removed => "Removed",
    };

    let source_label = match &applied.source {
        OverrideSource::Channel => "channel override".to_string(),
        OverrideSource::Group(id) => format!("via group {id}"),
    };

    let kind_class = match &applied.kind {
        OverrideKind::Modified => "kiln-override-diff--modified",
        OverrideKind::Added => "kiln-override-diff--added",
        OverrideKind::Removed => "kiln-override-diff--removed",
    };

    let file_name = applied
        .file
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    rsx! {
        div { class: "kiln-override-diff {kind_class}",
            // Header
            div { class: "kiln-override-diff__header",
                span { class: "kiln-override-diff__kind", "{kind_label}" }
                span { class: "kiln-override-diff__source", "{source_label}" }
            }

            // CXL comparison (only for Modified)
            if applied.kind == OverrideKind::Modified {
                div { class: "kiln-override-diff__compare",
                    div { class: "kiln-override-diff__panel",
                        div { class: "kiln-override-diff__label", "Base" }
                        pre { class: "kiln-override-diff__code kiln-override-diff__code--base",
                            "{base_cxl}"
                        }
                    }
                    div { class: "kiln-override-diff__panel",
                        div { class: "kiln-override-diff__label", "Override" }
                        pre { class: "kiln-override-diff__code kiln-override-diff__code--override",
                            "{resolved_cxl}"
                        }
                    }
                }
            }

            if applied.kind == OverrideKind::Added {
                div { class: "kiln-override-diff__added",
                    div { class: "kiln-override-diff__label", "Added CXL" }
                    pre { class: "kiln-override-diff__code kiln-override-diff__code--override",
                        "{resolved_cxl}"
                    }
                }
            }

            if applied.kind == OverrideKind::Removed {
                div { class: "kiln-override-diff__removed",
                    div { class: "kiln-override-diff__label", "Removed CXL" }
                    pre { class: "kiln-override-diff__code kiln-override-diff__code--base",
                        "{base_cxl}"
                    }
                }
            }

            // File link
            div { class: "kiln-override-diff__file",
                span { class: "kiln-override-diff__file-label", "\u{2192} " }
                span { class: "kiln-override-diff__file-path", "{file_name}" }
            }
        }
    }
}
