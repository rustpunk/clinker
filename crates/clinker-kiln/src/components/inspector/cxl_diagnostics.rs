use dioxus::prelude::*;

use crate::cxl_bridge::{CxlDiagnostic, DiagSeverity};

/// Renders a list of CXL parse/type diagnostics below an expression input.
///
/// Each diagnostic shows the error message and an optional how-to-fix hint.
/// Index-based keys are acceptable here: diagnostics are ephemeral, recalculated
/// on every keystroke, and the list is small with no stable identity.
#[component]
pub fn CxlDiagnostics(diagnostics: Vec<CxlDiagnostic>) -> Element {
    if diagnostics.is_empty() {
        return rsx! {};
    }

    rsx! {
        div {
            class: "kiln-cxl-diagnostics",
            for (i, diag) in diagnostics.iter().enumerate() {
                div {
                    key: "diag-{i}",
                    class: match diag.severity {
                        DiagSeverity::Error => "kiln-cxl-diag kiln-cxl-diag--error",
                        DiagSeverity::Warning => "kiln-cxl-diag kiln-cxl-diag--warning",
                    },
                    span { class: "kiln-cxl-diag-msg", "{diag.message}" }
                    if !diag.how_to_fix.is_empty() {
                        span {
                            class: "kiln-cxl-diag-fix",
                            " \u{2192} {diag.how_to_fix}"
                        }
                    }
                }
            }
        }
    }
}
