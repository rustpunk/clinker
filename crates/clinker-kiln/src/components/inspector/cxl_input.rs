use dioxus::prelude::*;

use crate::cxl_bridge::{CxlValidation, validate_expr};

use super::cxl_diagnostics::CxlDiagnostics;

/// A single CXL expression text input with real-time parse validation.
///
/// Each `CxlInput` owns its own local signals for the text content and
/// validation result. This is safe because each instance is keyed by
/// `"{stage_id}-{label}"` and lives in its own component hook scope.
///
/// On every keystroke, `cxl_bridge::validate_expr()` runs the CXL Pratt
/// parser synchronously (sub-millisecond for typical expressions). Parse
/// errors are rendered as red diagnostics below the input.
///
/// Doc: spec §5.2 — Inspector form field generation.
#[component]
pub fn CxlInput(label: String, initial_value: String) -> Element {
    let mut text = use_signal(move || initial_value.clone());
    let initial_for_validate = text.peek().clone();
    let mut validation = use_signal(move || validate_expr(&initial_for_validate));

    let on_input = move |e: FormEvent| {
        let new_text = e.value();
        let result = validate_expr(&new_text);
        text.set(new_text);
        validation.set(result);
    };

    let v: CxlValidation = (validation)();
    let input_class = if v.is_valid {
        "kiln-cxl-input"
    } else {
        "kiln-cxl-input kiln-cxl-input--error"
    };

    rsx! {
        div {
            class: "kiln-cxl-field",

            label {
                class: "kiln-cxl-label",
                "{label}"
            }

            input {
                class: "{input_class}",
                r#type: "text",
                value: "{text}",
                oninput: on_input,
            }

            if !v.is_valid {
                CxlDiagnostics { diagnostics: v.errors }
            }
        }
    }
}
