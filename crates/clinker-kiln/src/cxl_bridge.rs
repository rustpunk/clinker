/// Thin adapter between the `cxl` compiler crate and the Kiln UI.
///
/// Wraps `cxl::parser::Parser::parse()` into a UI-friendly validation result
/// that components can render without depending on cxl types directly.
///
/// Phase 2a: parser only (no resolver/typechecker — those need a real schema).
/// Phase 2b will add `validate_with_schema()` using `resolve_program` + `type_check`.

use cxl::parser::Parser;

/// Result of validating a CXL expression, suitable for rendering.
#[derive(Clone, Debug, PartialEq)]
pub struct CxlValidation {
    pub is_valid: bool,
    pub errors: Vec<CxlDiagnostic>,
}

/// A single diagnostic message derived from a `ParseError`.
#[derive(Clone, Debug, PartialEq)]
pub struct CxlDiagnostic {
    /// Byte offset into the expression string (start of error span).
    pub start: usize,
    /// Byte offset into the expression string (end of error span).
    pub end: usize,
    pub message: String,
    pub severity: DiagSeverity,
    /// Actionable fix suggestion (may be empty).
    pub how_to_fix: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DiagSeverity {
    Error,
    #[allow(dead_code)]
    Warning,
}

/// Parse a CXL expression and return UI-friendly validation diagnostics.
///
/// Called synchronously on every keystroke in a CXL input field. The Pratt
/// parser is fast enough (sub-millisecond for <200-char expressions) that
/// no debounce is needed.
pub fn validate_expr(source: &str) -> CxlValidation {
    if source.trim().is_empty() {
        return CxlValidation {
            is_valid: true,
            errors: vec![],
        };
    }

    let result = Parser::parse(source);

    let errors: Vec<CxlDiagnostic> = result
        .errors
        .iter()
        .map(|e| CxlDiagnostic {
            start: e.span.start as usize,
            end: e.span.end as usize,
            message: e.message.clone(),
            severity: DiagSeverity::Error,
            how_to_fix: e.how_to_fix.clone(),
        })
        .collect();

    CxlValidation {
        is_valid: errors.is_empty(),
        errors,
    }
}
