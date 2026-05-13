use std::sync::Arc;

use clinker_record::Value;

use crate::lexer::Span;

/// Runtime evaluation error with diagnostic metadata.
///
/// Follows the Boa/Starlark convention: struct with kind enum + shared
/// span. Span byte offsets are relative to [`Self::source_expr`] when
/// that field is populated, which is how the miette `Diagnostic` impl
/// renders the offending subexpression with a caret.
///
/// `source_row`, `source_expr`, and `triggering_field` are attached at
/// the outermost eval boundary (`map_err` wrapper at record-level
/// evaluation entry) so the 14 internal constructors stay cheap on the
/// hot path. `triggering_value` is extracted on demand from the
/// `EvalErrorKind` payload via [`extract_triggering_value`].
#[derive(Debug, Clone)]
pub struct EvalError {
    pub kind: EvalErrorKind,
    pub span: Span,
    /// Input row index that was being evaluated when the error fired.
    pub source_row: Option<u64>,
    /// CXL source text the program was parsed from. Drives miette's
    /// underlined-expression rendering at `source_code()`.
    pub source_expr: Option<Arc<str>>,
    /// Output field the evaluator was computing when the error fired.
    /// Set at the emit-statement boundary; absent for filter / non-emit
    /// failure paths.
    pub triggering_field: Option<Arc<str>>,
}

/// Error categories. `#[non_exhaustive]` allows future growth.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EvalErrorKind {
    TypeMismatch {
        expected: &'static str,
        got: &'static str,
    },
    DivisionByZero,
    ConversionFailed {
        value: String,
        target: &'static str,
    },
    RegexCompile {
        pattern: String,
        error: String,
    },
    IndexOutOfBounds {
        index: i64,
        length: usize,
    },
    ArityMismatch {
        name: String,
        expected: usize,
        got: usize,
    },
    IntegerOverflow {
        op: &'static str,
    },
    StringTooLarge {
        size: usize,
        limit: usize,
    },
}

impl std::fmt::Display for EvalErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypeMismatch { expected, got } => {
                write!(f, "type mismatch: expected {}, got {}", expected, got)
            }
            Self::DivisionByZero => write!(f, "division by zero"),
            Self::ConversionFailed { value, target } => write!(
                f,
                "conversion failed: cannot convert '{}' to {}",
                value, target
            ),
            Self::RegexCompile { pattern, error } => {
                write!(f, "invalid regex '{}': {}", pattern, error)
            }
            Self::IndexOutOfBounds { index, length } => {
                write!(f, "index {} out of bounds (length {})", index, length)
            }
            Self::ArityMismatch {
                name,
                expected,
                got,
            } => write!(
                f,
                "argument count mismatch for '{}': expected {}, got {}",
                name, expected, got
            ),
            Self::IntegerOverflow { op } => write!(f, "integer overflow in {} operation", op),
            Self::StringTooLarge { size, limit } => write!(
                f,
                "string output exceeds maximum size ({} bytes, limit {})",
                size, limit
            ),
        }
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.source_row, self.triggering_field.as_deref()) {
            (Some(row), Some(field)) => {
                write!(f, "row {row}: field '{field}': {}", self.kind)
            }
            (Some(row), None) => write!(f, "row {row}: {}", self.kind),
            (None, Some(field)) => write!(f, "field '{field}': {}", self.kind),
            (None, None) => write!(f, "{}", self.kind),
        }
    }
}

impl std::error::Error for EvalError {}

impl EvalError {
    pub fn new(kind: EvalErrorKind, span: Span) -> Self {
        Self {
            kind,
            span,
            source_row: None,
            source_expr: None,
            triggering_field: None,
        }
    }

    pub fn type_mismatch(expected: &'static str, got: &'static str, span: Span) -> Self {
        Self::new(EvalErrorKind::TypeMismatch { expected, got }, span)
    }

    pub fn division_by_zero(span: Span) -> Self {
        Self::new(EvalErrorKind::DivisionByZero, span)
    }

    pub fn conversion_failed(value: impl Into<String>, target: &'static str, span: Span) -> Self {
        Self::new(
            EvalErrorKind::ConversionFailed {
                value: value.into(),
                target,
            },
            span,
        )
    }

    pub fn integer_overflow(op: &'static str, span: Span) -> Self {
        Self::new(EvalErrorKind::IntegerOverflow { op }, span)
    }

    pub fn string_too_large(size: usize, limit: usize, span: Span) -> Self {
        Self::new(EvalErrorKind::StringTooLarge { size, limit }, span)
    }

    /// Attach the source row currently being evaluated. Called by the
    /// boundary wrapper at record-level eval entry, so every error
    /// surfaced past that boundary carries the row that triggered it.
    pub fn with_source_row(mut self, row: u64) -> Self {
        self.source_row = Some(row);
        self
    }

    /// Attach the original CXL source text for miette rendering.
    pub fn with_source_expr(mut self, expr: Arc<str>) -> Self {
        self.source_expr = Some(expr);
        self
    }

    /// Attach the output field name the evaluator was computing.
    /// Set at the emit-statement boundary.
    pub fn with_triggering_field(mut self, field: Arc<str>) -> Self {
        self.triggering_field = Some(field);
        self
    }

    /// Extract the value carried by the underlying [`EvalErrorKind`]
    /// payload, when the variant exposes one. Best-effort: variants
    /// that describe a shape (`TypeMismatch`, `DivisionByZero`, etc.)
    /// return `None`; variants whose payload is a concrete value or
    /// integer (`ConversionFailed`, `IndexOutOfBounds`, `ArityMismatch`)
    /// surface that value so DLQ consumers see what failed without
    /// re-running the pipeline.
    pub fn triggering_value(&self) -> Option<Value> {
        extract_triggering_value(&self.kind)
    }
}

/// Extract the value carried by an [`EvalErrorKind`] payload, when the
/// variant exposes one. Symmetric with the runtime DLQ writer's
/// `triggering_value` column.
pub fn extract_triggering_value(kind: &EvalErrorKind) -> Option<Value> {
    match kind {
        EvalErrorKind::ConversionFailed { value, .. } => {
            Some(Value::String(Box::<str>::from(value.as_str())))
        }
        EvalErrorKind::IndexOutOfBounds { index, .. } => Some(Value::Integer(*index)),
        EvalErrorKind::ArityMismatch { got, .. } => Some(Value::Integer(*got as i64)),
        EvalErrorKind::TypeMismatch { .. }
        | EvalErrorKind::DivisionByZero
        | EvalErrorKind::RegexCompile { .. }
        | EvalErrorKind::IntegerOverflow { .. }
        | EvalErrorKind::StringTooLarge { .. } => None,
    }
}

impl miette::Diagnostic for EvalError {
    fn code<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        let s: &'static str = match &self.kind {
            EvalErrorKind::TypeMismatch { .. } => "cxl::eval::type_mismatch",
            EvalErrorKind::DivisionByZero => "cxl::eval::division_by_zero",
            EvalErrorKind::ConversionFailed { .. } => "cxl::eval::conversion_failed",
            EvalErrorKind::RegexCompile { .. } => "cxl::eval::regex_compile",
            EvalErrorKind::IndexOutOfBounds { .. } => "cxl::eval::index_out_of_bounds",
            EvalErrorKind::ArityMismatch { .. } => "cxl::eval::arity_mismatch",
            EvalErrorKind::IntegerOverflow { .. } => "cxl::eval::integer_overflow",
            EvalErrorKind::StringTooLarge { .. } => "cxl::eval::string_too_large",
        };
        Some(Box::new(s))
    }

    fn severity(&self) -> Option<miette::Severity> {
        Some(miette::Severity::Error)
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        self.source_expr
            .as_ref()
            .map(|s| s as &dyn miette::SourceCode)
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        let start = self.span.start as usize;
        let len = self.span.end.saturating_sub(self.span.start) as usize;
        let s = miette::SourceSpan::new(start.into(), len);
        Some(Box::new(std::iter::once(
            miette::LabeledSpan::new_with_span(Some(self.kind.to_string()), s),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Span;

    fn s(start: u32, end: u32) -> Span {
        Span { start, end }
    }

    #[test]
    fn source_row_appears_in_display() {
        let err = EvalError::division_by_zero(s(0, 1)).with_source_row(42);
        let rendered = err.to_string();
        assert!(rendered.contains("row 42"), "got: {rendered}");
        assert!(rendered.contains("division by zero"), "got: {rendered}");
    }

    #[test]
    fn triggering_field_appears_in_display() {
        let err = EvalError::division_by_zero(s(0, 1))
            .with_source_row(7)
            .with_triggering_field(Arc::from("total"));
        let rendered = err.to_string();
        assert!(rendered.contains("field 'total'"), "got: {rendered}");
    }

    #[test]
    fn miette_source_code_returns_attached_expr() {
        let expr: Arc<str> = Arc::from("emit total = a / b");
        let err = EvalError::division_by_zero(s(13, 18)).with_source_expr(Arc::clone(&expr));
        let report = miette::Report::new(err);
        let rendered = format!("{:?}", report.with_source_code(expr.to_string()));
        // The fancy formatter underlines the failing subexpression.
        // We assert on the expression text and the per-error code arm.
        assert!(rendered.contains("a / b"), "got: {rendered}");
        assert!(
            rendered.contains("cxl::eval::division_by_zero"),
            "got: {rendered}"
        );
    }

    #[test]
    fn extract_triggering_value_table() {
        assert_eq!(
            extract_triggering_value(&EvalErrorKind::ConversionFailed {
                value: "not-a-number".to_string(),
                target: "int",
            }),
            Some(Value::String(Box::<str>::from("not-a-number")))
        );
        assert_eq!(
            extract_triggering_value(&EvalErrorKind::IndexOutOfBounds {
                index: 12,
                length: 3,
            }),
            Some(Value::Integer(12))
        );
        assert_eq!(
            extract_triggering_value(&EvalErrorKind::ArityMismatch {
                name: "round".to_string(),
                expected: 2,
                got: 5,
            }),
            Some(Value::Integer(5))
        );
        assert_eq!(
            extract_triggering_value(&EvalErrorKind::DivisionByZero),
            None
        );
        assert_eq!(
            extract_triggering_value(&EvalErrorKind::TypeMismatch {
                expected: "Int",
                got: "String",
            }),
            None
        );
        assert_eq!(
            extract_triggering_value(&EvalErrorKind::IntegerOverflow { op: "+" }),
            None
        );
    }
}
