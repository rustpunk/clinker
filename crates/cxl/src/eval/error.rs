use crate::lexer::Span;

/// Runtime evaluation error with diagnostic metadata.
/// Follows the Boa/Starlark convention: struct with kind enum + shared span.
#[derive(Debug, Clone)]
pub struct EvalError {
    pub kind: EvalErrorKind,
    pub span: Span,
}

/// Error categories. `#[non_exhaustive]` allows future growth.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EvalErrorKind {
    TypeMismatch { expected: &'static str, got: &'static str },
    DivisionByZero,
    ConversionFailed { value: String, target: &'static str },
    RegexCompile { pattern: String, error: String },
    IndexOutOfBounds { index: i64, length: usize },
    ArityMismatch { name: String, expected: usize, got: usize },
    IntegerOverflow { op: &'static str },
    StringTooLarge { size: usize, limit: usize },
}

impl std::fmt::Display for EvalErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypeMismatch { expected, got } =>
                write!(f, "type mismatch: expected {}, got {}", expected, got),
            Self::DivisionByZero =>
                write!(f, "division by zero"),
            Self::ConversionFailed { value, target } =>
                write!(f, "conversion failed: cannot convert '{}' to {}", value, target),
            Self::RegexCompile { pattern, error } =>
                write!(f, "invalid regex '{}': {}", pattern, error),
            Self::IndexOutOfBounds { index, length } =>
                write!(f, "index {} out of bounds (length {})", index, length),
            Self::ArityMismatch { name, expected, got } =>
                write!(f, "argument count mismatch for '{}': expected {}, got {}", name, expected, got),
            Self::IntegerOverflow { op } =>
                write!(f, "integer overflow in {} operation", op),
            Self::StringTooLarge { size, limit } =>
                write!(f, "string output exceeds maximum size ({} bytes, limit {})", size, limit),
        }
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl std::error::Error for EvalError {}

impl EvalError {
    pub fn new(kind: EvalErrorKind, span: Span) -> Self {
        Self { kind, span }
    }

    pub fn type_mismatch(expected: &'static str, got: &'static str, span: Span) -> Self {
        Self::new(EvalErrorKind::TypeMismatch { expected, got }, span)
    }

    pub fn division_by_zero(span: Span) -> Self {
        Self::new(EvalErrorKind::DivisionByZero, span)
    }

    pub fn conversion_failed(value: impl Into<String>, target: &'static str, span: Span) -> Self {
        Self::new(EvalErrorKind::ConversionFailed { value: value.into(), target }, span)
    }

    pub fn integer_overflow(op: &'static str, span: Span) -> Self {
        Self::new(EvalErrorKind::IntegerOverflow { op }, span)
    }

    pub fn string_too_large(size: usize, limit: usize, span: Span) -> Self {
        Self::new(EvalErrorKind::StringTooLarge { size, limit }, span)
    }
}
