//! Stable exit classes and bounded, sanitized diagnostics.

use std::io;

use thiserror::Error;

use crate::limits::MAX_DIAGNOSTIC_BYTES;

/// Process-level classification promised by the gate CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitClass {
    /// Deterministic schema or policy rejection.
    Policy,
    /// Invalid invocation or a tooling/internal failure.
    Tool,
}

impl ExitClass {
    /// Return the stable process exit code.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Policy => 1,
            Self::Tool => 2,
        }
    }
}

/// Typed failure returned by every production gate boundary.
#[derive(Debug, Error)]
pub enum GateError {
    /// Untrusted input violated a deterministic contract.
    #[error("{code}: {detail}")]
    Policy {
        /// Stable diagnostic family.
        code: &'static str,
        /// Sanitized field-addressed detail.
        detail: String,
    },
    /// The invocation was malformed or incompatible.
    #[error("usage: {0}")]
    Usage(String),
    /// A required local tooling operation failed.
    #[error("{operation} failed ({kind:?})")]
    Io {
        /// Sanitized operation name; paths are intentionally excluded.
        operation: &'static str,
        /// Stable I/O category without platform path detail.
        kind: io::ErrorKind,
    },
    /// A gate invariant or dependency failed independently of policy input.
    #[error("{code}: {detail}")]
    Internal {
        /// Stable diagnostic family.
        code: &'static str,
        /// Sanitized bounded detail.
        detail: String,
    },
}

impl GateError {
    /// Construct a deterministic policy rejection.
    pub fn policy(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Policy {
            code,
            detail: sanitize(&detail.into(), MAX_DIAGNOSTIC_BYTES),
        }
    }

    /// Construct an invalid-invocation failure.
    pub fn usage(detail: impl Into<String>) -> Self {
        Self::Usage(sanitize(&detail.into(), MAX_DIAGNOSTIC_BYTES))
    }

    /// Construct a path-free I/O failure.
    pub fn io(operation: &'static str, error: &io::Error) -> Self {
        Self::Io {
            operation,
            kind: error.kind(),
        }
    }

    /// Construct an internal tooling failure.
    pub fn internal(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Internal {
            code,
            detail: sanitize(&detail.into(), MAX_DIAGNOSTIC_BYTES),
        }
    }

    /// Return the stable process-level classification.
    #[must_use]
    pub const fn class(&self) -> ExitClass {
        match self {
            Self::Policy { .. } => ExitClass::Policy,
            Self::Usage(_) | Self::Io { .. } | Self::Internal { .. } => ExitClass::Tool,
        }
    }

    /// Render the public diagnostic without paths, control characters, or unbounded detail.
    #[must_use]
    pub fn diagnostic(&self) -> String {
        sanitize(&self.to_string(), MAX_DIAGNOSTIC_BYTES)
    }
}

/// Bound a diagnostic on a UTF-8 character boundary and normalize controls.
#[must_use]
pub fn sanitize(input: &str, limit: usize) -> String {
    let mut output = String::with_capacity(input.len().min(limit));
    for character in input.chars() {
        let normalized = if character.is_control() && !matches!(character, '\t') {
            ' '
        } else {
            character
        };
        if output.len() + normalized.len_utf8() > limit {
            break;
        }
        output.push(normalized);
    }
    output
}
