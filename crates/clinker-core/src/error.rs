use std::fmt;

/// Top-level pipeline error enum with From impls for subsystem errors.
#[derive(Debug)]
pub enum PipelineError {
    Config(crate::config::ConfigError),
    Schema(crate::schema::SchemaError),
    Format(clinker_format::FormatError),
    Eval(cxl::eval::EvalError),
    Compilation {
        transform_name: String,
        messages: Vec<String>,
    },
    Io(std::io::Error),
    ThreadPool(String),
    /// Multiple errors collected from parallel writer threads.
    /// DataFusion `Collection` pattern (PR #14439).
    Multiple(Vec<PipelineError>),
    /// Plan-time invariant violated at runtime — Clinker bug, not a data
    /// error. ALWAYS aborts the run regardless of `ErrorStrategy::Continue`.
    /// Mirrors DataFusion's `internal_err!` macro and PR #9241 / #12086
    /// post-mortem on unreachable-arm panics in long-lived executors.
    Internal {
        op: &'static str,
        node: String,
        detail: String,
    },
    /// Finalize-time accumulator failure (overflow, type mismatch, etc.).
    /// Wraps `AccumulatorError` with the failing transform + binding for
    /// diagnostics. Routed to the DLQ under `Continue`, propagated under
    /// `FailFast`.
    Accumulator {
        transform: String,
        binding: String,
        source: clinker_record::accumulator::AccumulatorError,
    },
    /// Streaming aggregation detected an out-of-order group key on the
    /// USER-INPUT path (`StreamingAggregator<AddRaw>`). ALWAYS hard-aborts
    /// regardless of error strategy — the user's declared sort order was
    /// wrong. Surfaced to the user as a DLQ-styled error. Phase 16 Task
    /// 16.4.3.
    SortOrderViolation {
        message: String,
    },
    /// Spill-merge produced an out-of-order group key. This is a
    /// **Clinker bug** because the LoserTree should never produce
    /// out-of-order keys; the only way to reach this variant is an
    /// internal invariant violation in the spill-recovery path. ALWAYS
    /// hard-aborts. Phase 16 Task 16.4.3 (D70/D72).
    MergeSortOrderViolation {
        message: String,
    },
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(e) => write!(f, "config error: {e}"),
            Self::Schema(e) => write!(f, "schema error: {e}"),
            Self::Format(e) => write!(f, "format error: {e}"),
            Self::Eval(e) => write!(f, "evaluation error: {e}"),
            Self::Compilation {
                transform_name,
                messages,
            } => {
                write!(
                    f,
                    "CXL compilation failed for transform '{transform_name}': "
                )?;
                for msg in messages {
                    write!(f, "\n  {msg}")?;
                }
                Ok(())
            }
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::ThreadPool(e) => write!(f, "thread pool error: {e}"),
            Self::Multiple(errors) => {
                write!(f, "{} errors:", errors.len())?;
                for e in errors {
                    write!(f, "\n  - {e}")?;
                }
                Ok(())
            }
            Self::Internal { op, node, detail } => {
                write!(f, "internal error in {op} '{node}': {detail}")
            }
            Self::Accumulator {
                transform,
                binding,
                source,
            } => write!(
                f,
                "accumulator finalize failed for {transform}.{binding}: {source:?}"
            ),
            Self::SortOrderViolation { message } => {
                write!(f, "sort-order violation: {message}")
            }
            Self::MergeSortOrderViolation { message } => {
                write!(f, "spill-merge sort-order violation: {message}")
            }
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<crate::config::ConfigError> for PipelineError {
    fn from(e: crate::config::ConfigError) -> Self {
        Self::Config(e)
    }
}

impl From<clinker_format::FormatError> for PipelineError {
    fn from(e: clinker_format::FormatError) -> Self {
        Self::Format(e)
    }
}

impl From<cxl::eval::EvalError> for PipelineError {
    fn from(e: cxl::eval::EvalError) -> Self {
        Self::Eval(e)
    }
}

impl From<crate::schema::SchemaError> for PipelineError {
    fn from(e: crate::schema::SchemaError) -> Self {
        Self::Schema(e)
    }
}

impl From<std::io::Error> for PipelineError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
