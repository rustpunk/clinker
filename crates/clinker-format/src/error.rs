use std::fmt;

/// Errors produced by format readers and writers.
///
/// Streaming: errors are returned per-record, not buffered. The executor
/// decides whether to abort or skip based on the error strategy.
#[derive(Debug)]
#[non_exhaustive]
pub enum FormatError {
    Io(std::io::Error),
    Csv(csv::Error),
    Json(String),
    Xml(String),
    FixedWidth(String),
    InvalidRecord {
        row: u64,
        message: String,
    },
    SchemaInference(String),
    /// Reader saw >64 off-schema keys on a single input record. The
    /// partial record (first 64 off-schema keys captured into
    /// `$meta.*`) travels with the error so the executor can route it
    /// to DLQ. The 65th key (the one that triggered the cap) is
    /// returned verbatim alongside.
    MetadataCapExceeded {
        record: clinker_record::Record,
        key: String,
        count: usize,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Csv(e) => write!(f, "CSV error: {e}"),
            Self::Json(msg) => write!(f, "JSON error: {msg}"),
            Self::Xml(msg) => write!(f, "XML error: {msg}"),
            Self::FixedWidth(msg) => write!(f, "fixed-width error: {msg}"),
            Self::InvalidRecord { row, message } => {
                write!(f, "invalid record at row {row}: {message}")
            }
            Self::SchemaInference(msg) => write!(f, "schema inference failed: {msg}"),
            Self::MetadataCapExceeded { key, count, .. } => write!(
                f,
                "per-record metadata cap exceeded at key {key:?} (captured {count} keys before the cap)"
            ),
        }
    }
}

impl std::error::Error for FormatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Csv(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FormatError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<csv::Error> for FormatError {
    fn from(e: csv::Error) -> Self {
        Self::Csv(e)
    }
}
