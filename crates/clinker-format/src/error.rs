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
    InvalidRecord { row: u64, message: String },
    SchemaInference(String),
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Csv(e) => write!(f, "CSV error: {e}"),
            Self::InvalidRecord { row, message } => {
                write!(f, "invalid record at row {row}: {message}")
            }
            Self::SchemaInference(msg) => write!(f, "schema inference failed: {msg}"),
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
