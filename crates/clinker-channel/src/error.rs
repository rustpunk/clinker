use std::path::PathBuf;

/// Errors from channel file parsing and validation.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("I/O error reading channel file: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error in {path}: {source}")]
    Yaml {
        path: PathBuf,
        source: serde_saphyr::Error,
    },
    #[error("invalid dotted path `{path}`: {reason}")]
    InvalidDottedPath { path: String, reason: String },
}
