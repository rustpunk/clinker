use std::path::PathBuf;

/// Errors from channel file parsing and validation.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("I/O error reading channel file: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error in {path}: {source}")]
    Yaml {
        path: PathBuf,
        source: Box<serde_saphyr::Error>,
    },
    #[error("invalid UTF-8 in {path}: {source}")]
    Utf8 {
        path: PathBuf,
        source: std::str::Utf8Error,
    },
    #[error("invalid dotted path `{path}`: {reason}")]
    InvalidDottedPath { path: String, reason: String },
    #[error(
        "channel `{channel_id}`: multiple overlay candidates for target `{target}`: {}",
        .candidates.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ")
    )]
    AmbiguousOverlay {
        channel_id: String,
        target: String,
        candidates: Vec<PathBuf>,
    },
    #[error(
        "overlay {path} declares target `{declared}` that disagrees with its filename: {reason}"
    )]
    OverlayTargetMismatch {
        path: PathBuf,
        declared: String,
        reason: String,
    },
    #[error("invalid channel manifest at line {line}: {reason}. {correction}")]
    InvalidManifest {
        line: u64,
        reason: String,
        correction: String,
    },
    #[error("invalid group `{group}` at line {line}: {reason}. {correction}")]
    InvalidGroup {
        group: String,
        line: u64,
        reason: String,
        correction: String,
    },
    #[error("channel resource `{channel}` is invalid: {reason}")]
    InvalidChannelResource { channel: String, reason: String },
    #[error("group `{group}` has an invalid target set: {reason}")]
    InvalidGroupTargets { group: String, reason: String },
}
