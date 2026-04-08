use std::path::PathBuf;

/// All errors produced by the channel override and composition system.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML parse error: {0}")]
    Yaml(#[from] clinker_core::yaml::YamlError),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("channel not found: {id}")]
    ChannelNotFound { id: String },

    #[error("group not found: {id}")]
    GroupNotFound { id: String },

    #[error("override target not found: {name} in {file}")]
    OverrideTargetNotFound { name: String, file: PathBuf },

    #[error("add_transformations anchor not found: {name} after {after} in {file}")]
    AddAfterNotFound {
        name: String,
        after: String,
        file: PathBuf,
    },

    #[error("composition not found: {path}")]
    CompositionNotFound { path: PathBuf },

    #[error("no transforms with provenance from composition: {path}")]
    RemoveCompositionNotFound { path: PathBuf },

    #[error("failed to parse when: condition: {expr}")]
    WhenParseError { expr: String },

    #[error("circular import detected: {path}")]
    CircularImport { path: PathBuf },

    #[error("import depth exceeded ({depth}) at: {path}")]
    ImportDepthExceeded { path: PathBuf, depth: usize },

    #[error("validation error: {0}")]
    Validation(String),
}
